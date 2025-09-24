import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMExtractionStrategy, LLMConfig, AsyncUrlSeeder, SeedingConfig

from .base import BaseCollector
from ..utils.sqlite_manager import SQLiteStateManager
from ..models import QPCollectorConfig, PropertyExtractionSchema, DATA_FRESHNESS_HOURS


class QPCollector(BaseCollector):
    """Quick Possession collector with two-table architecture matching Google Sheets pattern."""

    # Table schemas
    URLS_SCHEMA = {
        "property_id": "TEXT NOT NULL",
        "url": "TEXT PRIMARY KEY",
        "competitor_id": "TEXT NOT NULL",
        "status": "TEXT NOT NULL DEFAULT 'active'",  # 'active', 'inactive'
        "first_seen": "TIMESTAMP NOT NULL",
        "last_seen": "TIMESTAMP NOT NULL", 
        "extraction_status": "TEXT NOT NULL DEFAULT 'pending'",  # 'pending', 'success', 'failed'
        "last_attempted_extraction": "TIMESTAMP",
        "metadata": "TEXT",  # JSON string
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    }

    PROPERTIES_SCHEMA = {
        "property_id": "TEXT PRIMARY KEY",
        "address": "TEXT NOT NULL",
        "community": "TEXT",
        "price": "REAL",
        "sqft": "REAL", 
        "beds": "INTEGER",
        "baths": "REAL",
        "main_image_url": "TEXT",
        "features": "TEXT",  # JSON string
        "extracted_at": "TIMESTAMP NOT NULL",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    }

    URLS_INDEXES = ["competitor_id", "status", "extraction_status", "last_seen"]
    PROPERTIES_INDEXES = ["property_id"]

    def __init__(self):
        super().__init__("quickpossession")
        self.state_manager = SQLiteStateManager("quickpossession")
        self._initialize_tables()

    def _initialize_tables(self) -> None:
        """Initialize two-table schema."""
        success = self.state_manager.create_multiple_tables({
            "urls": (self.URLS_SCHEMA, self.URLS_INDEXES),
            "properties": (self.PROPERTIES_SCHEMA, self.PROPERTIES_INDEXES)
        })
        if not success:
            raise Exception("Failed to initialize two-table schema")

    def _generate_property_id(self, competitor_id: str, existing_ids: Set[str]) -> str:
        """Generate unique property_id for competitor."""
        competitor_ids = [
            int(pid.split('_')[1]) for pid in existing_ids 
            if pid.startswith(f"{competitor_id}_")
        ]
        next_seq = max(competitor_ids, default=0) + 1
        return f"{competitor_id}_{next_seq:05d}"

    def _competitor_needs_scan(self, competitor_id: str) -> bool:
        """Check if competitor needs URL discovery (>24h since last scan)."""
        cutoff_time = datetime.now() - timedelta(hours=DATA_FRESHNESS_HOURS)
        
        result = self.state_manager.query(
            "SELECT MAX(last_seen) as last_scan FROM urls WHERE competitor_id = ?",
            [competitor_id]
        )
        
        if not result or not result[0]['last_scan']:
            self.logger.info(f"🆕 No previous scans found for {competitor_id} - proceeding with discovery")
            return True
            
        last_scan = datetime.fromisoformat(result[0]['last_scan'].replace('Z', '+00:00'))
        hours_since = (datetime.now() - last_scan).total_seconds() / 3600
        
        if last_scan < cutoff_time:
            self.logger.info(f"🕐 {competitor_id} last scanned {hours_since:.1f}h ago - proceeding with discovery")
            return True
        else:
            self.logger.info(f"⏭️ Skipping {competitor_id} - scanned {hours_since:.1f}h ago (< {DATA_FRESHNESS_HOURS}h threshold)")
            return False

    async def _discover_competitor_urls(self, competitor: Dict, config: QPCollectorConfig) -> List[Dict]:
        """Discover live URLs for single competitor using AsyncUrlSeeder."""
        competitor_id = competitor["code"]
        domain = competitor["domain"]
        pattern = competitor["pattern"]
        
        self.logger.info(f"Discovering URLs for {competitor_id} on {domain}...")
        
        try:
            seeding_config = SeedingConfig(
                source='sitemap+cc',
                pattern=pattern,
                extract_head=True,
                live_check=True,
                concurrency=5,
                filter_nonsense_urls=True,
                verbose=False
            )
            
            async with AsyncUrlSeeder() as seeder:
                urls_data = await seeder.urls(domain, config=seeding_config)
            
            self.logger.info(f"Found {len(urls_data)} live URLs for {competitor_id}")
            return [{"url": item["url"], "metadata": item.get("head_data", {})} for item in urls_data]
            
        except Exception as e:
            self.logger.error(f"URL discovery failed for {competitor_id}: {str(e)}")
            self.errors.append(f"{competitor_id}: URL discovery error - {str(e)}")
            return []

    async def _collect_raw(self, config: QPCollectorConfig) -> bool:
        """Stage 1: URL Discovery and Lifecycle Management."""
        try:
            self.logger.info("Starting Stage 1: URL Discovery and Lifecycle Management")
            current_time = datetime.now().isoformat()
            
            # Get existing property_ids for unique ID generation
            existing_properties = self.state_manager.query("SELECT property_id FROM urls")
            existing_ids = {row["property_id"] for row in existing_properties}
            
            for competitor in config.competitors:
                competitor_id = competitor["code"]
                
                # Skip if scanned recently
                if not self._competitor_needs_scan(competitor_id):
                    self.logger.info(f"Skipping {competitor_id} - scanned within {DATA_FRESHNESS_HOURS}h")
                    continue
                
                self.logger.info(f"Processing competitor: {competitor_id}")
                
                # Get current URLs in database for this competitor
                existing_urls = self.state_manager.query(
                    "SELECT url, property_id FROM urls WHERE competitor_id = ?",
                    [competitor_id]
                )
                existing_url_set = {row["url"] for row in existing_urls}
                url_to_property_id = {row["url"]: row["property_id"] for row in existing_urls}
                
                # Discover live URLs
                live_urls_data = await self._discover_competitor_urls(competitor, config)
                live_url_set = {item["url"] for item in live_urls_data}
                live_urls_map = {item["url"]: item for item in live_urls_data}
                
                # Determine URL status changes
                new_urls = live_url_set - existing_url_set
                missing_urls = existing_url_set - live_url_set  
                existing_live_urls = existing_url_set.intersection(live_url_set)
                
                # Add new URLs
                new_url_records = []
                for url in new_urls:
                    property_id = self._generate_property_id(competitor_id, existing_ids)
                    existing_ids.add(property_id)
                    
                    url_data = live_urls_map[url]
                    new_url_records.append({
                        "property_id": property_id,
                        "url": url,
                        "competitor_id": competitor_id,
                        "status": "active",
                        "first_seen": current_time,
                        "last_seen": current_time,
                        "extraction_status": "pending",
                        "metadata": json.dumps(url_data.get("metadata", {}))
                    })
                
                if new_url_records:
                    self.state_manager.insert_or_ignore("urls", new_url_records)
                    self.logger.info(f"Added {len(new_url_records)} new URLs for {competitor_id}")
                
                # Mark missing URLs as inactive
                if missing_urls:
                    self.state_manager.update_records_batch(
                        "urls",
                        {"status": "inactive", "updated_at": current_time},
                        "url IN ({})".format(','.join('?' * len(missing_urls))),
                        list(missing_urls)
                    )
                    self.logger.info(f"Marked {len(missing_urls)} URLs as inactive for {competitor_id}")
                
                # Update existing live URLs
                if existing_live_urls:
                    self.state_manager.update_records_batch(
                        "urls", 
                        {"last_seen": current_time, "status": "active", "updated_at": current_time},
                        "url IN ({})".format(','.join('?' * len(existing_live_urls))),
                        list(existing_live_urls)
                    )
                    self.logger.info(f"Updated {len(existing_live_urls)} existing URLs for {competitor_id}")
                
                self.stats["total_attempted"] += len(live_urls_data)
                self.stats["successful"] += len(live_urls_data)
                
                # Rate limiting
                if config.request_delay:
                    await asyncio.sleep(config.request_delay)
            
            self.logger.info("Stage 1 Complete: URL Discovery and Lifecycle Management")
            return True
            
        except Exception as e:
            self.logger.error(f"Stage 1 failed: {str(e)}")
            self.errors.append(f"URL discovery error: {str(e)}")
            return False

    def _get_urls_for_extraction(self, limit_per_competitor: int) -> List[Dict]:
        """Get URLs pending extraction, limited per competitor."""
        all_pending = []
        
        for competitor in self.config.competitors:
            competitor_id = competitor["code"]
            pending_urls = self.state_manager.query(
                """SELECT property_id, url, competitor_id FROM urls 
                   WHERE competitor_id = ? AND status = 'active' AND extraction_status = 'pending'
                   ORDER BY first_seen LIMIT ?""",
                [competitor_id, limit_per_competitor]
            )
            all_pending.extend(pending_urls)
        
        self.logger.info(f"Found {len(all_pending)} URLs pending extraction")
        return all_pending

    async def _extract_batch_data(self, url_batch: List[Dict], batch_processing: bool = True) -> List[Dict]:
        """Extract data from URLs, with batch or individual processing."""
        if batch_processing:
            return await self._extract_batch(url_batch)
        else:
            return await self._extract_individual(url_batch)

    async def _extract_batch(self, url_batch: List[Dict]) -> List[Dict]:
        """Batch extraction using crawl4ai."""
        urls_to_crawl = [record["url"] for record in url_batch]
        # Log each URL being processed
        for i, record in enumerate(url_batch, 1):
            self.logger.info(f"🏠 Processing property {i}/{len(url_batch)}: {record['property_id']} - {record['url']}")
    
        llm_config = LLMConfig(provider="openai/gpt-4o-mini")
        llm_strategy = LLMExtractionStrategy(
            llm_config=llm_config,
            schema=PropertyExtractionSchema.model_json_schema(),
            instruction="""
            Extract property listing data according to the JSON schema.
            RULES:
            1. 'address' is the most important field - must be a complete street address.
            2. For 'price' and 'sqft', extract only numerical values.
            3. For 'beds' and 'baths', extract integers or floats.
            4. If a field cannot be found, use null.
            5. Return valid JSON that adheres to the schema.
            """
        )
        
        crawler_config = CrawlerRunConfig(extraction_strategy=llm_strategy, verbose=False)
        results = []
        
        async with AsyncWebCrawler() as crawler:
            tasks = [crawler.arun(url, config=crawler_config) for url in urls_to_crawl]
            crawl_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(crawl_results):
                property_id = url_batch[i]["property_id"]
                url = url_batch[i]["url"]
                
                if isinstance(result, Exception) or not result.success or not result.extracted_content:
                    self.logger.info(f"❌ Failed extraction for {property_id}: {url}")
                    results.append({"property_id": property_id, "status": "failed", "data": None})
                else:
                    try:
                        data = json.loads(result.extracted_content)
                        data = data[0] if isinstance(data, list) and data else data
                        
                        if data and isinstance(data.get("address"), str) and data["address"].strip():
                            address = data.get("address", "")[:50] + "..." if len(data.get("address", "")) > 50 else data.get("address", "")
                            self.logger.info(f"✅ Success for {property_id}: {address}")
                            results.append({"property_id": property_id, "status": "success", "data": data})
                        else:
                            self.logger.info(f"❌ Invalid data for {property_id}: Missing/invalid address")
                            results.append({"property_id": property_id, "status": "failed", "data": None})
                    except json.JSONDecodeError:
                        self.logger.info(f"❌ JSON parse error for {property_id}: {url}")
                        results.append({"property_id": property_id, "status": "failed", "data": None})
        
        return results

    async def _extract_individual(self, url_batch: List[Dict]) -> List[Dict]:
        """Individual extraction (for future features like content tracking)."""
        results = []
        for url_record in url_batch:
            batch_result = await self._extract_batch([url_record])
            results.extend(batch_result)
        return results

    def _commit_extraction_results(self, batch_results: List[Dict]) -> None:
        """Commit extraction results to both tables atomically."""
        current_time = datetime.now().isoformat()
        property_records = []
        
        for result in batch_results:
            property_id = result["property_id"]
            status = result["status"]
            
            # Update URLs table
            self.state_manager.update_record(
                "urls",
                {
                    "extraction_status": status,
                    "last_attempted_extraction": current_time,
                    "updated_at": current_time
                },
                "property_id = ?",
                [property_id]
            )
            
            # Insert into properties table if successful
            if status == "success" and result["data"]:
                data = result["data"]
                property_record = {
                    "property_id": property_id,
                    "address": data.get("address"),
                    "community": data.get("community"),
                    "price": data.get("price"),
                    "sqft": data.get("sqft"), 
                    "beds": data.get("beds"),
                    "baths": data.get("baths"),
                    "main_image_url": data.get("main_image_url"),
                    "features": json.dumps(data.get("features", {})),
                    "extracted_at": current_time
                }
                property_records.append(property_record)
        
        if property_records:
            self.state_manager.insert_or_ignore("properties", property_records)
            self.logger.info(f"Committed {len(property_records)} successful extractions")

    async def _transform(self, config: QPCollectorConfig) -> bool:
        """Stage 2: Data Extraction with batch processing."""
        try:
            self.logger.info("Starting Stage 2: Data Extraction")
            self.config = config  # Store for use in _get_urls_for_extraction
            
            pending_urls = self._get_urls_for_extraction(config.url_limit_per_competitor)
            
            if not pending_urls:
                self.logger.info("No URLs to process. Stage 2 Complete.")
                return True
            
            # Process in batches of 10
            batch_size = 10
            batches = [pending_urls[i:i + batch_size] for i in range(0, len(pending_urls), batch_size)]
            
            self.logger.info(f"Processing {len(pending_urls)} URLs in {len(batches)} batches")
            
            successful_extractions = 0
            failed_extractions = 0
            
            for batch_num, url_batch in enumerate(batches, 1):
                self.logger.info(f"Processing batch {batch_num}/{len(batches)}")
                
                # Extract data (batch processing by default)
                batch_results = await self._extract_batch_data(
                    url_batch, 
                    batch_processing=getattr(config, 'batch_processing', True)
                )
                
                # Commit results
                self._commit_extraction_results(batch_results)
                
                # Update stats
                batch_successful = sum(1 for r in batch_results if r["status"] == "success")
                batch_failed = len(batch_results) - batch_successful
                
                successful_extractions += batch_successful
                failed_extractions += batch_failed
                
                self.logger.info(f"Batch {batch_num} complete: {batch_successful} success, {batch_failed} failed")
            
            # Update collector stats
            self.stats["successful"] = successful_extractions
            self.stats["failed"] = failed_extractions
            
            self.logger.info(f"Stage 2 Complete: {successful_extractions} successful, {failed_extractions} failed")
            return True
            
        except Exception as e:
            self.logger.error(f"Stage 2 failed: {str(e)}")
            self.errors.append(f"Extraction error: {str(e)}")
            return False

    def _finalize(self, config: QPCollectorConfig):
        """Finalize collection with summary statistics."""
        # Get final statistics
        urls_stats = self.state_manager.get_stats("urls", "extraction_status")
        properties_count = self.state_manager.count("properties")
        
        # Log final statistics
        self.logger.info(f"Collection complete:")
        self.logger.info(f"  URLs: {urls_stats}")
        self.logger.info(f"  Properties: {properties_count}")
        
        # Enhanced metadata
        metadata = {
            "urls_stats": urls_stats,
            "properties_count": properties_count,
            "successful_extractions": urls_stats.get("success", 0),
            "failed_extractions": urls_stats.get("failed", 0), 
            "pending_extractions": urls_stats.get("pending", 0),
            "errors": self.errors,
            "state_db_path": str(self.state_manager.db_path)
        }
        
        # Create result (no CSV export needed - normalize reads from SQL)
        return self._create_result([], config.output_path, metadata)