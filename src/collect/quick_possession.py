import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMExtractionStrategy, LLMConfig, AsyncUrlSeeder, SeedingConfig

from .base import BaseCollector
from ..utils.sqlite_manager import SQLiteStateManager
from ..models import QPCollectorConfig, QPListing, DATA_FRESHNESS_HOURS


class QPCollector(BaseCollector):
    """Quick Possession property collector with SQLite-based caching."""

    TABLE_NAME = "properties"

    # QP-specific table schema
    QP_SCHEMA = {
        # Business key first
        "competitor_id": "TEXT NOT NULL",

        # Technical primary key
        "url": "TEXT PRIMARY KEY",
        "property_id": "TEXT",

        # Discovery phase
        "discovered_at": "TIMESTAMP NOT NULL",
        "extraction_status": "TEXT DEFAULT 'pending'",

        # Audit columns
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "metadata": "TEXT",  # JSON string

        # Extraction phase
        "extracted_at": "TIMESTAMP",

        # Extracted property data
        "address": "TEXT",
        "community": "TEXT",
        "price": "REAL",
        "sqft": "REAL",
        "beds": "INTEGER",
        "baths": "REAL",
        "main_image_url": "TEXT",
        "features": "TEXT"  # JSON string
    }

    QP_INDEXES = ["competitor_id", "extraction_status", "discovered_at"]

    def __init__(self):
        super().__init__("quickpossession")
        self.state_manager = SQLiteStateManager("quickpossession")
        self._initialize_qp_table()

    def _initialize_qp_table(self) -> None:
        """Initialize QP-specific table schema."""
        success = self.state_manager.create_table(self.TABLE_NAME, self.QP_SCHEMA, self.QP_INDEXES)
        if not success:
            raise Exception("Failed to initialize QP properties table")

    def _has_fresh_data(self) -> bool:
        """Check if any properties exist within the freshness window."""
        cutoff_time = datetime.now() - timedelta(hours=DATA_FRESHNESS_HOURS)
        count = self.state_manager.count(
            self.TABLE_NAME,
            "discovered_at > ?",
            [cutoff_time.isoformat()]
        )

        has_fresh = count > 0
        self.logger.info(f"Fresh data check: {count} properties within {DATA_FRESHNESS_HOURS}h window (fresh: {has_fresh})")
        return has_fresh

    def _get_existing_competitors(self) -> Set[str]:
        """Get competitor IDs that have fresh data."""
        cutoff_time = datetime.now() - timedelta(hours=DATA_FRESHNESS_HOURS)
        results = self.state_manager.query(
            f"SELECT DISTINCT competitor_id FROM {self.TABLE_NAME} WHERE discovered_at > ?",
            [cutoff_time.isoformat()]
        )

        competitors = {row["competitor_id"] for row in results}
        self.logger.info(f"Found existing competitors with fresh data: {competitors}")
        return competitors

    def _add_discovered_urls(self, urls_data: List[Dict[str, Any]]) -> int:
        """Add newly discovered URLs to cache."""
        if not urls_data:
            return 0

        # Prepare data for insertion
        for url_data in urls_data:
            url_data["created_at"] = datetime.now().isoformat()
            url_data["updated_at"] = datetime.now().isoformat()

        new_count = self.state_manager.insert_or_ignore(self.TABLE_NAME, urls_data)
        self.logger.info(f"Added {new_count} new properties to cache")
        return new_count

    def _get_pending_urls_for_competitor(self, competitor_id: str, limit: int) -> List[Dict[str, Any]]:
        """Get pending URLs for specific competitor with limit."""
        results = self.state_manager.query(f"""
            SELECT url, competitor_id, property_id, discovered_at, metadata
            FROM {self.TABLE_NAME}
            WHERE extraction_status = 'pending' AND competitor_id = ?
            ORDER BY discovered_at
            LIMIT ?
        """, [competitor_id, limit])

        self.logger.info(f"Found {len(results)} pending properties for {competitor_id} (limit: {limit})")
        return results

    def _update_extraction_result(self, url: str, extraction_result: Dict[str, Any]) -> bool:
        """Update single URL with extraction result."""
        extraction_status = extraction_result.get("extraction_status", "failed")
        extracted_at = datetime.now().isoformat()

        updates = {
            "extraction_status": extraction_status,
            "extracted_at": extracted_at,
            "updated_at": extracted_at
        }

        # Add extracted fields if successful
        if extraction_status == "success":
            property_fields = ["address", "community", "price", "sqft", "beds", "baths", "main_image_url"]
            for field in property_fields:
                if field in extraction_result:
                    updates[field] = extraction_result[field]

            # Handle features as JSON
            if "features" in extraction_result:
                updates["features"] = extraction_result["features"]

        success = self.state_manager.update_record(self.TABLE_NAME, updates, "url = ?", [url])

        if success:
            self.logger.debug(f"Updated extraction result for {url}: {extraction_status}")
        else:
            self.logger.warning(f"Failed to update extraction result for {url}")

        return success

    def _get_extraction_stats(self) -> Dict[str, int]:
        """Get extraction statistics."""
        stats = self.state_manager.get_stats(self.TABLE_NAME, "extraction_status")

        # Ensure all statuses are present
        for status in ["pending", "success", "failed"]:
            if status not in stats:
                stats[status] = 0

        return stats

    async def _collect_raw(self, config: QPCollectorConfig) -> bool:
        """Discover URLs with caching - incremental discovery based on existing state."""
        try:
            # Check for fresh data
            has_fresh_data = self._has_fresh_data()
            existing_competitors = self._get_existing_competitors()

            if has_fresh_data:
                self.logger.info(f"Found fresh data for competitors: {existing_competitors}")
            else:
                self.logger.info("No fresh data found - starting full discovery")

            # Discover URLs for new competitors only
            new_urls_data = []

            for competitor in config.competitors:
                competitor_code = competitor["code"]

                if competitor_code in existing_competitors:
                    self.logger.info(f"Skipping URL discovery for {competitor_code} - already in fresh cache")
                    continue

                self.logger.info(f"Discovering URLs for new competitor: {competitor_code}")
                competitor_urls = await self._discover_competitor_urls(competitor, config)

                # Prepare URL data for cache
                for url in competitor_urls:
                    property_id = hashlib.md5(url.encode()).hexdigest()[:12]
                    url_data = {
                        "competitor_id": competitor["code"],
                        "url": url,
                        "property_id": property_id,
                        "discovered_at": datetime.now().isoformat(),
                        "extraction_status": "pending",
                        "metadata": {
                            "discovered_in_run": True,
                            "discovery_timestamp": datetime.now().isoformat()
                        }
                    }
                    new_urls_data.append(url_data)

                self.stats["total_attempted"] += len(competitor_urls)
                self.stats["successful"] += len(competitor_urls)

                # Respect rate limiting
                if config.request_delay:
                    await asyncio.sleep(config.request_delay)

            # Add new URLs to cache
            if new_urls_data:
                self._add_discovered_urls(new_urls_data)

            # Get current stats for logging
            stats = self._get_extraction_stats()
            total_urls = sum(stats.values())
            pending_count = stats.get("pending", 0)

            self.logger.info(f"URL discovery complete: {total_urls} total properties, {pending_count} pending extraction")

            # Store empty raw_data since we're using SQLite
            self.raw_data = []
            return True

        except Exception as e:
            self.logger.error(f"URL discovery failed: {str(e)}")
            self.errors.append(f"URL discovery error: {str(e)}")
            return False

    async def _discover_competitor_urls(self, competitor: Dict, config: QPCollectorConfig) -> List[str]:
        """Discover URLs for a single competitor."""
        code = competitor['code']
        domain = competitor['domain']
        pattern = competitor['pattern']

        self.logger.info(f"Discovering URLs for {code}...")

        try:
            seeding_config = SeedingConfig(
                source='sitemap+cc',
                pattern=pattern,
                live_check=True,
                concurrency=5
            )
            async with AsyncUrlSeeder() as seeder:
                urls_data = await seeder.urls(domain, config=seeding_config)

            discovered_urls = [item['url'] for item in urls_data]
            self.logger.info(f"Found {len(discovered_urls)} live URLs for {code}")
            return discovered_urls

        except Exception as e:
            self.logger.error(f"URL discovery failed for {code}: {str(e)}")
            self.errors.append(f"{code} URL discovery error: {str(e)}")
            return []

    async def _transform(self, config: QPCollectorConfig) -> bool:
        """Extract property data with caching - process pending URLs one by one."""
        try:
            # Get pending URLs with per-competitor limits
            all_pending_urls = []

            for competitor in config.competitors:
                competitor_id = competitor["code"]
                competitor_pending = self._get_pending_urls_for_competitor(
                    competitor_id,
                    config.url_limit_per_competitor
                )
                all_pending_urls.extend(competitor_pending)

            if not all_pending_urls:
                self.logger.info("No pending properties to extract")
                self.processed_data = []
                return True

            self.logger.info(f"Starting extraction for {len(all_pending_urls)} pending properties")

            # Extract URLs one by one with immediate saving
            successful_extractions = 0
            failed_extractions = 0

            for i, url_data in enumerate(all_pending_urls, 1):
                url = url_data["url"]

                try:
                    self.logger.info(f"Extracting property {i}/{len(all_pending_urls)}: {url}")

                    # Extract single URL
                    extraction_results = await self._extract_property_data([url], config)
                    extraction_result = extraction_results[0] if extraction_results else {"extraction_status": "failed"}

                    # Immediately save to SQLite
                    save_success = self._update_extraction_result(url, extraction_result)

                    if save_success and extraction_result.get("extraction_status") == "success":
                        successful_extractions += 1
                        self.logger.debug(f"✅ Successfully extracted and saved: {url}")
                    else:
                        failed_extractions += 1
                        self.logger.debug(f"❌ Failed extraction or save: {url}")

                except Exception as e:
                    failed_extractions += 1
                    self.logger.error(f"Exception during extraction of {url}: {e}")

                    # Still try to save failure status
                    self._update_extraction_result(url, {"extraction_status": "failed"})

            # Final stats
            final_stats = self._get_extraction_stats()
            total_success = final_stats.get("success", 0)
            total_failed = final_stats.get("failed", 0)
            total_pending = final_stats.get("pending", 0)

            self.logger.info(f"Extraction complete: {successful_extractions} new successes, {failed_extractions} new failures")
            self.logger.info(f"Overall stats: {total_success} successful, {total_failed} failed, {total_pending} pending")

            # Store minimal processed_data for compatibility
            self.processed_data = [{"extraction_summary": "SQLite state used - see state database for details"}]

            return True

        except Exception as e:
            self.logger.error(f"Transform stage failed: {str(e)}")
            self.errors.append(f"Transform error: {str(e)}")
            return False

    async def _extract_property_data(self, urls: List[str], config: QPCollectorConfig) -> List[Dict]:
        """Extract structured data from URLs using LLM."""
        llm_config = LLMConfig(provider=config.llm_provider)
        llm_strategy = LLMExtractionStrategy(
            llm_config=llm_config,
            schema=QPListing.model_json_schema(),
            instruction="""
            Extract property listing data according to the JSON schema.
            RULES:
            1. 'address' and 'community' are the most important fields.
            2. For 'price' and 'sqft', extract only numerical values.
            3. For 'beds' and 'baths', extract integers or floats.
            4. If a field cannot be found, use null.
            5. Return valid JSON that adheres to the schema.
            """
        )

        crawler_config = CrawlerRunConfig(
            extraction_strategy=llm_strategy,
            verbose=False
        )
        all_results = []

        async with AsyncWebCrawler() as crawler:
            tasks = [crawler.arun(url, config=crawler_config) for url in urls]
            crawl_results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(crawl_results):
                url = urls[i]
                record = {"url": url}

                if isinstance(result, Exception) or not result.success or not result.extracted_content:
                    record["extraction_status"] = "failed"
                    self.logger.debug(f"Extraction failed: {url}")
                else:
                    try:
                        raw_data = json.loads(result.extracted_content)
                        data = raw_data[0] if isinstance(raw_data, list) and raw_data else raw_data

                        if data and isinstance(data.get("address"), str):
                            record["extraction_status"] = "success"
                            record.update(data)
                            self.logger.debug(f"Extraction success: {url}")
                        else:
                            record["extraction_status"] = "failed"
                            self.logger.debug(f"Invalid data extracted: {url}")
                    except Exception:
                        record["extraction_status"] = "failed"
                        self.logger.debug(f"JSON parse error: {url}")

                all_results.append(record)

        return all_results

    def _finalize(self, config: QPCollectorConfig):
        """Override finalize to export SQLite state to CSV and provide statistics."""
        # Export state to CSV for pipeline compatibility
        csv_export_success = self.state_manager.export_table_to_csv(
            self.TABLE_NAME,
            config.output_path,
            "competitor_id, discovered_at"
        )

        if csv_export_success:
            self.logger.info(f"Exported state to CSV: {config.output_path}")
        else:
            self.logger.error(f"Failed to export state to CSV: {config.output_path}")

        # Get final statistics
        final_stats = self._get_extraction_stats()
        total_urls = sum(final_stats.values())
        successful = final_stats.get("success", 0)
        failed = final_stats.get("failed", 0)
        pending = final_stats.get("pending", 0)

        # Update collector stats for reporting
        self.stats["total_attempted"] = total_urls
        self.stats["successful"] = successful
        self.stats["failed"] = failed

        # Log final statistics
        self._log_collection_stats(total_urls, successful, failed)

        # Enhanced metadata
        metadata = {
            "total_properties": total_urls,
            "successful_extractions": successful,
            "failed_extractions": failed,
            "pending_extractions": pending,
            "extraction_breakdown": final_stats,
            "csv_export_success": csv_export_success,
            "state_db_path": str(self.state_manager.db_path),
            "errors": self.errors
        }

        # Create final result
        return self._create_result(self.processed_data, config.output_path, metadata)