import asyncio
import hashlib
import json
from datetime import datetime
from typing import List, Dict, Any
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMExtractionStrategy, LLMConfig, AsyncUrlSeeder, SeedingConfig

from .base import BaseCollector
from ..models import QPCollectorConfig, QPListing


class QPCollector(BaseCollector):
    """Quick Possession property collector using pipeline pattern."""
    
    def __init__(self):
        super().__init__("quickpossession")
    
    async def _collect_raw(self, config: QPCollectorConfig) -> bool:
        """Discover URLs from all competitors."""
        try:
            all_urls = []
            
            for competitor in config.competitors:
                competitor_urls = await self._discover_competitor_urls(competitor, config)
                
                # Apply URL limit
                limited_urls = competitor_urls[:config.url_limit_per_competitor]
                self.logger.info(f"Limiting {competitor['code']} to {len(limited_urls)} URLs (from {len(competitor_urls)} found)")
                
                # Add competitor context to each URL
                for url in limited_urls:
                    url_data = {
                        "url": url,
                        "competitor_id": competitor["code"],
                        "competitor_name": competitor["name"],
                        "discovered_at": datetime.now().isoformat()
                    }
                    all_urls.append(url_data)
                
                self.stats["total_attempted"] += len(competitor_urls)
                self.stats["successful"] += len(limited_urls)
                
                # Respect rate limiting
                if config.request_delay:
                    await asyncio.sleep(config.request_delay)
            
            self.raw_data = all_urls
            self.logger.info(f"URL discovery complete: {len(all_urls)} URLs ready for extraction")
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
        """Extract property data from URLs using LLM."""
        try:
            if not self.raw_data:
                self.logger.warning("No URLs to process in transform stage")
                return True
            
            # Extract URLs for batch processing
            urls = [item["url"] for item in self.raw_data]
            self.logger.info(f"Starting LLM extraction for {len(urls)} URLs")
            
            # Run async extraction
            extracted_data = await self._extract_property_data(urls, config)

            # Combine URL metadata with extraction results
            processed_records = []
            for i, url_data in enumerate(self.raw_data):
                extraction_result = extracted_data[i] if i < len(extracted_data) else {}
                
                # Generate property_id from URL hash
                property_id = hashlib.md5(url_data["url"].encode()).hexdigest()[:12]
                
                # Create QPListing record
                record_data = {
                    "property_id": property_id,
                    "competitor_id": url_data["competitor_id"],
                    "url": url_data["url"],
                    "fetched_at": datetime.now(),
                    "metadata": {
                        "discovered_at": url_data["discovered_at"],
                        "extraction_status": extraction_result.get("extraction_status", "failed")
                    }
                }
                
                # Add extracted property data if successful
                if extraction_result.get("extraction_status") == "success":
                    property_fields = ["address", "community", "price", "sqft", "beds", "baths", "main_image_url", "features"]
                    for field in property_fields:
                        record_data[field] = extraction_result.get(field)
                
                processed_records.append(record_data)
            
            self.processed_data = processed_records
            successful_extractions = len([r for r in processed_records if r["metadata"]["extraction_status"] == "success"])
            self.logger.info(f"Transform complete: {successful_extractions}/{len(processed_records)} successful extractions")
            
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
            verbose=True 
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