import time
import requests
from datetime import datetime
from typing import List, Dict, Any

from .base import BaseCollector
from ..models import ReviewsCollectorConfig


class ReviewsCollector(BaseCollector):
    """Google Reviews collector using DataForSEO API with pipeline pattern."""
    
    def __init__(self):
        super().__init__("reviews")
    
    async def _collect_raw(self, config: ReviewsCollectorConfig) -> bool:
        """Collect reviews from DataForSEO API for all competitors."""
        try:
            all_reviews = []
            
            for competitor in config.competitors:
                competitor_reviews = self._get_competitor_reviews(competitor, config)
                
                # Add competitor context to each review
                for review in competitor_reviews:
                    review["competitor_id"] = competitor["competitor_id"]
                    review["competitor_name"] = competitor["name"]
                    review["collected_at"] = datetime.now().isoformat()
                
                all_reviews.extend(competitor_reviews)
                
                self.stats["total_attempted"] += 1
                if competitor_reviews:
                    self.stats["successful"] += 1
                else:
                    self.stats["failed"] += 1
                
                if config.request_delay:
                    time.sleep(config.request_delay)
            
            self.raw_data = all_reviews
            self.logger.info(f"Review collection complete: {len(all_reviews)} reviews from {len(config.competitors)} competitors")
            return True
            
        except Exception as e:
            self.logger.error(f"Review collection failed: {str(e)}")
            self.errors.append(f"Collection error: {str(e)}")
            return False
    
    def _get_competitor_reviews(self, competitor: Dict, config: ReviewsCollectorConfig) -> List[Dict]:
        """Get reviews for a single competitor using DataForSEO API."""
        name = competitor["name"]
        cid = competitor["cid"]
        
        self.logger.info(f"Processing: {name} (CID: {cid})")
        
        auth = requests.auth.HTTPBasicAuth(config.dataforseo_username, config.dataforseo_password)
        
        post_payload = [{
            "keyword": f"cid:{cid}",
            "location_name": config.location_name,
            "language_name": config.language_name,
            "depth": config.review_depth,
            "sort_by": "newest"
        }]
        
        try:
            post_response = requests.post(
                "https://api.dataforseo.com/v3/business_data/google/reviews/task_post",
                json=post_payload,
                auth=auth,
                timeout=30
            )
            post_response.raise_for_status()
            post_data = post_response.json()
            
            if post_data.get("status_code") != 20000 or not post_data.get("tasks"):
                self.logger.warning(f"API task creation failed for {name}")
                self.errors.append(f"{name}: API task creation failed")
                return []
            
            task_id = post_data["tasks"][0]["id"]
            self.logger.info(f"Task created for {name}: {task_id}")
            
        except Exception as e:
            self.logger.error(f"Error creating task for {name}: {e}")
            self.errors.append(f"{name}: Task creation error - {str(e)}")
            return []
        
        # Poll for completion
        task_ready = False
        start_time = time.time()
        max_wait_seconds = 300
        
        while not task_ready and (time.time() - start_time) < max_wait_seconds:
            try:
                time.sleep(15)
                ready_response = requests.get(
                    "https://api.dataforseo.com/v3/business_data/google/reviews/tasks_ready", 
                    auth=auth
                )
                ready_response.raise_for_status()
                ready_data = ready_response.json()
                
                if ready_data.get('tasks') and any(
                    task.get('id') == task_id 
                    for res in ready_data['tasks'] 
                    for task in res.get('result', [])
                ):
                    self.logger.info(f"Task ready for {name}")
                    task_ready = True
                else:
                    elapsed = int(time.time() - start_time)
                    self.logger.debug(f"Waiting for {name} task... ({elapsed}s elapsed)")
                    
            except Exception as e:
                self.logger.error(f"Polling error for {name}: {e}")
                self.errors.append(f"{name}: Polling error - {str(e)}")
                return []
        
        if not task_ready:
            self.logger.warning(f"Task timed out for {name}")
            self.errors.append(f"{name}: Task timed out")
            return []
        
        # Get results
        try:
            get_response = requests.get(
                f"https://api.dataforseo.com/v3/business_data/google/reviews/task_get/{task_id}",
                auth=auth,
                timeout=30
            )
            get_response.raise_for_status()
            results_data = get_response.json()
            
            if results_data.get("status_code") != 20000:
                self.logger.warning(f"Failed to retrieve results for {name}")
                self.errors.append(f"{name}: Failed to retrieve results")
                return []
            
            result_items = results_data["tasks"][0]["result"][0].get("items", [])
            reviews = []
            
            for item in result_items:
                rating_info = item.get("rating", {})
                review = {
                    'review_id': item.get('review_id'),
                    'cid': cid,
                    'review_text': item.get('review_text'),
                    'rating': rating_info.get('value'),
                    'timestamp': item.get('timestamp'),
                    'reviewer_name': item.get('profile_name'),
                }
                reviews.append(review)
            
            self.logger.info(f"Found {len(reviews)} reviews for {name}")
            return reviews
            
        except Exception as e:
            self.logger.error(f"Error fetching results for {name}: {e}")
            self.errors.append(f"{name}: Results fetch error - {str(e)}")
            return []
    
    async def _transform(self, config: ReviewsCollectorConfig) -> bool:
        """Transform raw review data to ReviewListing schema."""
        try:
            processed_records = []
            
            for review_data in self.raw_data:
                record_data = {
                    "review_id": review_data.get("review_id"),
                    "competitor_id": review_data.get("competitor_id"),
                    "cid": review_data.get("cid"),
                    "fetched_at": datetime.now(),
                    "review_text": review_data.get("review_text"),
                    "rating": review_data.get("rating"),
                    "timestamp": review_data.get("timestamp"),
                    "reviewer_name": review_data.get("reviewer_name")
                }
                
                processed_records.append(record_data)
            
            self.processed_data = processed_records
            self.logger.info(f"Transform complete: {len(processed_records)} reviews processed")
            return True
            
        except Exception as e:
            self.logger.error(f"Transform stage failed: {str(e)}")
            self.errors.append(f"Transform error: {str(e)}")
            return False