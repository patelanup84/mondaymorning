import pandas as pd
from pathlib import Path

from .base import BaseNormalizer
from ..models import ReviewListing
from ..config import CLEAN_DIR


class ReviewsNormalizer(BaseNormalizer):
    """Normalizer for Google reviews."""
    
    def __init__(self):
        super().__init__("reviews")
    
    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate review data against ReviewListing schema."""
        valid_records = []
        
        for _, row in df.iterrows():
            try:
                record = row.to_dict()
                
                if pd.notna(record.get('fetched_at')):
                    record['fetched_at'] = pd.to_datetime(record['fetched_at'])
                    
                validated = ReviewListing(**record)
                valid_records.append(validated.model_dump())
                
            except Exception as e:
                self.logger.debug(f"Invalid review record: {e}")
                continue
        
        return pd.DataFrame(valid_records)
    
    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived fields to review data."""
        # Reviews don't need additional enrichment for now
        return df
    
    def _merge(self, new_df: pd.DataFrame, master_path: Path) -> pd.DataFrame:
        """Merge new reviews with existing master database."""
        if master_path.exists():
            existing_df = pd.read_parquet(master_path)
            
            # Append-only with deduplication
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            merged_df = combined_df.drop_duplicates(subset=['review_id'], keep='last')
            
            new_count = len(merged_df) - len(existing_df)
            self.logger.info(f"Merge: {new_count} new reviews added")
            
            return merged_df
        else:
            self.logger.info("Creating new reviews master database")
            return new_df
    
    def _get_master_path(self) -> Path:
        """Get path for reviews master database."""
        return CLEAN_DIR / "reviews_master.parquet"