import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Set

from .base import BaseNormalizer
from ..models import QPListing
from ..config import CLEAN_DIR


class PropertiesNormalizer(BaseNormalizer):
    """Normalizer for quick possession properties."""
    
    def __init__(self):
        super().__init__("properties")
    
    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate property data against QPListing schema."""
        import ast
        valid_records = []
        
        for _, row in df.iterrows():
            try:
                record = row.to_dict()
                
                # Convert datetime
                if pd.notna(record.get('fetched_at')):
                    record['fetched_at'] = pd.to_datetime(record['fetched_at'])
                
                # Parse string dicts back to actual dicts
                if pd.notna(record.get('metadata')) and isinstance(record['metadata'], str):
                    record['metadata'] = ast.literal_eval(record['metadata'])
                
                if pd.notna(record.get('features')) and isinstance(record['features'], str):
                    record['features'] = ast.literal_eval(record['features'])
                
                validated = QPListing(**record)
                valid_records.append(validated.model_dump())
                
            except Exception as e:
                self.logger.debug(f"Invalid property record: {e}")
                continue
        
        return pd.DataFrame(valid_records)
    
    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived fields to property data."""
        enriched = df.copy()
        
        # Calculate price per sqft
        enriched['price_per_sqft'] = None
        mask = (pd.notna(enriched['price']) & 
                pd.notna(enriched['sqft']) & 
                (enriched['sqft'] > 0))
        enriched.loc[mask, 'price_per_sqft'] = enriched.loc[mask, 'price'] / enriched.loc[mask, 'sqft']
        
        # Add processing timestamp
        enriched['processed_at'] = datetime.now()
        
        return enriched
    
    def _merge(self, new_df: pd.DataFrame, master_path: Path) -> pd.DataFrame:
        """Merge new properties with existing master database."""
        if master_path.exists():
            existing_df = pd.read_parquet(master_path)
        else:
            existing_df = pd.DataFrame()
            self.logger.info("Creating new properties master database")
        
        if existing_df.empty:
            new_df['status'] = 'new'
            return new_df
        
        # Determine status flags
        existing_ids = set(existing_df['property_id'])
        new_ids = set(new_df['property_id'])
        
        # Mark new/updated properties
        new_df['status'] = 'new'
        new_df.loc[new_df['property_id'].isin(existing_ids), 'status'] = 'updated'
        
        # Mark removed properties as sold
        removed_df = existing_df[~existing_df['property_id'].isin(new_ids)].copy()
        removed_df['status'] = 'sold'
        removed_df['processed_at'] = datetime.now()
        
        # Combine and deduplicate
        result_df = pd.concat([new_df, removed_df], ignore_index=True)
        result_df = result_df.drop_duplicates(subset=['property_id'], keep='first')
        
        self.logger.info(f"Merge: {len(new_df)} new/updated, {len(removed_df)} sold")
        return result_df
    
    def _get_master_path(self) -> Path:
        """Get path for properties master database."""
        return CLEAN_DIR / "properties_master.parquet"