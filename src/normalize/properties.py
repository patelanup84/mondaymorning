import pandas as pd
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Set

from .base import BaseNormalizer
from ..models import QPListing
from ..config import CLEAN_DIR, RAW_DIR


class PropertiesNormalizer(BaseNormalizer):
    """Normalizer for quick possession properties."""
    
    def __init__(self):
        super().__init__("properties")

    def _load_raw_data(self, collector_name: str) -> pd.DataFrame | None:
        """
        Loads property data from the collector's SQLite database.
        It specifically fetches records where extraction was successful.
        """
        db_path = RAW_DIR / f"{collector_name}.db"
        if not db_path.exists():
            self.logger.error(f"SQLite database not found at {db_path}")
            return None

        try:
            con = sqlite3.connect(db_path)
            # Query only for records that were successfully extracted.
            query = "SELECT * FROM properties WHERE extraction_status = 'success'"
            df = pd.read_sql_query(query, con)
            con.close()
            
            self.logger.info(f"Loaded {len(df)} successful records from {db_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load data from SQLite DB: {e}")
            return None

    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate property data against QPListing schema."""
        valid_records = []
        
        for _, row in df.iterrows():
            try:
                record = row.to_dict()
                
                # Convert datetime fields from string format
                if pd.notna(record.get('discovered_at')):
                    record['discovered_at'] = pd.to_datetime(record['discovered_at'])
                
                if pd.notna(record.get('extracted_at')):
                    record['extracted_at'] = pd.to_datetime(record['extracted_at'])
                
                # *** FIX: Use json.loads for JSON strings ***
                if pd.notna(record.get('metadata')) and isinstance(record['metadata'], str):
                    record['metadata'] = json.loads(record['metadata'])
                
                if pd.notna(record.get('features')) and isinstance(record['features'], str):
                    record['features'] = json.loads(record['features'])

                # Now, validation should pass without any patching.
                validated = QPListing(**record)
                valid_records.append(validated.model_dump())
                
            except Exception as e:
                self.logger.debug(f"Invalid property record: {row.get('url', 'N/A')}: {e}")
                continue
        
        self.logger.info(f"Validation successful for {len(valid_records)}/{len(df)} records.")
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
        
        # Improved logging for merge status
        new_count = len(new_df[new_df['status']=='new'])
        updated_count = len(new_df[new_df['status']=='updated'])
        sold_count = len(removed_df)
        self.logger.info(f"Merge: {new_count} new, {updated_count} updated, {sold_count} sold")

        return result_df
    
    def _get_master_path(self) -> Path:
        """Get path for properties master database."""
        return CLEAN_DIR / "properties_master.parquet"