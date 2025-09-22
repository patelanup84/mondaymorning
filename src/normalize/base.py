from abc import ABC, abstractmethod
import pandas as pd
import json
from pathlib import Path
from typing import Optional
from datetime import datetime
import logging
import glob

from ..config import CLEAN_DIR, RAW_DIR


class BaseNormalizer(ABC):
    """Abstract base class for data normalizers."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"normalize.{name}")
    
    def normalize(self, collector_name: str) -> Optional[Path]:
        """Main normalization pipeline."""
        try:
            self.logger.info(f"Processing {self.name} data for collector: {collector_name}")
            
            # Load data using the new abstract method
            raw_df = self._load_raw_data(collector_name)
            if raw_df is None or raw_df.empty:
                self.logger.warning("No raw data loaded to process")
                return None
            
            validated_df = self._validate(raw_df)
            if validated_df.empty:
                self.logger.warning("No valid records after validation")
                return None
            
            # Enrich with derived fields
            enriched_df = self._enrich(validated_df)
            
            # Merge with master database
            master_path = self._get_master_path()
            merged_df = self._merge(enriched_df, master_path)

            # *** FIX: Convert columns with dicts/lists to JSON strings before saving ***
            for col in merged_df.select_dtypes(include=['object']).columns:
                # Check if any non-null values in the column are lists or dicts
                if any(isinstance(i, (dict, list)) for i in merged_df[col].dropna()):
                    self.logger.info(f"Standardizing complex column '{col}' to JSON strings before saving.")
                    merged_df[col] = merged_df[col].apply(
                        lambda x: json.dumps(x) if x is not None else None
                    )
            
            # Save results
            merged_df.to_parquet(master_path, index=False)
            
            csv_path = master_path.with_suffix('.csv')
            merged_df.to_csv(csv_path, index=False)
            
            self.logger.info(f"Processed: {len(merged_df)} total records")
            return master_path
            
        except Exception as e:
            self.logger.error(f"Normalization failed: {e}", exc_info=True)
            return None
    
    @abstractmethod
    def _load_raw_data(self, collector_name: str) -> Optional[pd.DataFrame]:
        """Load raw data from the collector's output source (e.g., SQLite, CSV)."""
        pass
    
    @abstractmethod
    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data against schema."""
        pass
    
    @abstractmethod
    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived fields."""
        pass
    
    @abstractmethod
    def _merge(self, new_df: pd.DataFrame, master_path: Path) -> pd.DataFrame:
        """Merge with existing master database."""
        pass
    
    @abstractmethod
    def _get_master_path(self) -> Path:
        """Get path for master database."""
        pass