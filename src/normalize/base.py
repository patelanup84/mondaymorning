from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from typing import Optional
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
            # Find input file
            input_file = self._find_latest_output(collector_name)
            if not input_file:
                self.logger.warning(f"No output file found for {collector_name}")
                return None
            
            self.logger.info(f"Processing {self.name} from {input_file}")
            
            # Read and validate data
            raw_df = pd.read_csv(input_file)
            if raw_df.empty:
                self.logger.warning("No data to process")
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
            
            # Save results
            merged_df.to_parquet(master_path, index=False)
            
            csv_path = master_path.with_suffix('.csv')
            merged_df.to_csv(csv_path, index=False)
            
            self.logger.info(f"Processed: {len(merged_df)} total records")
            return master_path
            
        except Exception as e:
            self.logger.error(f"Normalization failed: {e}")
            return None
    
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
    
    def _find_latest_output(self, collector_name: str) -> Optional[Path]:
        """Find most recent collector output file."""
        pattern = RAW_DIR / f"*{collector_name}*.csv"
        files = glob.glob(str(pattern))
        
        if not files:
            return None
        
        latest = max(files, key=lambda f: Path(f).stat().st_mtime)
        return Path(latest)