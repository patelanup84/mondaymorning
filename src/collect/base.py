from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple
import pandas as pd
import logging

from ..models import BaseCollectorConfig, CollectionResult


class BaseCollector(ABC):
    """Abstract base class for all data collectors using pipeline pattern."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"collector.{name}")
        self.raw_data = []
        self.processed_data = []
        self.errors = []
        self.stats = {"total_attempted": 0, "successful": 0, "failed": 0}
    
    async def collect(self, config: BaseCollectorConfig) -> CollectionResult:
        """Main pipeline orchestrator."""
        try:
            # Pipeline stages
            setup_success = self._setup(config)
            if not setup_success:
                raise Exception("Setup stage failed")
            
            collect_success = await self._collect_raw(config)
            if not collect_success:
                raise Exception("Collection stage failed")
            
            transform_success = await self._transform(config)
            if not transform_success:
                raise Exception("Transform stage failed")
            
            result = self._finalize(config)
            return result
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
    
    def _setup(self, config: BaseCollectorConfig) -> bool:
        """Setup logging, output paths, and initialize tracking."""
        try:
            self.logger.info(f"Starting {self.name} collection")
            
            # Ensure output directory exists
            config.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Initialize tracking
            self.raw_data = []
            self.processed_data = []
            self.errors = []
            self.stats = {"total_attempted": 0, "successful": 0, "failed": 0}
            
            self.logger.info(f"Processing {len(config.competitors)} competitors")
            return True
            
        except Exception as e:
            self.logger.error(f"Setup failed: {str(e)}")
            return False
    
    @abstractmethod
    async def _collect_raw(self, config: BaseCollectorConfig) -> bool:
        """Collect raw data from sources. Must populate self.raw_data and self.stats."""
        pass
    
    @abstractmethod
    async def _transform(self, config: BaseCollectorConfig) -> bool:
        """Transform raw data to schema-compliant format. Must populate self.processed_data."""
        pass
    
    def _finalize(self, config: BaseCollectorConfig) -> CollectionResult:
        """Save results, log statistics, and create final result object."""
        # Save to CSV
        save_success, save_message = self._save_to_csv(self.processed_data, config.output_path)
        if save_success:
            self.logger.info(save_message)
        else:
            self.logger.error(save_message)
        
        # Log collection statistics
        self._log_collection_stats(
            self.stats["total_attempted"],
            self.stats["successful"], 
            self.stats["failed"]
        )
        
        # Create metadata
        metadata = {
            "total_attempted": self.stats["total_attempted"],
            "successful": self.stats["successful"],
            "failed": self.stats["failed"],
            "errors": self.errors,
            "collection_duration": "calculated_in_subclass"  # Can be enhanced later
        }
        
        # Create final result
        return self._create_result(self.processed_data, config.output_path, metadata)
    
    def _save_to_csv(self, data: List[Dict[str, Any]], output_path: Path) -> Tuple[bool, str]:
        """Save data to CSV file. Returns (success, message)."""
        try:
            if not data:
                return False, "No data to save"
            
            df = pd.DataFrame(data)
            df.to_csv(output_path, index=False)
            return True, f"Saved {len(data)} records to {output_path}"
        
        except Exception as e:
            return False, f"Failed to save CSV: {str(e)}"
    
    def _create_result(
        self, 
        data: List[Dict[str, Any]], 
        output_path: Path,
        metadata: Dict[str, Any] = None
    ) -> CollectionResult:
        """Create standardized collection result."""
        return CollectionResult(
            data=data,
            metadata=metadata or {},
            output_path=output_path,
            collector_name=self.name,
            collection_time=datetime.now()
        )
    
    def _log_collection_stats(self, total_attempted: int, successful: int, failed: int) -> None:
        """Log collection statistics."""
        self.logger.info(f"Collection complete: {successful}/{total_attempted} successful, {failed} failed")
        
        if failed > 0:
            failure_rate = (failed / total_attempted) * 100
            if failure_rate > 50:
                self.logger.warning(f"High failure rate: {failure_rate:.1f}%")
            else:
                self.logger.info(f"Failure rate: {failure_rate:.1f}%")