from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from typing import Optional, Dict
import logging

from ..config import CLEAN_DIR, ENRICHED_DIR, OUR_COMPANY_ID


class BaseAnalyzer(ABC):
    """Abstract base class for analysis modules with built-in benchmarking."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"analysis.{name}")
        self.our_company_id = OUR_COMPANY_ID
    
    def analyze(self) -> Optional[pd.DataFrame]:
        """Main analysis pipeline with automatic benchmarking."""
        try:
            self.logger.info(f"Starting {self.name} analysis")
            
            data = self._load_data()
            if data is None or data.empty:
                self.logger.warning("No data available for analysis")
                return None
            
            result = self._compute_analysis(data)
            if result is None or result.empty:
                self.logger.warning("No results from analysis computation")
                return None
            
            benchmarked_result = self._apply_benchmarking(result)
            self._save_results(benchmarked_result)
            
            self.logger.info(f"Analysis complete: {len(benchmarked_result)} records")
            return benchmarked_result
            
        except Exception as e:
            self.logger.error(f"Analysis failed: {e}")
            return None
    
    @abstractmethod
    def _load_data(self) -> Optional[pd.DataFrame]:
        """Load required data for analysis."""
        pass
    
    @abstractmethod
    def _compute_analysis(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute the focused analysis result."""
        pass
    
    def _get_benchmark_config(self) -> Dict[str, Dict]:
        """Override this to specify which columns to benchmark."""
        return {}
    
    def _apply_benchmarking(self, result: pd.DataFrame) -> pd.DataFrame:
        """Apply benchmarking analysis to results."""
        if 'competitor_id' not in result.columns:
            return result
        
        config = self._get_benchmark_config()
        if not config:
            return result
        
        benchmarked = result.copy()
        
        # Add rankings
        for column, settings in config.items():
            if column not in result.columns:
                continue
            
            ascending = settings.get('rank_ascending', True)
            rank_col = f"{column}_rank"
            benchmarked[rank_col] = result[column].rank(ascending=ascending, method='min').astype('Int64')
        
        # Add deltas if our company exists
        if self.our_company_id in result['competitor_id'].values:
            our_row = result[result['competitor_id'] == self.our_company_id].iloc[0]
            
            for column, settings in config.items():
                if column not in result.columns or pd.isna(our_row[column]):
                    continue
                
                our_value = our_row[column]
                
                # Absolute delta
                abs_delta_col = f"{column}_delta_abs"
                benchmarked[abs_delta_col] = result[column] - our_value
                
                # Percentage delta
                if settings.get('include_percent', False) and our_value != 0:
                    pct_delta_col = f"{column}_delta_pct"
                    benchmarked[pct_delta_col] = ((result[column] - our_value) / our_value * 100).round(1)
                
                # Human readable description
                vs_us_col = f"{column}_vs_us"
                benchmarked[vs_us_col] = benchmarked.apply(
                    lambda row: self._format_delta(row[column], our_value, settings), axis=1
                )
        
        return benchmarked
    
    def _format_delta(self, competitor_value: float, our_value: float, settings: Dict) -> str:
        """Format delta as human readable string."""
        if pd.isna(competitor_value) or pd.isna(our_value):
            return "N/A"
        
        delta = competitor_value - our_value
        if abs(delta) < 0.01:
            return "Same as us"
        
        format_type = settings.get('format', 'number')
        if format_type == 'currency':
            abs_str = f"${abs(delta):,.0f}"
        else:
            abs_str = f"{abs(delta):,.0f}"
        
        direction = "above" if delta > 0 else "below"
        
        if settings.get('include_percent', False) and our_value != 0:
            pct = abs(delta) / our_value * 100
            return f"{abs_str} ({pct:.1f}%) {direction} us"
        else:
            return f"{abs_str} {direction} us"
    
    def _save_results(self, result: pd.DataFrame) -> None:
        """Save analysis results to files."""
        parquet_path = ENRICHED_DIR / f"{self.name}.parquet"
        result.to_parquet(parquet_path, index=False)
        
        csv_path = ENRICHED_DIR / f"{self.name}.csv"
        result.to_csv(csv_path, index=False)
        
        self.logger.info(f"Results saved: {parquet_path}")