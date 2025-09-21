import pandas as pd
from pathlib import Path
from typing import Optional, Dict

from .base import BaseAnalyzer
from ..config import CLEAN_DIR


class PropertiesTableAnalyzer(BaseAnalyzer):
    """Analyzer for competitor KPI comparison table with automatic benchmarking."""
    
    def __init__(self):
        super().__init__("properties_table")
    
    def _load_data(self) -> Optional[pd.DataFrame]:
        """Load active properties from master database."""
        properties_path = CLEAN_DIR / "properties_master.parquet"
        
        if not properties_path.exists():
            self.logger.error("Properties master database not found")
            return None
        
        df = pd.read_parquet(properties_path)
        active_df = df[df['status'].isin(['new', 'updated', 'active'])].copy()
        
        if active_df.empty:
            self.logger.warning("No active properties found")
            return None
        
        return active_df
    
    def _compute_analysis(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Compute competitor KPI comparison table."""
        competitor_metrics = []
        
        for competitor_id, group in data.groupby('competitor_id'):
            metrics = {
                'competitor_id': competitor_id,
                'active_listings': len(group),
                'avg_price': round(group['price'].mean()) if 'price' in group.columns and group['price'].notna().any() else None,
                'median_price': round(group['price'].median()) if 'price' in group.columns and group['price'].notna().any() else None,
                'avg_sqft': round(group['sqft'].mean()) if 'sqft' in group.columns and group['sqft'].notna().any() else None,
                'median_price_per_sqft': round(group['price_per_sqft'].median(), 2) if 'price_per_sqft' in group.columns and group['price_per_sqft'].notna().any() else None,
                'communities_count': group['community'].nunique() if 'community' in group.columns else None,
                'price_range_min': round(group['price'].min()) if 'price' in group.columns and group['price'].notna().any() else None,
                'price_range_max': round(group['price'].max()) if 'price' in group.columns and group['price'].notna().any() else None
            }
            
            competitor_metrics.append(metrics)
        
        if not competitor_metrics:
            return None
        
        result_df = pd.DataFrame(competitor_metrics)
        result_df = result_df.sort_values('active_listings', ascending=False)
        
        return result_df
    
    def _get_benchmark_config(self) -> Dict[str, Dict]:
        """Configure benchmarking for key competitive metrics."""
        return {
            'active_listings': {
                'rank_ascending': False,
                'include_percent': True,
                'format': 'number'
            },
            'median_price': {
                'rank_ascending': True,
                'include_percent': True,
                'format': 'currency'
            },
            'median_price_per_sqft': {
                'rank_ascending': True,
                'include_percent': True,
                'format': 'currency'
            },
            'communities_count': {
                'rank_ascending': False,
                'include_percent': True,
                'format': 'number'
            }
        }