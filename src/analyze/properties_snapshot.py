import pandas as pd
from pathlib import Path
from typing import Optional, Dict

from .base import BaseAnalyzer
from ..config import CLEAN_DIR


class PropertiesSnapshotAnalyzer(BaseAnalyzer):
    """Analyzer for executive summary snapshot metrics with competitive positioning."""
    
    def __init__(self):
        super().__init__("properties_snapshot")
    
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
        """Compute executive summary snapshot with competitive context."""
        snapshot_metrics = []
        
        # Calculate competitor stats for context
        competitor_stats = data.groupby('competitor_id').agg({
            'price': ['count', 'mean', 'median'],
            'price_per_sqft': 'median',
            'community': 'nunique'
        }).round(2)
        
        competitor_stats.columns = ['_'.join(col).strip() for col in competitor_stats.columns]
        competitor_stats = competitor_stats.reset_index()
        
        # Market overview
        total_listings = len(data)
        total_competitors = data['competitor_id'].nunique()
        total_communities = data['community'].nunique() if 'community' in data.columns else 0
        
        # Our company data
        our_data = data[data['competitor_id'] == self.our_company_id] if self.our_company_id in data['competitor_id'].values else pd.DataFrame()
        
        # Market overview with our position
        snapshot_metrics.append({
            'metric_type': 'market_overview',
            'metric_name': 'total_active_listings',
            'value': total_listings,
            'display_value': f"{total_listings:,}",
            'competitive_context': f"{total_competitors} competitors tracked",
            'our_position': f"We have {len(our_data)} listings" if not our_data.empty else "We have no active listings"
        })
        
        snapshot_metrics.append({
            'metric_type': 'market_overview',
            'metric_name': 'total_competitors',
            'value': total_competitors,
            'display_value': str(total_competitors),
            'competitive_context': f"Including us" if not our_data.empty else "We are not currently active",
            'our_position': "Active competitor" if not our_data.empty else "Not in market"
        })
        
        # Price metrics with competitive positioning
        if 'price' in data.columns and data['price'].notna().any():
            market_avg_price = data['price'].mean()
            
            our_price_position = "N/A"
            if not our_data.empty and 'price' in our_data.columns:
                our_avg_price = our_data['price'].mean()
                price_rank = (competitor_stats['price_mean'] <= our_avg_price).sum()
                our_price_position = f"Ranked {price_rank}/{total_competitors} (${our_avg_price:,.0f} avg)"
            
            snapshot_metrics.append({
                'metric_type': 'pricing',
                'metric_name': 'market_avg_price',
                'value': market_avg_price,
                'display_value': f"${market_avg_price:,.0f}",
                'competitive_context': f"Range: ${data['price'].min():,.0f} - ${data['price'].max():,.0f}",
                'our_position': our_price_position
            })
        
        # Price per sqft metrics
        if 'price_per_sqft' in data.columns and data['price_per_sqft'].notna().any():
            market_median_ppsf = data['price_per_sqft'].median()
            
            our_ppsf_position = "N/A"
            if not our_data.empty and 'price_per_sqft' in our_data.columns:
                our_median_ppsf = our_data['price_per_sqft'].median()
                ppsf_rank = (competitor_stats['price_per_sqft_median'] <= our_median_ppsf).sum()
                our_ppsf_position = f"Ranked {ppsf_rank}/{total_competitors} (${our_median_ppsf:.0f}/sqft)"
            
            snapshot_metrics.append({
                'metric_type': 'pricing',
                'metric_name': 'market_median_ppsf',
                'value': market_median_ppsf,
                'display_value': f"${market_median_ppsf:.0f}/sqft",
                'competitive_context': f"Range: ${data['price_per_sqft'].min():.0f} - ${data['price_per_sqft'].max():.0f}/sqft",
                'our_position': our_ppsf_position
            })
        
        # Inventory leaders
        top_competitors = competitor_stats.nlargest(3, 'price_count')
        for i, (_, comp) in enumerate(top_competitors.iterrows()):
            comp_id = comp['competitor_id']
            count = int(comp['price_count'])
            
            position_note = ""
            if comp_id == self.our_company_id:
                position_note = " (Us)"
            
            snapshot_metrics.append({
                'metric_type': 'inventory_leaders',
                'metric_name': f'inventory_leader_{i+1}',
                'value': count,
                'display_value': f"{comp_id}: {count} listings{position_note}",
                'competitive_context': f"#{i+1} by active inventory",
                'our_position': f"We rank #{(competitor_stats['price_count'] >= len(our_data)).sum()}/{total_competitors}" if not our_data.empty else "Not ranked"
            })
        
        if not snapshot_metrics:
            return None
        
        return pd.DataFrame(snapshot_metrics)
    
    def _get_benchmark_config(self) -> Dict[str, Dict]:
        """No additional benchmarking needed - competitive context built into metrics."""
        return {}