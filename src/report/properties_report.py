import pandas as pd
from typing import Optional, Dict

from src.report.base import BaseReport


class PropertiesReport(BaseReport):
    """Report generator for properties competitive analysis."""
    
    def __init__(self):
        super().__init__("properties_report")
    
    def _load_analysis_data(self) -> Optional[Dict[str, pd.DataFrame]]:
        """Load properties analysis data from enriched directory."""
        data = {}
        
        # Load properties snapshot (executive summary)
        snapshot_df = self._load_parquet_safe("properties_snapshot.parquet")
        if snapshot_df is not None:
            data['snapshot'] = snapshot_df
        
        # Load properties table (competitor comparison)
        table_df = self._load_parquet_safe("properties_table.parquet")
        if table_df is not None:
            data['table'] = table_df
        
        if not data:
            return None
        
        return data
    
    def _get_template_name(self) -> str:
        """Get the properties report template name."""
        return "properties_report.html.j2"