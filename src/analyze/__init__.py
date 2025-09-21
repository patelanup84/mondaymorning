from typing import List, Dict, Optional
import pandas as pd

from .base import BaseAnalyzer
from .properties_table import PropertiesTableAnalyzer
from .properties_snapshot import PropertiesSnapshotAnalyzer

# Registry mapping analysis names to analyzer classes
ANALYZERS = {
    "properties_table": PropertiesTableAnalyzer,
    "properties_snapshot": PropertiesSnapshotAnalyzer
}


def compute_analysis(analysis_names: List[str], **kwargs) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Run specified analysis modules and return results.
    
    Args:
        analysis_names: List of analysis modules to run
        **kwargs: Additional parameters passed to analyzers
        
    Returns:
        Dict mapping analysis name to DataFrame result (None if failed)
    """
    results = {}
    
    for analysis_name in analysis_names:
        analyzer = get_analyzer(analysis_name, **kwargs)
        if analyzer:
            result = analyzer.analyze()
            results[analysis_name] = result
        else:
            results[analysis_name] = None
            
    return results


def get_analyzer(analysis_name: str, **kwargs) -> Optional[BaseAnalyzer]:
    """Get analyzer instance by name."""
    if analysis_name in ANALYZERS:
        analyzer_class = ANALYZERS[analysis_name]
        return analyzer_class(**kwargs)
    return None


def list_analyzers() -> List[str]:
    """List available analyzer names."""
    return list(ANALYZERS.keys())