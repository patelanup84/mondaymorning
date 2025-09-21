from typing import List, Dict, Optional
from pathlib import Path

from .base import BaseNormalizer
from .properties import PropertiesNormalizer
from .reviews import ReviewsNormalizer

# Registry mapping collector names to normalizers
NORMALIZERS = {
    "quickpossession": PropertiesNormalizer,
    "reviews": ReviewsNormalizer
}


def build_canonical(collector_names: List[str]) -> Dict[str, Optional[Path]]:
    """
    Normalize collector outputs into canonical master databases.
    
    Args:
        collector_names: List of collector names to process
        
    Returns:
        Dict mapping data type to output path (None if skipped)
    """
    results = {}
    
    for collector_name in collector_names:
        normalizer = get_normalizer(collector_name)
        if normalizer:
            result_path = normalizer.normalize(collector_name)
            data_type = _get_data_type(collector_name)
            results[data_type] = result_path
        else:
            results[collector_name] = None
            
    return results


def get_normalizer(collector_name: str) -> Optional[BaseNormalizer]:
    """Get normalizer instance for collector."""
    if collector_name in NORMALIZERS:
        return NORMALIZERS[collector_name]()
    return None


def list_normalizers() -> List[str]:
    """List available normalizer names."""
    return list(NORMALIZERS.keys())


def _get_data_type(collector_name: str) -> str:
    """Map collector name to data type."""
    mapping = {
        "quickpossession": "properties",
        "reviews": "reviews"
    }
    return mapping.get(collector_name, collector_name)