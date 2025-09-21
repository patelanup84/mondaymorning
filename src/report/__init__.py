from typing import List, Dict, Optional
from pathlib import Path

from src.report.base import BaseReport
from src.report.properties_report import PropertiesReport

# Registry mapping report names to report classes
REPORTS = {
    "properties": PropertiesReport
}


def generate_reports(report_names: List[str], **kwargs) -> Dict[str, Optional[Path]]:
    """
    Generate specified reports and return results.
    
    Args:
        report_names: List of report names to generate
        **kwargs: Additional parameters passed to report generators
        
    Returns:
        Dict mapping report name to output path (None if failed)
    """
    results = {}
    
    for report_name in report_names:
        report_generator = get_report_generator(report_name, **kwargs)
        if report_generator:
            result_path = report_generator.render()
            results[report_name] = result_path
        else:
            results[report_name] = None
            
    return results


def get_report_generator(report_name: str, **kwargs) -> Optional[BaseReport]:
    """Get report generator instance by name."""
    if report_name in REPORTS:
        report_class = REPORTS[report_name]
        return report_class(**kwargs)
    return None


def list_reports() -> List[str]:
    """List available report names."""
    return list(REPORTS.keys())