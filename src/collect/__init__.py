from .base import BaseCollector
from .quick_possession import QPCollector
from .google_reviews import ReviewsCollector

# Manual registry for available collectors
COLLECTORS = {
    "quickpossession": QPCollector,
    "reviews": ReviewsCollector
}

def get_collector(name: str) -> BaseCollector:
    """Get collector instance by name."""
    if name not in COLLECTORS:
        raise ValueError(f"Unknown collector: {name}. Available: {list(COLLECTORS.keys())}")
    
    return COLLECTORS[name]()

def list_collectors() -> list:
    """List available collector names."""
    return list(COLLECTORS.keys())