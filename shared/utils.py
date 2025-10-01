"""Common utility functions."""
from typing import Any, Dict, List, Optional
import csv
import os

def format_currency(amount: Optional[float]) -> str:
    """Format number as currency (e.g., 650000 -> $650,000)."""
    if amount is None:
        return "N/A"
    return f"${amount:,.0f}"

def format_number(num: Optional[float]) -> str:
    """Format number with commas (e.g., 2400 -> 2,400)."""
    if num is None:
        return "N/A"
    return f"{num:,.0f}"

def calculate_price_per_sqft(price: Optional[float], sqft: Optional[float]) -> Optional[float]:
    """Calculate price per square foot."""
    if price and sqft and sqft > 0:
        return round(price / sqft, 2)
    return None

def load_competitors_csv() -> Dict[str, str]:
    """Load competitor mapping from CSV (competitor_id -> name)."""
    csv_path = os.path.join('data', 'reference', 'competitors.csv')
    competitor_map = {}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            competitor_id = row.get('competitor_id', '').strip()
            name = row.get('name', '').strip()
            if competitor_id and name:
                competitor_map[competitor_id] = name
    
    return competitor_map

# Load once at module import
COMPETITOR_MAP = load_competitors_csv()