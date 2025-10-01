"""Business layer: Property table logic and transformations."""
from typing import List, Optional, Dict, Any
import json
from features.property_table.models import PropertyRepository
from shared.utils import format_currency, format_number, calculate_price_per_sqft

class PropertyTableService:
    """Service for property table business logic."""
    
    def __init__(self):
        self.repo = PropertyRepository()
    
    def get_properties(
        self,
        competitors: Optional[List[str]] = None,
        communities: Optional[List[str]] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        search_address: Optional[str] = None,
        search_features: Optional[str] = None,
        sort_by: str = 'price',
        sort_order: str = 'desc',
        page: int = 1,
        per_page: int = 10
    ) -> Dict[str, Any]:
        """Get properties with applied filters and business transformations."""
        # TODO: Call repository to get raw data
        # TODO: Transform each property (calculate $/sqft, parse features JSON)
        # TODO: Format currency/numbers for display
        # TODO: Return formatted data with pagination info
        pass
    
    def get_filter_options(self) -> Dict[str, List[str]]:
        """Get available filter options (competitors, communities)."""
        # TODO: Get unique competitors from repo
        # TODO: Get unique communities from repo
        # TODO: Return dict with both lists
        pass
    
    def _transform_property(self, prop: Any, url_data: Any) -> Dict[str, Any]:
        """Transform raw property to display format."""
        # TODO: Calculate price_per_sqft
        # TODO: Parse features JSON to list
        # TODO: Map competitor_id to display name
        # TODO: Format currency and numbers
        # TODO: Return transformed dict
        pass
    
    def _parse_features(self, features_json: Optional[str]) -> List[str]:
        """Parse features JSON string to list."""
        if not features_json:
            return []
        try:
            return json.loads(features_json)
        except:
            return []
