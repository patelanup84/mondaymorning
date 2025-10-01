"""Business layer: Property table logic and transformations."""
from typing import List, Optional, Dict, Any
import json
from features.property_table.models import PropertyRepository
from shared.utils import (
    format_currency, 
    format_number, 
    calculate_price_per_sqft,
    COMPETITOR_MAP
)

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
        # Get raw data from repository
        data = self.repo.get_all_with_filters(
            competitors=competitors,
            communities=communities,
            price_min=price_min,
            price_max=price_max,
            search_address=search_address,
            search_features=search_features,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            per_page=per_page
        )
        
        # Transform each property for display
        transformed_properties = []
        for item in data['properties']:
            transformed = self._transform_property(item['property'], item['url'])
            transformed_properties.append(transformed)
        
        return {
            'properties': transformed_properties,
            'pagination': data['pagination'],
            'filters_applied': {
                'competitors': competitors or [],
                'communities': communities or [],
                'price_range': [price_min, price_max] if price_min or price_max else None,
                'search_address': search_address,
                'search_features': search_features
            }
        }
    
    def get_filter_options(self) -> Dict[str, List[Dict[str, str]]]:
        """Get available filter options (competitors, communities)."""
        # Get unique competitor IDs
        competitor_ids = self.repo.get_unique_competitors()
        
        # Map to display names
        competitors = [
            {
                'id': cid,
                'name': COMPETITOR_MAP.get(cid, cid)  # Fallback to ID if not in CSV
            }
            for cid in competitor_ids
        ]
        
        # Get unique communities
        communities = self.repo.get_unique_communities()
        
        return {
            'competitors': competitors,
            'communities': communities
        }
    
    def _transform_property(self, prop: Any, url_data: Any) -> Dict[str, Any]:
        """Transform raw property to display format."""
        # Calculate price per sqft
        price_per_sqft = calculate_price_per_sqft(prop.price, prop.sqft)
        
        # Parse features JSON
        features = self._parse_features(prop.features)
        
        # Map competitor_id to display name
        competitor_name = COMPETITOR_MAP.get(url_data.competitor_id, url_data.competitor_id)
        
        return {
            'property_id': prop.property_id,
            'image_url': prop.main_image_url,
            'competitor_id': url_data.competitor_id,
            'competitor': competitor_name,
            'community': prop.community or 'N/A',
            'address': prop.address or 'N/A',
            'url': url_data.url,
            'price': prop.price,
            'price_formatted': format_currency(prop.price),
            'sqft': prop.sqft,
            'sqft_formatted': format_number(prop.sqft),
            'price_per_sqft': price_per_sqft,
            'price_per_sqft_formatted': format_currency(price_per_sqft) if price_per_sqft else 'N/A',
            'beds': prop.beds or 'N/A',
            'baths': prop.baths or 'N/A',
            'features': features,
            'extracted_at': prop.extracted_at
        }
    
    def _parse_features(self, features_json: Optional[str]) -> List[str]:
        """Parse features JSON string to list."""
        if not features_json:
            return []
        try:
            parsed = json.loads(features_json)
            # Handle both list and dict formats
            if isinstance(parsed, list):
                return parsed
            elif isinstance(parsed, dict):
                return list(parsed.values())
            return []
        except:
            return []
