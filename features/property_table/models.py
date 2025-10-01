"""Data layer: Property models and database queries."""
from typing import List, Optional, Dict, Any
from shared.db import db
from datetime import datetime

class Property(db.Model):
    """Property entity from qp database."""
    __tablename__ = 'properties'
    
    property_id = db.Column(db.Text, primary_key=True)
    address = db.Column(db.Text)
    community = db.Column(db.Text)
    price = db.Column(db.Float)
    sqft = db.Column(db.Float)
    beds = db.Column(db.Integer)
    baths = db.Column(db.Float)
    main_image_url = db.Column(db.Text)
    features = db.Column(db.Text)  # JSON string
    extracted_at = db.Column(db.DateTime)

class Url(db.Model):
    """URL entity for property links and competitor mapping."""
    __tablename__ = 'urls'
    
    url = db.Column(db.Text, primary_key=True)
    property_id = db.Column(db.Text, db.ForeignKey('properties.property_id'))
    competitor_id = db.Column(db.Text)
    status = db.Column(db.Text)
    first_seen = db.Column(db.DateTime)
    last_seen = db.Column(db.DateTime)

class PropertyRepository:
    """Repository for property queries."""
    
    @staticmethod
    def get_all_with_filters(
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
        """Query properties with filters, sorting, and pagination."""
        # TODO: Build query with joins to urls table
        # TODO: Apply filters (competitor, community, price range)
        # TODO: Apply search (address, features)
        # TODO: Apply sorting
        # TODO: Paginate results
        # TODO: Return dict with properties and pagination metadata
        pass
    
    @staticmethod
    def get_unique_competitors() -> List[str]:
        """Get list of all unique competitor_ids."""
        # TODO: Query distinct competitor_id from urls table
        pass
    
    @staticmethod
    def get_unique_communities() -> List[str]:
        """Get list of all unique communities."""
        # TODO: Query distinct community from properties table
        pass
