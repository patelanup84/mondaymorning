"""Data layer: Property models and database queries."""
from typing import List, Optional, Dict, Any
from shared.db import db
from datetime import datetime
from sqlalchemy import or_, and_

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
        # Base query: join properties with urls
        query = db.session.query(Property, Url).join(
            Url, Property.property_id == Url.property_id
        )
        
        # Filter: active status only
        query = query.filter(Url.status == 'active')
        
        # Filter: competitors
        if competitors and len(competitors) > 0:
            query = query.filter(Url.competitor_id.in_(competitors))
        
        # Filter: communities
        if communities and len(communities) > 0:
            query = query.filter(Property.community.in_(communities))
        
        # Filter: price range
        if price_min is not None:
            query = query.filter(Property.price >= price_min)
        if price_max is not None:
            query = query.filter(Property.price <= price_max)
        
        # Search: address (partial match, case-insensitive)
        if search_address:
            query = query.filter(Property.address.ilike(f'%{search_address}%'))
        
        # Search: features (OR logic for multiple keywords)
        if search_features:
            keywords = [k.strip() for k in search_features.split(',') if k.strip()]
            if keywords:
                feature_filters = [Property.features.ilike(f'%{kw}%') for kw in keywords]
                query = query.filter(or_(*feature_filters))
        
        # Sorting
        sort_column = getattr(Property, sort_by, None) or getattr(Url, sort_by, None)
        if sort_column is not None:
            if sort_order.lower() == 'asc':
                query = query.order_by(sort_column.asc())
            else:
                query = query.order_by(sort_column.desc())
        
        # Pagination
        total = query.count()
        results = query.limit(per_page).offset((page - 1) * per_page).all()
        
        # Transform to list of dicts
        properties = []
        for prop, url in results:
            properties.append({
                'property': prop,
                'url': url
            })
        
        return {
            'properties': properties,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': (total + per_page - 1) // per_page
            }
        }
    
    @staticmethod
    def get_unique_competitors() -> List[str]:
        """Get list of all unique competitor_ids from active properties."""
        result = db.session.query(Url.competitor_id).filter(
            Url.status == 'active'
        ).distinct().all()
        return [r[0] for r in result if r[0]]
    
    @staticmethod
    def get_unique_communities() -> List[str]:
        """Get list of all unique communities from active properties."""
        result = db.session.query(Property.community).join(
            Url, Property.property_id == Url.property_id
        ).filter(
            Url.status == 'active'
        ).distinct().all()
        return sorted([r[0] for r in result if r[0]])