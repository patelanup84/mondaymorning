"""Presentation layer: Property table HTTP routes."""
from flask import Blueprint, render_template, request
from typing import Optional, List
from features.property_table.service import PropertyTableService

bp = Blueprint('property_table', __name__, template_folder='templates')
service = PropertyTableService()

@bp.route('/properties')
def list_properties():
    """Display property table with filters and pagination."""
    # Parse query params - competitors (multi-select)
    competitors = request.args.getlist('competitor')
    competitors = [c for c in competitors if c]  # Remove empty strings
    
    # Parse query params - communities (multi-select)
    communities = request.args.getlist('community')
    communities = [c for c in communities if c]
    
    # Parse query params - price range
    price_min = request.args.get('price_min', type=float)
    price_max = request.args.get('price_max', type=float)
    
    # Parse query params - search
    search_address = request.args.get('search_address', '').strip()
    search_features = request.args.get('search_features', '').strip()
    
    # Parse query params - sorting
    sort_by = request.args.get('sort_by', 'price')
    sort_order = request.args.get('sort_order', 'desc')
    
    # Parse query params - pagination
    page = request.args.get('page', 1, type=int)
    
    # Get properties from service
    data = service.get_properties(
        competitors=competitors if competitors else None,
        communities=communities if communities else None,
        price_min=price_min,
        price_max=price_max,
        search_address=search_address if search_address else None,
        search_features=search_features if search_features else None,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        per_page=10
    )
    
    # Get filter options for dropdowns
    filter_options = service.get_filter_options()
    
    # Render template
    return render_template(
        'property_table.html',
        properties=data['properties'],
        pagination=data['pagination'],
        filters_applied=data['filters_applied'],
        filter_options=filter_options,
        current_sort_by=sort_by,
        current_sort_order=sort_order
    )
