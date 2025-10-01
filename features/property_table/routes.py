"""Presentation layer: Property table HTTP routes."""
from flask import Blueprint, render_template, request
from typing import Optional, List
from features.property_table.service import PropertyTableService

bp = Blueprint('property_table', __name__, template_folder='templates')
service = PropertyTableService()

@bp.route('/properties')
def list_properties():
    """Display property table with filters and pagination."""
    # Parse query params
    competitors = request.args.getlist('competitor')
    communities = request.args.getlist('community')
    price_min = request.args.get('price_min', type=float)
    price_max = request.args.get('price_max', type=float)
    search_address = request.args.get('search_address', type=str)
    search_features = request.args.get('search_features', type=str)
    sort_by = request.args.get('sort_by', 'price')
    sort_order = request.args.get('sort_order', 'desc')
    page = request.args.get('page', 1, type=int)
    
    # TODO: Call service.get_properties() with parsed params
    # TODO: Get filter options from service.get_filter_options()
    # TODO: Render template with data
    pass
