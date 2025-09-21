# =============================================================================
# Core Data Models and Configuration Schemas
# =============================================================================

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from pathlib import Path
from datetime import datetime


# =============================================================================
# Business Constants
# =============================================================================

OUR_COMPANY_ID = "PRM"


# =============================================================================
# Data Models (Domain Objects)
# =============================================================================

class QPListing(BaseModel):
    """Quick Possession property listing data model."""
    # URL Discovery phase
    property_id: str
    competitor_id: str
    url: str
    fetched_at: datetime
    metadata: Optional[Dict[str, Any]] = None
    
    # LLM Extraction phase
    address: str
    community: Optional[str] = None
    price: Optional[float] = None
    sqft: Optional[float] = None
    beds: Optional[int] = None
    baths: Optional[float] = None
    main_image_url: Optional[str] = None
    features: Optional[Dict[str, Any]] = None


class ReviewListing(BaseModel):
    """Google review data model."""
    review_id: str
    competitor_id: str
    cid: str
    fetched_at: datetime
    review_text: Optional[str] = None
    rating: Optional[float] = None
    timestamp: Optional[str] = None
    reviewer_name: Optional[str] = None


# =============================================================================
# Collector Configuration Models
# =============================================================================

class BaseCollectorConfig(BaseModel):
    """Base configuration for all collectors."""
    competitors: List[Dict[str, Any]]
    output_path: Path
    limits: Optional[Dict[str, int]] = None
    request_delay: Optional[float] = 2.0


class QPCollectorConfig(BaseCollectorConfig):
    """Configuration for Quick Possession collector."""
    url_limit_per_competitor: int = 5
    llm_provider: str = "openai/gpt-4o-mini"
    location_name: Optional[str] = "Calgary,Alberta,Canada"
    language_name: Optional[str] = "English"


class ReviewsCollectorConfig(BaseCollectorConfig):
    """Configuration for Google Reviews collector."""
    review_depth: int = 10  # Reviews per competitor
    dataforseo_username: str
    dataforseo_password: str
    location_name: str = "Calgary,Alberta,Canada"
    language_name: str = "English"


# =============================================================================
# Collection Result Models
# =============================================================================

class CollectionResult(BaseModel):
    """Standardized result from any collector."""
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    output_path: Path
    collector_name: str
    collection_time: datetime
    
    class Config:
        arbitrary_types_allowed = True