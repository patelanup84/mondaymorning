#!/usr/bin/env python3

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import asyncio
import logging
from datetime import datetime

from src.models import QPCollectorConfig, ReviewsCollectorConfig
from src.collect import get_collector

# Setup logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def test_qp_collector():
    """Test Quick Possession collector."""
    print("\n" + "="*50)
    print("TESTING QP COLLECTOR")
    print("="*50)
    
    # Create config
    config = QPCollectorConfig(
        competitors=[
            {
                'code': 'PRM',
                'name': 'Prominent Homes',
                'domain': 'prominenthomes.ca',
                'pattern': '*/quick-possessions/*?'
            },
        ],
        output_path=Path("data/test_qp_output.csv"),
        url_limit_per_competitor=2,  # Small limit for testing
        llm_provider="openai/gpt-4o-mini"
    )
    
    # Get collector and run
    collector = get_collector("quickpossession")
    result = await collector.collect(config)
    
    print(f"\nResults:")
    print(f"- Collector: {result.collector_name}")
    print(f"- Records: {len(result.data)}")
    print(f"- Output: {result.output_path}")
    print(f"- Metadata: {result.metadata}")
    
    return result

async def test_reviews_collector():
    """Test Reviews collector."""
    print("\n" + "="*50)
    print("TESTING REVIEWS COLLECTOR")
    print("="*50)
    
    # Create config (you'll need real DataForSEO credentials)
    config = ReviewsCollectorConfig(
        competitors=[
            {
                "competitor_id": "Sterl-1",
                "name": "Sterling Homes",
                "cid": "2902061545298546083"
            },
        ],
        output_path=Path("data/test_reviews_output.csv"),
        review_depth=5,  # Small number for testing
        dataforseo_username="your_username",  # Replace with real creds
        dataforseo_password="your_password"   # Replace with real creds
    )
    
    # Get collector and run
    collector = get_collector("reviews")
    result = await collector.collect(config)
    
    print(f"\nResults:")
    print(f"- Collector: {result.collector_name}")
    print(f"- Records: {len(result.data)}")
    print(f"- Output: {result.output_path}")
    print(f"- Metadata: {result.metadata}")
    
    return result

async def main():
    """Run tests."""
    try:
        # Test QP collector (requires OpenAI API key in environment)
        qp_result = await test_qp_collector()
        
        # Test Reviews collector (requires DataForSEO credentials)
        # Uncomment when you have credentials:
        # reviews_result = await test_reviews_collector()
        
        print("\n" + "="*50)
        print("TESTS COMPLETE")
        print("="*50)
        
    except Exception as e:
        print(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
