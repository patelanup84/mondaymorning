#!/usr/bin/env python3

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from src.normalize import build_canonical

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_normalize():
    print("Testing normalize module...")
    
    # Test with quickpossession data
    results = build_canonical(['quickpossession'])
    
    print(f"Results: {results}")
    
    for data_type, path in results.items():
        if path:
            print(f"✅ {data_type}: {path}")
        else:
            print(f"❌ {data_type}: skipped")

if __name__ == "__main__":
    test_normalize()
