#!/usr/bin/env python3

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from src.analyze.properties_table import PropertiesTableAnalyzer
from src.analyze.properties_snapshot import PropertiesSnapshotAnalyzer
from src.config import OUR_COMPANY_ID

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_analyze():
    print("Testing analysis modules with built-in benchmarking...")
    print(f"Our company ID: {OUR_COMPANY_ID}")
    print("=" * 60)
    
    # Test properties table analyzer
    table_analyzer = PropertiesTableAnalyzer()
    table_result = table_analyzer.analyze()
    
    # Test properties snapshot analyzer
    snapshot_analyzer = PropertiesSnapshotAnalyzer()
    snapshot_result = snapshot_analyzer.analyze()
    
    # Display results
    if table_result is not None:
        print(f"\n✅ properties_table analysis completed")
        print(f"Result shape: {table_result.shape}")
        print(f"Columns: {list(table_result.columns)}")
        
        print(f"\n📊 COMPETITOR KPI TABLE WITH BENCHMARKING:")
        print("-" * 50)
        
        # Show core metrics
        core_cols = ['competitor_id', 'active_listings', 'median_price', 'median_price_per_sqft', 'communities_count']
        available_core_cols = [col for col in core_cols if col in table_result.columns]
        if available_core_cols:
            print("Core Metrics:")
            print(table_result[available_core_cols].to_string(index=False))
        
        # Show ranking columns
        rank_cols = [col for col in table_result.columns if col.endswith('_rank')]
        if rank_cols:
            print(f"\nRankings:")
            rank_display = table_result[['competitor_id'] + rank_cols]
            print(rank_display.to_string(index=False))
        
        # Show delta columns
        delta_cols = [col for col in table_result.columns if '_vs_us' in col]
        if delta_cols:
            print(f"\nCompetitive Deltas (vs {OUR_COMPANY_ID}):")
            delta_display = table_result[['competitor_id'] + delta_cols]
            print(delta_display.to_string(index=False))
        elif OUR_COMPANY_ID not in table_result['competitor_id'].values:
            print(f"\n⚠️  No delta analysis - {OUR_COMPANY_ID} not found in data")
            print("Add your company data to see competitive benchmarking")
    else:
        print(f"❌ properties_table: analysis failed")
    
    if snapshot_result is not None:
        print(f"\n✅ properties_snapshot analysis completed")
        print(f"Result shape: {snapshot_result.shape}")
        
        print(f"\n📈 EXECUTIVE SNAPSHOT WITH COMPETITIVE CONTEXT:")
        print("-" * 50)
        
        display_cols = ['metric_name', 'display_value', 'competitive_context', 'our_position']
        available_display_cols = [col for col in display_cols if col in snapshot_result.columns]
        
        if available_display_cols:
            snapshot_display = snapshot_result[available_display_cols]
            
            # Format for better readability
            for _, row in snapshot_display.iterrows():
                print(f"\n{row['metric_name'].replace('_', ' ').title()}:")
                print(f"  Value: {row['display_value']}")
                if 'competitive_context' in row:
                    print(f"  Market: {row['competitive_context']}")
                if 'our_position' in row:
                    print(f"  Our Position: {row['our_position']}")
    else:
        print(f"❌ properties_snapshot: analysis failed")
    
    print("\n" + "=" * 60)
    print("BENCHMARKING FEATURES:")
    print("- Rankings: How each competitor ranks on key metrics")
    print("- Deltas: Absolute and percentage differences vs our company")
    print("- Competitive Context: Market ranges and positioning")
    print(f"- Our Position: Where {OUR_COMPANY_ID} stands vs competitors")

if __name__ == "__main__":
    test_analyze()
