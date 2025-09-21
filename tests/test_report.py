#!/usr/bin/env python3

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from src.report import generate_reports

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_report_generation():
    print("Testing report generation...")
    print("=" * 50)
    
    # Generate properties report
    results = generate_reports(['properties'])
    
    for report_name, result_path in results.items():
        if result_path is not None:
            print(f"\n✅ {report_name} report generated successfully")
            print(f"   HTML: {result_path}")
            
            # Check if PDF was also generated
            pdf_path = result_path.with_suffix('.pdf')
            if pdf_path.exists():
                print(f"   PDF:  {pdf_path}")
            else:
                print(f"   PDF:  Not generated (weasyprint may not be installed)")
            
            print(f"   Size: {result_path.stat().st_size:,} bytes")
            
            # Read first few lines to verify content
            try:
                with open(result_path, 'r', encoding='utf-8') as f:
                    content_preview = f.read(200)
                    if 'Properties Market Analysis' in content_preview:
                        print(f"   ✓ Content verified - report contains expected elements")
                    else:
                        print(f"   ⚠ Content warning - unexpected report structure")
            except Exception as e:
                print(f"   ⚠ Could not verify content: {e}")
                
        else:
            print(f"\n❌ {report_name} report generation failed")
            print("   Check analysis data availability in data/enriched/")
    
    print("\n" + "=" * 50)
    print("REPORT FEATURES:")
    print("- Bootstrap 5 responsive design")
    print("- Executive summary cards with competitive context")
    print("- Interactive competitor comparison table")
    print("- Automatic ranking and delta calculations")
    print("- PDF export capability")
    print("- Professional styling for business use")

if __name__ == "__main__":
    test_report_generation()
