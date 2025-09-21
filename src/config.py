from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Project root
ROOT = Path(__file__).resolve().parents[1]

# Data directories
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT / "data"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", ROOT / "outputs"))

RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
ENRICHED_DIR = DATA_DIR / "enriched"
ASSETS_DIR = DATA_DIR / "assets"

REPORTS_DIR = OUTPUT_DIR / "reports"
DASH_DIR = OUTPUT_DIR / "dashboards"

# Competitive Analysis Settings
OUR_COMPANY_ID = "PRM"

# Ensure directories exist
for path in [RAW_DIR, CLEAN_DIR, ENRICHED_DIR, ASSETS_DIR, REPORTS_DIR, DASH_DIR]:
    path.mkdir(parents=True, exist_ok=True)