from pathlib import Path
import os
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

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

# Logging configuration
LOGS_DIR = Path(os.getenv("LOGS_DIR", ROOT / "logs"))
LOG_LEVEL_CONSOLE = os.getenv("LOG_LEVEL_CONSOLE", "INFO").upper()
LOG_LEVEL_FILE = os.getenv("LOG_LEVEL_FILE", "DEBUG").upper()
LOG_ENABLED = os.getenv("LOG_ENABLED", "true").lower() == "true"

# Input Configuration
COMPETITORS_CSV_PATH = Path(os.getenv("COMPETITORS_CSV_PATH", ROOT / "competitors.csv"))

# Ensure directories exist
for path in [RAW_DIR, CLEAN_DIR, ENRICHED_DIR, ASSETS_DIR, REPORTS_DIR, DASH_DIR, LOGS_DIR]:
    path.mkdir(parents=True, exist_ok=True)


def load_competitors_from_csv() -> List[Dict[str, Any]]:
    """Load competitors from CSV file using configured path."""
    try:
        if not COMPETITORS_CSV_PATH.exists():
            logging.getLogger(__name__).error(f"Competitors CSV file not found: {COMPETITORS_CSV_PATH}")
            return []
        
        df = pd.read_csv(COMPETITORS_CSV_PATH)
        if df.empty:
            logging.getLogger(__name__).warning(f"Competitors CSV is empty: {COMPETITORS_CSV_PATH}")
            return []
        
        # Convert DataFrame to list of dictionaries
        competitors = []
        for _, row in df.iterrows():
            competitor = {
                "code": row["competitor_id"],
                "name": row["name"],
                "domain": row["domain"],
                "pattern": row.get("pattern", ""),
                "cid": row.get("cid", "")
            }
            competitors.append(competitor)
        
        logging.getLogger(__name__).info(f"Loaded {len(competitors)} competitors from {COMPETITORS_CSV_PATH}")
        return competitors
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to load competitors from CSV: {e}")
        return []


def setup_logging(log_file_path: Optional[Path] = None, verbose: bool = False) -> Optional[Path]:
    """Initialize logging configuration with console and file handlers."""
    if not LOG_ENABLED and log_file_path is None:
        return None
    
    # Determine log level - default to INFO, DEBUG if verbose
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Use provided log file path or create timestamped one
    if log_file_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = LOGS_DIR / f"mondaymorning_{timestamp}.log"
    else:
        log_file = log_file_path
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)  # Use determined log level
    
    # Suppress debug messages from third-party libraries
    # Keep our own logging at the determined level
    # Suppress third-party library debug messages
    third_party_loggers = [
        'LiteLLM',
        'httpx',
        'httpcore', 
        'aiohttp',
        'urllib3',
        'requests',
        'openai',
        'anthropic',
        'playwright',
        'selenium'
    ]
    
    for logger_name in third_party_loggers:
        third_party_logger = logging.getLogger(logger_name)
        third_party_logger.setLevel(logging.WARNING)
    
    # Suppress specific crawl4ai loggers (keep FETCH only)
    crawl4ai_loggers = [
        'crawl4ai.scrape',
        'crawl4ai.extract', 
        'crawl4ai.complete',
        'crawl4ai.process'
    ]
    
    for logger_name in crawl4ai_loggers:
        crawl4ai_logger = logging.getLogger(logger_name)
        crawl4ai_logger.setLevel(logging.WARNING)
    
    # Console handler (use same level as root)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (use same level as root)
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Log session start
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info(f"MondayMorning Session Started - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Log file: {log_file}")
    logger.info("=" * 80)
    
    return log_file