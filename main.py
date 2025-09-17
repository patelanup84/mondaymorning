import argparse
import asyncio
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
from collections import defaultdict
from pathlib import Path

# --- Utility Imports ---
from utils.config_loader import load_config
from utils.sqlite_manager import SqliteManager
from utils.qp_repository import QPRepository
from utils.url_discovery import discover_competitor_urls
from utils.llm_extractor import extract_property_data

# =============================================================================
# --- Nodes ---
# =============================================================================

class Node:
    def execute(self, shared: Dict[str, Any]) -> None: raise NotImplementedError

class ConfigNode(Node):
    def execute(self, shared: Dict[str, Any]) -> None:
        print("\n⚙️  Executing ConfigNode...")
        shared['config'] = load_config()

        if shared.get('test_mode', False):
            print("🔧 Overriding configuration for TEST mode from config.yaml...")
            test_config = shared['config'].get('testing', {})
            assert test_config, "Testing config section not found in config.yaml"

            # Modify DB path to an isolated test database
            db_path = Path(shared['config']['database']['sqlite_path'])
            db_suffix = test_config.get('db_suffix', '_test')
            test_db_path = db_path.with_name(f"{db_path.stem}{db_suffix}{db_path.suffix}")
            shared['config']['database']['sqlite_path'] = str(test_db_path)
            print(f"  -> Database path set to: {test_db_path}")
            
            # Override pipeline settings for fast, cost-effective runs
            shared['config']['pipeline']['freshness_hours'] = test_config.get('freshness_hours', 0)
            shared['config']['pipeline']['extraction_limit_per_competitor'] = test_config.get('extraction_limit_per_competitor', 2)
            print(f"  -> Freshness check set to {shared['config']['pipeline']['freshness_hours']} hours.")
            print(f"  -> Extraction limit set to {shared['config']['pipeline']['extraction_limit_per_competitor']} per competitor.")

        print("✅ Config loaded successfully.")

class DatabaseNode(Node):
    def execute(self, shared: Dict[str, Any]) -> None:
        print("🗄️  Executing DatabaseNode (Init)...")
        db_path = shared['config']['database']['sqlite_path']
        assert db_path, "Database path not found in config."
        db_manager = SqliteManager(db_path)
        shared['db_manager'] = db_manager
        qp_repo = QPRepository(db_manager.conn)
        qp_repo.create_schema()
        shared['qp_repo'] = qp_repo
        print("✅ Database connection and QP repository established.")

    def close(self, shared: Dict[str, Any]) -> None:
        print("🗄️  Closing DatabaseNode connection...")
        if 'db_manager' in shared:
            shared['db_manager'].close()
            print("✅ Database connection closed.")

class URLDiscoveryNode(Node):
    """Discovers QP URLs for competitors, respecting test mode configuration."""
    def execute(self, shared: Dict[str, Any]) -> None:
        print("📡 Executing URLDiscoveryNode...")
        config = shared['config']
        repo: QPRepository = shared['qp_repo']
        all_discovered_urls = []
        
        competitors_to_process = {}
        if shared.get('test_mode', False):
            test_competitor_keys = config.get('testing', {}).get('competitors', [])
            print(f"🔬 Test Mode: Processing specific competitors: {test_competitor_keys}")
            for key in test_competitor_keys:
                if key in config['competitors']:
                    competitors_to_process[key] = config['competitors'][key]
        else:
            print("🏭 Production Mode: Processing all competitors.")
            competitors_to_process = config['competitors']

        for code, competitor_data in competitors_to_process.items():
            competitor = {'code': code, **competitor_data}
            
            freshness_hours = config['pipeline'].get('freshness_hours', 24)
            if freshness_hours > 0:
                existing_urls = repo.get_urls()
                competitor_urls = [u for u in existing_urls if u['competitor_code'] == code]
                if competitor_urls:
                    last_seen_times = [datetime.fromisoformat(u['last_seen']) for u in competitor_urls]
                    most_recent_scan = max(last_seen_times)
                    if most_recent_scan > (datetime.now(timezone.utc) - timedelta(hours=freshness_hours)):
                        print(f"⏭️  Skipping discovery for {code}, last scanned at {most_recent_scan.strftime('%Y-%m-%d %H:%M')}. Less than {freshness_hours} hours ago.")
                        continue
            
            seeder_settings = {"extract_head": True, "live_check": True, "concurrency": 5, "force": True}
            urls = asyncio.run(discover_competitor_urls(competitor, seeder_settings))
            for url_data in urls:
                url_data['competitor_code'] = code
            all_discovered_urls.extend(urls)
        
        shared['discovered_urls'] = all_discovered_urls
        print(f"✅ URL Discovery complete. Found {len(all_discovered_urls)} new/updated URLs to process.")

class URLLifecycleNode(Node):
    """Compares discovered URLs with the DB to manage status and find pending work."""
    def execute(self, shared: Dict[str, Any]) -> None:
        print("🔄 Executing URLLifecycleNode...")
        repo: QPRepository = shared['qp_repo']
        discovered_urls: List[Dict] = shared.get('discovered_urls', [])
        current_time = datetime.now(timezone.utc).isoformat()

        existing_urls_list = repo.get_urls()
        existing_urls_map = {item['url']: item for item in existing_urls_list}
        all_property_ids = [item['property_id'] for item in existing_urls_list]

        discovered_urls_map = {item['url']: item for item in discovered_urls}
        
        discovered_set = set(discovered_urls_map.keys())
        existing_set = set(existing_urls_map.keys())
        
        new_urls = discovered_set - existing_set
        missing_urls = existing_set - discovered_set
        existing_live_urls = discovered_set.intersection(existing_set)

        for url in new_urls:
            url_data = discovered_urls_map[url]
            comp_code = url_data['competitor_code']
            comp_ids = [int(pid.split('_')[1]) for pid in all_property_ids if pid.startswith(f"{comp_code}_")]
            next_seq = max(comp_ids, default=0) + 1
            prop_id = f"{comp_code}_{next_seq:05d}"
            all_property_ids.append(prop_id)
            repo.upsert_url({
                "property_id": prop_id, "url": url, "competitor_code": comp_code, "status": "active",
                "first_seen": current_time, "last_seen": current_time, "extraction_status": "pending",
                "last_attempted_extraction": None, "head_data_json": json.dumps(url_data.get('head_data', {}))
            })
        print(f"  -> Added {len(new_urls)} new URLs.")

        for url in missing_urls:
            existing_data = existing_urls_map[url]
            existing_data['status'] = 'inactive'
            existing_data['last_seen'] = current_time 
            repo.upsert_url(existing_data)
        print(f"  -> Marked {len(missing_urls)} URLs as inactive.")

        for url in existing_live_urls:
            existing_data = existing_urls_map[url]
            existing_data['status'] = 'active'
            existing_data['last_seen'] = current_time
            existing_data['head_data_json'] = json.dumps(discovered_urls_map[url].get('head_data', {}))
            repo.upsert_url(existing_data)
        print(f"  -> Updated {len(existing_live_urls)} existing URLs.")

        shared['pending_extraction_urls'] = repo.get_pending_urls()
        print(f"✅ Lifecycle management complete. Found {len(shared['pending_extraction_urls'])} URLs pending extraction.")

class ExtractionNode(Node):
    """Extracts structured data from URLs, applying a per-competitor limit."""
    def execute(self, shared: Dict[str, Any]) -> None:
        print("⛏️  Executing ExtractionNode...")
        pending_urls = shared.get('pending_extraction_urls', [])
        config = shared['config']
        llm_provider = 'openai/gpt-4o-mini'
        if not pending_urls:
            print("✅ No URLs to extract. Skipping.")
            shared['extracted_properties'] = []
            return
        
        limit = config.get('pipeline', {}).get('extraction_limit_per_competitor')
        final_urls_to_extract = []
        if limit and isinstance(limit, int) and limit > 0:
            print(f"⚠️  Applying extraction limit of {limit} URLs per competitor.")
            urls_by_competitor = defaultdict(list)
            for url_data in pending_urls:
                urls_by_competitor[url_data['competitor_code']].append(url_data)
            for competitor, urls in urls_by_competitor.items():
                print(f"  -> {competitor}: Found {len(urls)} pending, processing up to {limit}.")
                final_urls_to_extract.extend(urls[:limit])
        else:
            final_urls_to_extract = pending_urls
            
        all_extracted_data = []
        for url_data in final_urls_to_extract:
            data = asyncio.run(extract_property_data(url_data['url'], llm_provider))
            if data:
                data['property_id'] = url_data['property_id']
                data['url'] = url_data['url']
                all_extracted_data.append(data)
        shared['extracted_properties'] = all_extracted_data
        print(f"✅ Extraction complete. Successfully extracted {len(all_extracted_data)} properties.")

class PropertyStorageNode(Node):
    """Calculates status and stores property data in the database."""
    def execute(self, shared: Dict[str, Any]) -> None:
        print("💾 Executing PropertyStorageNode...")
        repo: QPRepository = shared['qp_repo']
        config = shared['config']
        properties_to_store = shared.get('extracted_properties', [])
        current_time = datetime.now(timezone.utc)
        
        if not properties_to_store:
            print("✅ No new properties to store. Skipping.")
            return

        all_urls_map = {url['property_id']: url for url in repo.get_urls()}
        new_listing_days = config['pipeline'].get('new_listing_days', 7)

        for prop in properties_to_store:
            property_id = prop['property_id']
            url_details = all_urls_map.get(property_id)

            if not url_details:
                continue

            listing_status = ''
            first_seen_dt = datetime.fromisoformat(url_details['first_seen'])
            
            if url_details['status'] == 'inactive':
                listing_status = 'removed'
            elif (current_time - first_seen_dt).days < new_listing_days:
                listing_status = 'new'
            else:
                listing_status = 'active'
            
            prop_to_save = {
                **prop,
                "features_json": json.dumps(prop.get("features", {})),
                "first_extracted_at": current_time.isoformat(),
                "last_updated_at": current_time.isoformat(),
                "competitor_code": url_details['competitor_code'],
                "first_seen": url_details['first_seen'],
                "last_seen": url_details['last_seen'],
                "listing_status": listing_status
            }
            repo.upsert_property(prop_to_save)
            repo.update_url_extraction_status(property_id, 'success', current_time.isoformat())
        
        print(f"✅ Stored {len(properties_to_store)} properties to the database with calculated statuses.")

class ExportNode(Node):
    """Exports the final property data to a CSV file during a test run."""
    def execute(self, shared: Dict[str, Any]) -> None:
        if not shared.get('test_mode', False):
            return # Only run in test mode

        test_config = shared['config'].get('testing', {})
        if not test_config.get('export_csv', False):
            print(" CSV export disabled in test config. Skipping.")
            return
        
        print("📤 Executing ExportNode...")
        db_manager: SqliteManager = shared['db_manager']
        export_dir = Path(test_config.get('export_dir', 'output'))
        
        try:
            export_dir.mkdir(parents=True, exist_ok=True)
            
            properties_df = db_manager.query_to_dataframe("SELECT * FROM qp_properties ORDER BY competitor_code, property_id")
            
            if properties_df.empty:
                print(" No properties found in the database to export.")
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = export_dir / f"test_export_{timestamp}.csv"
            
            properties_df.to_csv(file_path, index=False)
            print(f"✅ Successfully exported {len(properties_df)} properties to: {file_path}")

        except Exception as e:
            print(f"❌ Failed to export CSV: {e}")


# =============================================================================
# --- Flow & Main ---
# =============================================================================
def run_pipeline(test_mode: bool = False):
    """Runs the entire data pipeline from start to finish."""
    if test_mode:
        print("🚀🚀🚀 RUNNING PIPELINE IN TEST MODE 🚀🚀🚀")
    
    shared_state = {'test_mode': test_mode}
    db_node = DatabaseNode()
    try:
        ConfigNode().execute(shared_state)
        db_node.execute(shared_state)
        URLDiscoveryNode().execute(shared_state)
        URLLifecycleNode().execute(shared_state)
        ExtractionNode().execute(shared_state)
        PropertyStorageNode().execute(shared_state)
        ExportNode().execute(shared_state) # <-- New node added here
        print("\n🎉 Pipeline run completed successfully!")
    except Exception as e:
        print(f"\n❌ A critical error occurred during the pipeline run: {e}")
    finally:
        db_node.close(shared_state)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the MondayMorning data pipeline.")
    parser.add_argument(
        '--test', 
        action='store_true', 
        help="Run the pipeline in testing mode, using settings from the 'testing' block in config.yaml."
    )
    args = parser.parse_args()
    run_pipeline(test_mode=args.test)

