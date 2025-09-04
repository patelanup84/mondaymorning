import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, List

# --- Utility Imports ---
from collections import defaultdict
from utils.config_loader import load_config
from utils.sqlite_manager import SqliteManager
from utils.qp_repository import QPRepository # New Import
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
        print("✅ Config loaded successfully.")

class DatabaseNode(Node):
    def execute(self, shared: Dict[str, Any]) -> None:
        print("🗄️  Executing DatabaseNode (Init)...")
        db_path = shared['config']['database']['sqlite_path']
        assert db_path, "Database path not found in config."
        
        # 1. Create the generic manager
        db_manager = SqliteManager(db_path)
        shared['db_manager'] = db_manager
        
        # 2. Create the specific repository, passing the connection to it
        qp_repo = QPRepository(db_manager.conn)
        qp_repo.create_schema() # Ensure tables exist
        shared['qp_repo'] = qp_repo
        
        print("✅ Database connection and QP repository established.")

    def close(self, shared: Dict[str, Any]) -> None:
        print("🗄️  Closing DatabaseNode connection...")
        if 'db_manager' in shared:
            shared['db_manager'].close()
            print("✅ Database connection closed.")

class URLDiscoveryNode(Node):
    def execute(self, shared: Dict[str, Any]) -> None:
        print("📡 Executing URLDiscoveryNode...")
        # ... (This node's code remains unchanged)
        config = shared['config']
        competitors = config['competitors']
        active_competitors = [{'code': code, **data} for code, data in competitors.items() if data['active']]
        all_discovered_urls = []
        for competitor in active_competitors:
            seeder_settings = {"extract_head": True, "live_check": True, "concurrency": 5, "force": True}
            urls = asyncio.run(discover_competitor_urls(competitor, seeder_settings))
            for url_data in urls:
                url_data['competitor_code'] = competitor['code']
            all_discovered_urls.extend(urls)
        shared['discovered_urls'] = all_discovered_urls
        print(f"✅ URL Discovery complete. Found {len(all_discovered_urls)} total URLs.")

class URLLifecycleNode(Node):
    def execute(self, shared: Dict[str, Any]) -> None:
        print("🔄 Executing URLLifecycleNode...")
        repo: QPRepository = shared['qp_repo'] # Use the repository now
        # ... (The rest of the logic is the same, but calls repo instead of db)
        discovered_urls: List[Dict] = shared['discovered_urls']
        current_time = datetime.now(timezone.utc).isoformat()
        existing_urls_list = repo.get_urls()
        existing_urls_map = {item['url']: item for item in existing_urls_list}
        all_property_ids = [item['property_id'] for item in existing_urls_list]
        discovered_urls_set = {item['url'] for item in discovered_urls}
        existing_urls_set = set(existing_urls_map.keys())
        new_urls = discovered_urls_set - existing_urls_set
        for url in new_urls:
            url_data = next(item for item in discovered_urls if item['url'] == url)
            comp_code = url_data['competitor_code']
            comp_ids = [int(pid.split('_')[1]) for pid in all_property_ids if pid.startswith(f"{comp_code}_")]
            next_seq = max(comp_ids, default=0) + 1
            prop_id = f"{comp_code}_{next_seq:05d}"
            all_property_ids.append(prop_id)
            repo.upsert_url({
                "property_id": prop_id, "url": url, "competitor_code": comp_code,
                "status": "active", "first_seen": current_time, "last_seen": current_time,
                "extraction_status": "pending", "last_attempted_extraction": None,
                "head_data_json": json.dumps(url_data.get('head_data', {}))
            })
        print(f"  -> Added {len(new_urls)} new URLs to the database.")
        shared['pending_extraction_urls'] = repo.get_pending_urls()
        print(f"✅ Found {len(shared['pending_extraction_urls'])} URLs pending extraction.")

class ExtractionNode(Node):
    """Extracts structured data from URLs, applying a per-competitor limit."""
    def execute(self, shared: Dict[str, Any]) -> None:
        print("⛏️  Executing ExtractionNode...")
        pending_urls = shared.get('pending_extraction_urls', [])
        config = shared['config']
        llm_provider = config['collection'].get('llm_provider', 'openai/gpt-4o-mini')

        if not pending_urls:
            print("✅ No URLs to extract. Skipping.")
            shared['extracted_properties'] = []
            return
        
        # --- NEW PER-COMPETITOR LIMIT LOGIC ---
        limit = config.get('pipeline', {}).get('extraction_limit_per_competitor')
        
        final_urls_to_extract = []
        if limit and isinstance(limit, int) and limit > 0:
            print(f"⚠️  Applying extraction limit of {limit} URLs per competitor.")
            
            # Group URLs by competitor_code
            urls_by_competitor = defaultdict(list)
            for url_data in pending_urls:
                urls_by_competitor[url_data['competitor_code']].append(url_data)

            # Apply the limit to each competitor's list
            for competitor, urls in urls_by_competitor.items():
                print(f"  -> {competitor}: Found {len(urls)} pending, processing up to {limit}.")
                final_urls_to_extract.extend(urls[:limit])
        else:
            # If no limit, process all pending URLs
            final_urls_to_extract = pending_urls
        # --- END OF NEW LOGIC ---

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
    def execute(self, shared: Dict[str, Any]) -> None:
        print("💾 Executing PropertyStorageNode...")
        repo: QPRepository = shared['qp_repo'] # Use the repository now
        properties_to_store = shared.get('extracted_properties', [])
        current_time = datetime.now(timezone.utc).isoformat()
        if not properties_to_store:
            print("✅ No new properties to store. Skipping.")
            return
        for prop in properties_to_store:
            prop_to_save = {**prop, "features_json": json.dumps(prop.get("features", {})), "first_extracted_at": current_time, "last_updated_at": current_time}
            repo.upsert_property(prop_to_save)
            repo.update_url_extraction_status(prop['property_id'], 'success', current_time)
        print(f"✅ Stored {len(properties_to_store)} properties to the database.")

# =============================================================================
# --- Flow & Main ---
# =============================================================================
def run_pipeline():
    shared_state = {}
    db_node = DatabaseNode()
    try:
        ConfigNode().execute(shared_state)
        db_node.execute(shared_state)
        URLDiscoveryNode().execute(shared_state)
        URLLifecycleNode().execute(shared_state)
        ExtractionNode().execute(shared_state)
        PropertyStorageNode().execute(shared_state)
        print("\n🎉 Pipeline run completed successfully!")
    except Exception as e:
        print(f"\n❌ A critical error occurred during the pipeline run: {e}")
    finally:
        db_node.close(shared_state)

if __name__ == '__main__':
    run_pipeline()