import asyncio
from typing import List, Dict, Any
from crawl4ai import AsyncUrlSeeder, SeedingConfig

# This utility is responsible for discovering URLs for a single competitor.
# This modular approach allows us to easily reuse or test the discovery logic
# independently from the main pipeline flow.

async def discover_competitor_urls(competitor: Dict[str, Any], seeder_config_dict: Dict[str, Any]) -> List[Dict]:
    """
    Discovers all live URLs for a single competitor that match a given pattern.

    Args:
        competitor (Dict[str, Any]): A dictionary with competitor info 
                                     (must include 'domain' and 'pattern').
        seeder_config_dict (Dict[str, Any]): A dictionary of settings for the 
                                             crawl4ai SeedingConfig.

    Returns:
        List[Dict]: A list of URL data dictionaries from crawl4ai, or an empty list on failure.
    """
    domain = competitor.get("domain")
    pattern = competitor.get("pattern")
    
    # Fail-fast if essential data is missing
    assert domain, "Competitor dictionary must include a 'domain'."
    assert pattern, "Competitor dictionary must include a 'pattern'."

    print(f"🔍 Discovering live URLs for {competitor.get('name', domain)}...")

    try:
        # The SeedingConfig controls how we discover URLs. Using 'sitemap+cc' gives
        # us the best chance of finding all URLs.
        seeding_config = SeedingConfig(
            source='sitemap+cc',
            pattern=pattern,
            **seeder_config_dict
        )
        
        # The context manager ensures the seeder is properly closed.
        async with AsyncUrlSeeder() as seeder:
            urls_data = await seeder.urls(domain, config=seeding_config)
        
        print(f"✅ Found {len(urls_data)} live URLs for {competitor.get('name', domain)}.")
        return urls_data
    except Exception as e:
        print(f"❌ Exception during URL discovery for {domain}: {e}")
        return []

# This test block allows us to verify the discovery utility independently.
if __name__ == '__main__':
    from config_loader import load_config
    
    print("Testing URL discovery utility...")
    try:
        # Load our central configuration
        config = load_config()
        # Select a test competitor ('AKS' is a good example from the original script)
        test_competitor = config["competitors"]["PRM"]
        
        # Define seeder settings, similar to the original script
        # `live_check=True` is crucial to ensure we only get valid, accessible URLs.
        seeder_settings = {
            "extract_head": True,
            "live_check": True,
            "concurrency": 5,
            "force": True, # Bypasses cache for fresh results during testing
            "verbose": False
        }

        # The asyncio.run() function is used to execute our async function
        discovered_urls = asyncio.run(discover_competitor_urls(test_competitor, seeder_settings))
        
        assert isinstance(discovered_urls, list), "Result should be a list."
        
        if discovered_urls:
            print(f"\nSuccessfully discovered {len(discovered_urls)} URLs.")
            print("--- Sample URLs ---")
            for url_info in discovered_urls[:3]:
                print(f"- {url_info['url']}")
            print("---------------------\n")
        else:
            print("\nNo URLs were discovered for the test competitor.")

    except (AssertionError, Exception) as e:
        print(f"❌ Test Failed: {e}")