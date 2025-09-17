import asyncio
import json
import os
import re
from typing import Optional, Dict, Any, Union

from pydantic import BaseModel
from crawl4ai import (AsyncWebCrawler, CrawlerRunConfig, LLMExtractionStrategy, LLMConfig)

class QPListing(BaseModel):
    """Pydantic schema for data extraction from a single property page."""
    address: str
    community: Optional[str] = None
    price: Optional[float] = None
    sqft: Optional[float] = None
    beds: Optional[int] = None
    baths: Optional[float] = None
    main_image_url: Optional[str] = None
    features: Optional[Dict[str, Any]] = None

def _clean_numeric(value: Any) -> Optional[Union[float, int]]:
    """A helper to clean and convert a value to a number."""
    if value is None:
        return None
    cleaned_string = re.sub(r'[^\d.]', '', str(value))
    if not cleaned_string:
        return None
    try:
        num = float(cleaned_string)
        return int(num) if num.is_integer() else num
    except (ValueError, TypeError):
        return None

async def extract_property_data(url: str, llm_provider: str) -> Optional[Dict]:
    """
    Extracts, validates, and cleans property data from a single URL.

    Args:
        url (str): The URL of the property listing page.
        llm_provider (str): The LLM provider string (e.g., 'openai/gpt-4o-mini').

    Returns:
        Optional[Dict]: A cleaned and validated dictionary, or None on failure.
    """
    print(f"🤖 Extracting data from: {url}")
    api_token = os.getenv("OPENAI_API_KEY")
    assert api_token, "OPENAI_API_KEY environment variable not set."

    llm_instruction = """
    You are extracting property listing data from a home builder's quick possession page. Extract the following fields exactly as specified:
    - address: Full street address (e.g. "123 Main Street NW")
    - community: Neighborhood/community name (e.g. "Heartland")
    - price: Numerical price only, no currency symbols (e.g. 850000)
    - sqft: Total square footage as number (e.g. 2100)
    - beds: Number of bedrooms as integer (e.g. 3)
    - baths: Number of bathrooms, can be float (e.g. 2.5)
    - main_image_url: The full URL of the primary property image.
    - features: A dictionary of key features (e.g. {"Bonus Room": true, "Ensuite": true})
    
    RULES:
    1. If a field is not found, use null. Do not guess.
    2. Extract exact values. Do not interpret or convert units.
    3. Return a single, valid JSON object matching the schema.
    """
    
    try:
        llm_config = LLMConfig(provider=llm_provider, api_token=api_token)
        llm_strategy = LLMExtractionStrategy(llm_config=llm_config, schema=QPListing.model_json_schema(), instruction=llm_instruction)
        crawler_config = CrawlerRunConfig(extraction_strategy=llm_strategy)

        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url, config=crawler_config)

        if result.success and result.extracted_content:
            raw_data = json.loads(result.extracted_content)
            raw_data = raw_data[0] if isinstance(raw_data, list) and raw_data else raw_data

            if not raw_data:
                print(f"❌ Failed for {url}: LLM returned empty content.")
                return None

            # --- Validation Logic ---
            address = raw_data.get('address')
            if not (isinstance(address, str) and address.strip()):
                print(f"❌ Validation Failed for {url}: Missing or invalid address.")
                return None
            
            cleaned_data = {
                'address': address.strip(),
                'community': raw_data.get('community'),
                'price': _clean_numeric(raw_data.get('price')),
                'sqft': _clean_numeric(raw_data.get('sqft')),
                'beds': _clean_numeric(raw_data.get('beds')),
                'baths': _clean_numeric(raw_data.get('baths')),
                'main_image_url': raw_data.get('main_image_url'),
                'features': raw_data.get('features', {})
            }

            price = cleaned_data['price']
            sqft = cleaned_data['sqft']
            cleaned_data['price_per_sqft'] = round(price / sqft, 2) if price and sqft and sqft > 0 else None
            
            print(f"✅ Success & Validation Pass for {url}")
            return cleaned_data
        else:
            print(f"❌ Failed for {url}: Crawler error - {result.error_message}")
            return None
    except Exception as e:
        print(f"❌ Critical error during extraction for {url}: {e}")
        return None

if __name__ == '__main__':
    TEST_URL = "https://www.prominenthomes.ca/quick-possessions/18-waterford-mews/" # Replace with a real URL
    LLM_PROVIDER_FOR_TEST = 'openai/gpt-4o-mini'
    
    print(f"--- Testing Combined Extractor & Validator on: {TEST_URL} ---")
    
    if "your_api_key_here" in os.getenv("OPENAI_API_KEY", "") or not os.getenv("OPENAI_API_KEY"):
         print("\n⚠️ WARNING: OPENAI_API_KEY is not set.")
    elif "example.com" in TEST_URL:
        print("\n⚠️ WARNING: Please replace the placeholder TEST_URL.")
    else:
        validated_data = asyncio.run(extract_property_data(TEST_URL, LLM_PROVIDER_FOR_TEST))
        
        if validated_data:
            print("\n--- Cleaned & Validated Data ---")
            print(json.dumps(validated_data, indent=2))
            assert 'price_per_sqft' in validated_data
            print("--------------------------------\n")
            print("✅ Test finished successfully.")
        else:
            print("\n--- No data was returned after validation. ---")
            print("❌ Test finished with a failure.")