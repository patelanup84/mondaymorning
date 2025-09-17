# MondayMorning Data Pipeline

This project is an automated data pipeline designed to scrape, extract, and store real estate "Quick Possession" (QP) listings from competitor websites. It uses a modular, node-based architecture to manage the entire lifecycle of a listing, from initial discovery to final storage in a SQLite database, leveraging an AI model for robust data extraction.

## Workflow Overview

The pipeline executes a sequence of nodes, each with a specific responsibility. The process flows as follows:

1. **Configuration Loading**: The pipeline starts by loading settings from `config.yaml` and `competitors.yaml` into a single configuration object.
2. **Database Initialization**: It establishes a connection to the SQLite database specified in the configuration and creates the necessary tables (`qp_urls`, `qp_properties`) if they don't exist.
3. **URL Discovery**: For each active competitor, the pipeline discovers all URLs matching the specified domain and pattern. It respects a `freshness_hours` setting to avoid re-scanning competitors that have been checked recently.
4. **URL Lifecycle Management**: The discovered URLs are compared against the database. New URLs are added with a 'pending' extraction status, URLs that are no longer found are marked 'inactive', and existing URLs are updated.
5. **AI-Powered Data Extraction**: The pipeline processes all URLs pending extraction. It uses the `crawl4ai` library and a specified LLM (e.g., `openai/gpt-4o-mini`) to extract structured data based on a Pydantic schema. A configurable limit can be applied for testing purposes.
6. **Data Storage & Status Calculation**: The extracted and cleaned data is saved to the `qp_properties` table. The pipeline calculates a `listing_status` ('new', 'active', or 'removed') based on the URL's status and how long it has been tracked.

## Key Features

* **Modular Pipeline**: The entire workflow is broken down into clear, sequential nodes (`ConfigNode`, `URLDiscoveryNode`, `ExtractionNode`, etc.) for readability and maintenance.
* **Configuration Driven**: All core settings, including database paths, competitor details, and pipeline behavior, are managed in `config.yaml` and `competitors.yaml` files.
* **Automated URL Discovery**: Uses `crawl4ai`'s `AsyncUrlSeeder` to find all relevant listing pages from competitor sitemaps and other sources.
* **AI-Powered Extraction**: Leverages an LLM to parse unstructured HTML content into a clean, predefined JSON schema (`QPListing`), ensuring high-quality, structured data.
* **Persistent Storage**: All discovered URLs and extracted property data are stored in a local SQLite database for persistence and analysis.
* **Listing Lifecycle Tracking**: Automatically manages the status of each listing, from when it's first seen (`new`), while it's available (`active`), to when it's taken down (`removed`).
* **Efficient Scanning**: A "freshness" check ensures that competitors are not re-scanned unnecessarily, saving time and resources.

## Project Structure

```
.
├── competitors.yaml
├── config.yaml
├── main.py
├── requirements.txt
└── utils
    ├── __init__.py
    ├── config_loader.py
    ├── llm_extractor.py
    ├── qp_repository.py
    ├── sqlite_manager.py
    └── url_discovery.py
```

* `main.py`: The main entry point that defines and executes the pipeline nodes in sequence.
* `config.yaml` / `competitors.yaml`: Configuration files for pipeline settings and competitor definitions.
* `requirements.txt`: A list of all Python dependencies for the project.
* `utils/`: A package containing modular, reusable components.

  * `config_loader.py`: Utility to load and merge YAML configuration files.
  * `url_discovery.py`: Handles the discovery of URLs for a single competitor.
  * `llm_extractor.py`: Manages the AI-powered data extraction from a single URL.
  * `sqlite_manager.py`: A generic class to manage the SQLite database connection.
  * `qp_repository.py`: A specific class for all database operations related to QP listings.

## Setup and Installation

1. **Prerequisites**: Ensure you have Python 3.8+ installed.

2. **Install Dependencies**: Install all required packages using pip.

   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Variables**: The LLM extractor requires an OpenAI API key. Set this key as an environment variable. Create a `.env` file in the root directory or set it in your shell.

   ```
   OPENAI_API_KEY="your_openai_api_key_here"
   ```

4. **Configuration**:

   * **`config.yaml`**: Modify this file to set the correct path for your SQLite database (`sqlite_path`) and adjust pipeline behavior like `freshness_hours`.
   * **`competitors.yaml`**: Add or modify competitor entries. Ensure the `domain`, `pattern`, and `active` status are set correctly for the competitors you wish to track.

## How to Run

Execute the main pipeline script from the root directory of the project for a standard production run:

```bash
python main.py
```

### Testing Mode

To run the pipeline in an isolated and cost-effective testing mode, use the `--test` flag. This is highly recommended for development and debugging.

```bash
python main.py --test
```

Running in test mode will automatically:

* Use a separate database (`mondaymorning_test.db`) to avoid interfering with production data.
* Disable the freshness check, forcing the URL discovery to run every time.
* Limit the number of URLs sent for AI extraction to 2 per competitor, reducing API costs.

## Database Schema

The pipeline creates and manages two main tables in the SQLite database.

* **`qp_urls`**: This table tracks every unique property URL discovered.

  * `property_id`: A unique identifier for the listing (e.g., `PRM_00001`).
  * `url`: The full URL of the listing.
  * `status`: The current status of the URL (`active` or `inactive`).
  * `first_seen` / `last_seen`: Timestamps for tracking the listing's lifecycle.
  * `extraction_status`: The state of the data extraction process (`pending`, `success`, etc.).

* **`qp_properties`**: This table stores the structured data extracted from the URLs.

  * `property_id`: The foreign key linking back to the `qp_urls` table.
  * `address`, `community`, `price`, `sqft`, `beds`, `baths`: The core structured data extracted by the LLM.
  * `price_per_sqft`: A calculated field for analysis.
  * `listing_status`: A calculated status for analysis (`new`, `active`, `removed`).
  * `first_extracted_at` / `last_updated_at`: Timestamps for data freshness.
