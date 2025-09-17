import sqlite3
import json
from typing import List, Dict, Any

class QPRepository:
    """Handles all database operations for Quick Possession (QP) data."""
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def _execute_query(self, query: str, params: tuple = ()):
        self.cursor.execute(query, params)
        self.conn.commit()

    def create_schema(self):
        """Creates the necessary tables for the QP pipeline."""
        self._execute_query("""
        CREATE TABLE IF NOT EXISTS qp_urls (
            property_id TEXT PRIMARY KEY, url TEXT NOT NULL UNIQUE, competitor_code TEXT NOT NULL,
            status TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
            extraction_status TEXT, last_attempted_extraction TEXT, head_data_json TEXT
        );
        """)
        self._execute_query("""
        CREATE TABLE IF NOT EXISTS qp_properties (
            property_id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            address TEXT,
            community TEXT,
            price REAL,
            sqft REAL,
            beds INTEGER,
            baths REAL,
            main_image_url TEXT,
            features_json TEXT,
            price_per_sqft REAL,
            first_extracted_at TEXT NOT NULL,
            last_updated_at TEXT NOT NULL,
            -- New columns for analysis
            competitor_code TEXT,
            first_seen TEXT,
            last_seen TEXT,
            listing_status TEXT, -- 'new', 'active', or 'removed'
            FOREIGN KEY (property_id) REFERENCES qp_urls (property_id)
        );
        """)

    def get_urls(self) -> List[Dict[str, Any]]:
        self.cursor.execute("SELECT * FROM qp_urls")
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]

    def upsert_url(self, url_data: Dict[str, Any]):
        query = """
        INSERT INTO qp_urls (property_id, url, competitor_code, status, first_seen, last_seen, extraction_status, last_attempted_extraction, head_data_json)
        VALUES (:property_id, :url, :competitor_code, :status, :first_seen, :last_seen, :extraction_status, :last_attempted_extraction, :head_data_json)
        ON CONFLICT(url) DO UPDATE SET status = excluded.status, last_seen = excluded.last_seen, head_data_json = excluded.head_data_json;
        """
        self.cursor.execute(query, url_data)
        self.conn.commit()


    def upsert_property(self, prop_data: Dict[str, Any]):
        query = """
        INSERT INTO qp_properties (
            property_id, url, address, community, price, sqft, beds, baths,
            main_image_url, features_json, price_per_sqft, first_extracted_at,
            last_updated_at, competitor_code, first_seen, last_seen, listing_status
        ) VALUES (
            :property_id, :url, :address, :community, :price, :sqft, :beds, :baths,
            :main_image_url, :features_json, :price_per_sqft, :first_extracted_at,
            :last_updated_at, :competitor_code, :first_seen, :last_seen, :listing_status
        ) ON CONFLICT(property_id) DO UPDATE SET
            address = excluded.address, community = excluded.community, price = excluded.price,
            sqft = excluded.sqft, beds = excluded.beds, baths = excluded.baths,
            main_image_url = excluded.main_image_url, features_json = excluded.features_json,
            price_per_sqft = excluded.price_per_sqft, last_updated_at = excluded.last_updated_at,
            competitor_code = excluded.competitor_code, first_seen = excluded.first_seen,
            last_seen = excluded.last_seen, listing_status = excluded.listing_status;
        """
        self.cursor.execute(query, prop_data)
        self.conn.commit()

    def update_url_extraction_status(self, property_id: str, status: str, timestamp: str):
        query = "UPDATE qp_urls SET extraction_status = ?, last_attempted_extraction = ? WHERE property_id = ?"
        self._execute_query(query, (status, timestamp, property_id))

    def get_pending_urls(self) -> List[Dict[str, Any]]:
        self.cursor.execute("SELECT * FROM qp_urls WHERE status = 'active' AND extraction_status = 'pending'")
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]