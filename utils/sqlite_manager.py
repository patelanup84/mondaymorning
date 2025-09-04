import sqlite3
from pathlib import Path
import pandas as pd

class SqliteManager:
    """A generic manager for handling SQLite database connections and universal helpers."""
    def __init__(self, db_path: str):
        """Initializes the database connection."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        print(f"✅ Generic SqliteManager connected to: {self.db_path}")

    def list_tables(self) -> list:
        """Lists all tables in the database."""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        return [table[0] for table in tables]

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Returns the schema of a specific table as a DataFrame."""
        query = f"PRAGMA table_info({table_name});"
        return pd.read_sql_query(query, self.conn)
    
    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        """Runs a SELECT query and returns the results as a Pandas DataFrame."""
        return pd.read_sql_query(query, self.conn)

    def describe_table(self, table_name: str) -> pd.DataFrame:
        """
        Provides a detailed statistical analysis of a table's columns.

        Args:
            table_name (str): The name of the table to analyze.

        Returns:
            pd.DataFrame: A DataFrame with detailed stats for each column.
        """
        # Get the full table into a pandas DataFrame for efficient analysis
        table_df = self.query_to_dataframe(f"SELECT * FROM {table_name}")

        if table_df.empty:
            print(f"Table '{table_name}' is empty. No stats to calculate.")
            return self.get_table_schema(table_name) # Return basic schema for empty tables

        stats_list = []
        total_rows = len(table_df)

        for column in table_df.columns:
            col_series = table_df[column]
            
            # Basic Info
            stats = {
                'column': column,
                'dtype': str(col_series.dtype),
                'missing_percent': f"{(col_series.isnull().sum() / total_rows) * 100:.2f}%"
            }
            
            # First and Last Row Values
            stats['first_row_val'] = col_series.iloc[0]
            stats['last_row_val'] = col_series.iloc[-1]

            # Numeric Stats
            if pd.api.types.is_numeric_dtype(col_series):
                stats['min'] = col_series.min()
                stats['max'] = col_series.max()
                stats['average'] = f"{col_series.mean():.2f}" if col_series.notna().any() else 'N/A'
            else:
                stats['min'] = 'N/A'
                stats['max'] = 'N/A'
                stats['average'] = 'N/A'
            
            stats_list.append(stats)
            
        return pd.DataFrame(stats_list)

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()

# This test block initializes the database based on the path in config.yaml
if __name__ == '__main__':
    # We need to import the config loader to get the database path
    from config_loader import load_config
    
    print("Testing database manager...")
    try:
        config = load_config()
        db_path = config.get("database", {}).get("sqlite_path")
        
        assert db_path is not None, "Database path not found in config.yaml"
        
        print(f"Initializing database at: {db_path}")
        db_manager = SqliteManager(db_path)
        
        # Verify the database file was created
        assert os.path.exists(db_path), f"Database file was not created at {db_path}"
        
        print("✅ Database manager initialized successfully and file created.")
        db_manager.close()
    
    except (AssertionError, Exception) as e:
        print(f"❌ Test Failed: {e}")