import sqlite3
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import logging

from ..config import RAW_DIR


class SQLiteStateManager:
    """Generic SQLite state manager for collectors with flexible schema support."""
    
    def __init__(self, collector_name: str):
        self.collector_name = collector_name
        self.db_path = RAW_DIR / f"{collector_name}.db"
        self.logger = logging.getLogger(f"state.{collector_name}")
        self._ensure_directory()
    
    def _ensure_directory(self) -> None:
        """Ensure the database directory exists."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection with proper settings."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        return conn
    
    def create_table(self, table_name: str, schema: Dict[str, str], indexes: List[str] = None) -> bool:
        """
        Create table with given schema.
        
        Args:
            table_name: Name of table to create
            schema: Dict mapping column names to SQL types
            indexes: List of column names to index
            
        Returns:
            True if successful
        """
        try:
            with self.get_connection() as conn:
                # Build CREATE TABLE statement
                columns = []
                for col_name, col_type in schema.items():
                    columns.append(f"{col_name} {col_type}")
                
                columns_sql = ",\n                        ".join(columns)
                
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {columns_sql}
                    )
                """
                
                conn.execute(create_sql)
                
                # Create indexes
                if indexes:
                    for index_col in indexes:
                        index_name = f"idx_{table_name}_{index_col}"
                        conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({index_col})")
                
                conn.commit()
                self.logger.info(f"Table '{table_name}' created/verified in {self.db_path}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to create table '{table_name}': {e}")
            return False
    
    def insert_or_ignore(self, table_name: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        """
        Insert data using INSERT OR IGNORE (avoids duplicates).
        
        Args:
            table_name: Target table
            data: Single dict or list of dicts to insert
            
        Returns:
            Number of rows actually inserted
        """
        try:
            if not data:
                return 0
            
            # Normalize to list
            records = data if isinstance(data, list) else [data]
            
            with self.get_connection() as conn:
                inserted_count = 0
                
                for record in records:
                    # Convert any complex objects to JSON strings
                    processed_record = {}
                    for key, value in record.items():
                        if isinstance(value, (dict, list)):
                            processed_record[key] = json.dumps(value)
                        else:
                            processed_record[key] = value
                    
                    # Build INSERT statement
                    columns = list(processed_record.keys())
                    placeholders = ["?" for _ in columns]
                    values = list(processed_record.values())
                    
                    insert_sql = f"""
                        INSERT OR IGNORE INTO {table_name} 
                        ({', '.join(columns)}) 
                        VALUES ({', '.join(placeholders)})
                    """
                    
                    cursor = conn.execute(insert_sql, values)
                    if cursor.rowcount > 0:
                        inserted_count += 1
                
                conn.commit()
                self.logger.debug(f"Inserted {inserted_count}/{len(records)} new records into {table_name}")
                return inserted_count
                
        except Exception as e:
            self.logger.error(f"Failed to insert into {table_name}: {e}")
            return 0
    
    def update_record(self, table_name: str, updates: Dict[str, Any], where_clause: str, where_params: List[Any] = None) -> bool:
        """
        Update records matching WHERE clause.
        
        Args:
            table_name: Target table
            updates: Dict of column->value updates
            where_clause: SQL WHERE clause (without WHERE keyword)
            where_params: Parameters for WHERE clause
            
        Returns:
            True if successful
        """
        try:
            if not updates:
                return True
            
            # Process updates (convert complex objects to JSON)
            processed_updates = {}
            for key, value in updates.items():
                if isinstance(value, (dict, list)):
                    processed_updates[key] = json.dumps(value)
                else:
                    processed_updates[key] = value
            
            with self.get_connection() as conn:
                # Build UPDATE statement
                set_clauses = [f"{col} = ?" for col in processed_updates.keys()]
                update_values = list(processed_updates.values())
                
                update_sql = f"""
                    UPDATE {table_name} 
                    SET {', '.join(set_clauses)}
                    WHERE {where_clause}
                """
                
                # Combine update values with where parameters
                all_params = update_values + (where_params or [])
                
                cursor = conn.execute(update_sql, all_params)
                conn.commit()
                
                success = cursor.rowcount > 0
                if success:
                    self.logger.debug(f"Updated {cursor.rowcount} records in {table_name}")
                else:
                    self.logger.warning(f"No records updated in {table_name} with WHERE: {where_clause}")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to update {table_name}: {e}")
            return False
    
    def query(self, sql: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        """
        Execute SELECT query and return results as list of dicts.
        
        Args:
            sql: SQL query
            params: Query parameters
            
        Returns:
            List of result dictionaries
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(sql, params or [])
                
                # Convert sqlite3.Row objects to regular dicts
                results = []
                for row in cursor.fetchall():
                    row_dict = dict(row)
                    
                    # Parse JSON strings back to objects
                    for key, value in row_dict.items():
                        if isinstance(value, str) and value.startswith(('{"', '[')):
                            try:
                                row_dict[key] = json.loads(value)
                            except json.JSONDecodeError:
                                pass  # Keep as string if not valid JSON
                    
                    results.append(row_dict)
                
                return results
                
        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            return []
    
    def count(self, table_name: str, where_clause: str = None, where_params: List[Any] = None) -> int:
        """
        Count records in table with optional WHERE clause.
        
        Args:
            table_name: Target table
            where_clause: Optional WHERE clause (without WHERE keyword)
            where_params: Parameters for WHERE clause
            
        Returns:
            Record count
        """
        try:
            sql = f"SELECT COUNT(*) as count FROM {table_name}"
            params = []
            
            if where_clause:
                sql += f" WHERE {where_clause}"
                params = where_params or []
            
            result = self.query(sql, params)
            return result[0]["count"] if result else 0
            
        except Exception as e:
            self.logger.error(f"Count failed for {table_name}: {e}")
            return 0
    
    def export_table_to_csv(self, table_name: str, output_path: Path, order_by: str = None) -> bool:
        """
        Export table data to CSV file.
        
        Args:
            table_name: Source table
            output_path: CSV output path
            order_by: Optional ORDER BY clause
            
        Returns:
            True if successful
        """
        try:
            with self.get_connection() as conn:
                sql = f"SELECT * FROM {table_name}"
                if order_by:
                    sql += f" ORDER BY {order_by}"
                
                df = pd.read_sql_query(sql, conn)
                
                if df.empty:
                    self.logger.warning(f"No data to export from {table_name}")
                    return False
                
                # Convert timestamp columns to proper datetime
                for col in df.columns:
                    if col.endswith('_at') and df[col].dtype == 'object':
                        df[col] = pd.to_datetime(df[col], errors='ignore')
                
                df.to_csv(output_path, index=False)
                self.logger.info(f"Exported {len(df)} records from {table_name} to {output_path}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to export {table_name} to CSV: {e}")
            return False
    
    def cleanup_old_data(self, table_name: str, date_column: str, days_old: int) -> int:
        """
        Delete records older than specified days.
        
        Args:
            table_name: Target table
            date_column: Column containing date/timestamp
            days_old: Number of days threshold
            
        Returns:
            Number of deleted records
        """
        try:
            from datetime import timedelta
            cutoff_date = (datetime.now() - timedelta(days=days_old)).isoformat()
            
            with self.get_connection() as conn:
                cursor = conn.execute(
                    f"DELETE FROM {table_name} WHERE {date_column} < ?",
                    [cutoff_date]
                )
                deleted_count = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"Cleaned up {deleted_count} records older than {days_old} days from {table_name}")
                return deleted_count
                
        except Exception as e:
            self.logger.error(f"Cleanup failed for {table_name}: {e}")
            return 0
    
    def get_stats(self, table_name: str, group_by_column: str = None) -> Dict[str, int]:
        """
        Get statistics for table, optionally grouped by column.
        
        Args:
            table_name: Target table
            group_by_column: Optional column to group by
            
        Returns:
            Statistics dictionary
        """
        try:
            if group_by_column:
                sql = f"""
                    SELECT {group_by_column}, COUNT(*) as count 
                    FROM {table_name} 
                    GROUP BY {group_by_column}
                """
                results = self.query(sql)
                return {row[group_by_column]: row["count"] for row in results}
            else:
                total = self.count(table_name)
                return {"total": total}
                
        except Exception as e:
            self.logger.error(f"Failed to get stats for {table_name}: {e}")
            return {}