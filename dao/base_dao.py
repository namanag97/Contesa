#!/usr/bin/env python3
"""
Base Data Access Object for Call Center Analysis System
Provides common database functionality for all DAO classes.
"""

import sqlite3
import logging
from typing import Optional, List, Dict, Any, Generator
from contextlib import contextmanager

# Import the error handler for standardized error handling
from utils.error.error_handler import DatabaseError, exception_mapper

# Configure logging
logger = logging.getLogger(__name__)

class BaseDAO:
    """Base class for all Data Access Objects"""
    
    def __init__(self, db_path: str):
        """
        Initialize the base DAO with database path
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
    
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Context manager for database connections
        
        Yields:
            A configured SQLite connection
        
        Raises:
            DatabaseError: If a database error occurs
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            # Return dictionary-like rows
            conn.row_factory = sqlite3.Row
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {str(e)}")
            if conn:
                conn.rollback()
            raise DatabaseError(f"Database connection error: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    @exception_mapper({sqlite3.Error: DatabaseError})
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return the results
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            List of dictionaries representing the query results
            
        Raises:
            DatabaseError: If a database error occurs
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
    @exception_mapper({sqlite3.Error: DatabaseError})
    def execute_update(self, query: str, params: tuple = ()) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE query and return affected rows
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Number of affected rows
            
        Raises:
            DatabaseError: If a database error occurs
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            logger.error(f"Update execution error: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
    @exception_mapper({sqlite3.Error: DatabaseError})
    def execute_batch(self, query: str, params_list: List[tuple]) -> int:
        """
        Execute a batch of INSERT, UPDATE, or DELETE queries
        
        Args:
            query: SQL query to execute
            params_list: List of parameter tuples
            
        Returns:
            Number of affected rows
            
        Raises:
            DatabaseError: If a database error occurs
        """
        if not params_list:
            return 0
            
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(query, params_list)
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            logger.error(f"Batch execution error: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Params count: {len(params_list)}")
            raise
    
    @exception_mapper({sqlite3.Error: DatabaseError})
    def get_by_id(self, table: str, id_field: str, id_value: Any) -> Optional[Dict[str, Any]]:
        """
        Get a single record by ID
        
        Args:
            table: Table name
            id_field: Name of the ID field
            id_value: Value of the ID
            
        Returns:
            Dictionary representing the record or None if not found
            
        Raises:
            DatabaseError: If a database error occurs
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT * FROM {table} WHERE {id_field} = ?", (id_value,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting record by ID: {str(e)}")
            logger.error(f"Table: {table}, ID field: {id_field}, ID value: {id_value}")
            raise
    
    @exception_mapper({sqlite3.Error: DatabaseError})
    def get_table_columns(self, table: str) -> List[str]:
        """
        Get the column names for a table
        
        Args:
            table: Table name
            
        Returns:
            List of column names
            
        Raises:
            DatabaseError: If a database error occurs
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table})")
                return [row['name'] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting table columns: {str(e)}")
            logger.error(f"Table: {table}")
            raise
    
    @exception_mapper({sqlite3.Error: DatabaseError})
    def insert_or_update(self, table: str, data: Dict[str, Any], id_field: str) -> bool:
        """
        Insert or update a record in the database
        
        Args:
            table: Table name
            data: Dictionary of column names and values
            id_field: Name of the ID field for conflict resolution
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            DatabaseError: If a database error occurs
        """
        try:
            # Get table columns to filter valid fields
            columns = self.get_table_columns(table)
            
            # Filter fields that exist in the table
            fields = []
            placeholders = []
            values = []
            update_fields = []
            
            for field, value in data.items():
                if field in columns:
                    fields.append(field)
                    placeholders.append('?')
                    values.append(value)
                    if field != id_field:  # Don't update the ID field
                        update_fields.append(f"{field} = excluded.{field}")
            
            if not fields:
                logger.warning(f"No valid fields to insert/update for table {table}")
                return False
                
            fields_str = ', '.join(fields)
            placeholders_str = ', '.join(placeholders)
            update_fields_str = ', '.join(update_fields)
            
            # Construct and execute query
            query = f"""
            INSERT INTO {table} ({fields_str})
            VALUES ({placeholders_str})
            ON CONFLICT({id_field}) DO UPDATE SET
            {update_fields_str}
            """
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, values)
                conn.commit()
                
                return True
                
        except Exception as e:
            logger.error(f"Error in insert_or_update: {str(e)}")
            logger.error(f"Table: {table}, ID field: {id_field}")
            raise 