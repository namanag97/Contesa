#!/usr/bin/env python3
"""
Database Connection Pool for Call Center Analysis System
Provides a connection pool for efficient database access.
"""

import sqlite3
import logging
from typing import Optional
from contextlib import contextmanager

# Import from centralized configuration
from config import AppConfig

# Configure logging
logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection(db_path: Optional[str] = None):
    """
    Context manager for database connections.
    Provides a connection to the SQLite database with proper configuration.
    
    Args:
        db_path (str, optional): Path to the SQLite database file.
                                If not provided, uses the configured default.
        
    Yields:
        sqlite3.Connection: A configured SQLite connection
    """
    if db_path is None:
        db_path = AppConfig.get_db_path()
        
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        # Return dictionary-like rows
        conn.row_factory = sqlite3.Row
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()