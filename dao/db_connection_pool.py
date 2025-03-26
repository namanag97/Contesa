#!/usr/bin/env python3
"""
Database Connection Pool for Call Center Analysis System
Provides a connection pool for efficient database access.
"""

import sqlite3
import logging
import time
import queue
import threading
from typing import Optional, Dict, Any
from contextlib import contextmanager

# Configure logging
logger = logging.getLogger(__name__)

class ConnectionPool:
    """
    A simple connection pool for SQLite connections.
    Manages a pool of database connections for efficient reuse.
    """
    
    def __init__(self, db_path: str, max_connections: int = 5, timeout: float = 30.0):
        """
        Initialize the connection pool
        
        Args:
            db_path: Path to the SQLite database
            max_connections: Maximum number of connections in the pool
            timeout: Timeout for getting a connection from the pool
        """
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self.pool = queue.Queue(maxsize=max_connections)
        self.active_connections = 0
        self.lock = threading.RLock()
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool with initial connections"""
        logger.info("Initializing connection pool for {} with {} max connections".format(self.db_path, self.max_connections))
        # Start with one connection to avoid creating too many at startup
        self._create_connection()
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            # Return dictionary-like rows
            conn.row_factory = sqlite3.Row
            
            with self.lock:
                self.active_connections += 1
                
            logger.debug("Created new connection (active: {})".format(self.active_connections))
            return conn
        except sqlite3.Error as e:
            logger.error("Error creating database connection: {}".format(str(e)))
            raise
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a connection from the pool
        
        Returns:
            A SQLite connection
            
        Raises:
            TimeoutError: If timeout is reached while waiting for a connection
        """
        try:
            # Try to get a connection from the pool
            conn = self.pool.get(block=False)
            logger.debug("Got connection from pool")
            return conn
        except queue.Empty:
            # Pool is empty, create a new connection if under the limit
            with self.lock:
                if self.active_connections < self.max_connections:
                    return self._create_connection()
                
            # Wait for a connection to become available
            try:
                logger.debug("Waiting for connection (active: {})".format(self.active_connections))
                conn = self.pool.get(timeout=self.timeout)
                return conn
            except queue.Empty:
                logger.error("Timeout waiting for database connection")
                raise TimeoutError("Timed out waiting for database connection")
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return a connection to the pool"""
        if conn is None:
            return
            
        try:
            # Put the connection back in the pool
            self.pool.put(conn, block=False)
            logger.debug("Returned connection to pool")
        except queue.Full:
            # Pool is full, close the connection
            conn.close()
            with self.lock:
                self.active_connections -= 1
            logger.debug("Closed connection (active: {})".format(self.active_connections))
    
    def close_all(self):
        """Close all connections in the pool"""
        logger.info("Closing all database connections")
        
        # Close connections in the pool
        while not self.pool.empty():
            try:
                conn = self.pool.get(block=False)
                conn.close()
                with self.lock:
                    self.active_connections -= 1
            except queue.Empty:
                break
            except Exception as e:
                logger.error("Error closing connection: {}".format(str(e)))

# Global dictionary to store connection pools for different database paths
_connection_pools = {}  # Dict[str, ConnectionPool]
_pools_lock = threading.RLock()

def get_connection_pool(db_path: str, max_connections: int = 5, timeout: float = 30.0) -> ConnectionPool:
    """
    Get or create a connection pool for the given database path
    
    Args:
        db_path: Path to the SQLite database
        max_connections: Maximum number of connections in the pool
        timeout: Timeout for getting a connection from the pool
        
    Returns:
        A connection pool instance
    """
    with _pools_lock:
        if db_path not in _connection_pools:
            _connection_pools[db_path] = ConnectionPool(db_path, max_connections, timeout)
        return _connection_pools[db_path]

@contextmanager
def get_db_connection(db_path: str, max_connections: int = 5, timeout: float = 30.0):
    """
    Context manager for database connections.
    Provides a connection to the SQLite database with proper configuration.
    
    Args:
        db_path: Path to the SQLite database file
        max_connections: Maximum number of connections in the pool
        timeout: Timeout for getting a connection from the pool
        
    Yields:
        sqlite3.Connection: A configured SQLite connection
    """
    conn = None
    pool = get_connection_pool(db_path, max_connections, timeout)
    
    try:
        conn = pool.get_connection()
        yield conn
    except sqlite3.Error as e:
        logger.error("Database connection error: {}".format(str(e)))
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pool.return_connection(conn)

def close_all_connections():
    """Close all database connections in all pools"""
    with _pools_lock:
        for pool in _connection_pools.values():
            pool.close_all()
        _connection_pools.clear() 