#!/usr/bin/env python3
"""
Custom exceptions for database operations.
Provides standardized error handling for database-related issues.
"""

class DatabaseError(Exception):
    """Base exception for all database-related errors."""
    
    def __init__(self, message: str = "Database operation failed", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class ConnectionError(DatabaseError):
    """Exception raised when connection to the database fails."""
    
    def __init__(self, message: str = "Failed to connect to the database", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class QueryError(DatabaseError):
    """Exception raised when a database query fails."""
    
    def __init__(self, message: str = "Database query failed", query: str = None, *args, **kwargs):
        self.query = query
        full_message = message
        if query:
            full_message = "{} (Query: {})".format(message, query)
        super().__init__(full_message, *args, **kwargs)


class RecordNotFoundError(DatabaseError):
    """Exception raised when a requested record is not found."""
    
    def __init__(self, message: str = "Record not found", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class DuplicateRecordError(DatabaseError):
    """Exception raised when attempting to create a duplicate record."""
    
    def __init__(self, message: str = "Record already exists", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class TransactionError(DatabaseError):
    """Exception raised when a database transaction fails."""
    
    def __init__(self, message: str = "Transaction failed", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class ValidationError(DatabaseError):
    """Exception raised when data validation fails before database operation."""
    
    def __init__(self, message: str = "Data validation failed", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class MigrationError(DatabaseError):
    """Exception raised when a database migration fails."""
    
    def __init__(self, message: str = "Database migration failed", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class PoolError(DatabaseError):
    """Exception raised when there is an issue with the connection pool."""
    
    def __init__(self, message: str = "Connection pool error", *args, **kwargs):
        super().__init__(message, *args, **kwargs) 