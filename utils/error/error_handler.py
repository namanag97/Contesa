#!/usr/bin/env python3
"""
Error Handling Utilities
Provides standardized error handling functionality.
"""

import logging
import os
import sys
import traceback
from typing import Callable, Any, Optional, Dict, Type
from functools import wraps
import time

# Configure logging
logger = logging.getLogger(__name__)

class APIError(Exception):
    """Base class for API related errors"""
    def __init__(self, message: str, status_code: Optional[int] = None, api_response: Optional[Dict[str, Any]] = None):
        self.message = message
        self.status_code = status_code
        self.api_response = api_response
        super().__init__(self.message)
    
    def __str__(self) -> str:
        if self.status_code:
            return f"API Error ({self.status_code}): {self.message}"
        return f"API Error: {self.message}"

class DatabaseError(Exception):
    """Base class for database related errors"""
    def __init__(self, message: str, query: Optional[str] = None):
        self.message = message
        self.query = query
        super().__init__(self.message)
    
    def __str__(self) -> str:
        if self.query:
            # Truncate long queries for readability
            query_str = self.query[:100] + "..." if len(self.query) > 100 else self.query
            return f"Database Error in query '{query_str}': {self.message}"
        return f"Database Error: {self.message}"

class ValidationError(Exception):
    """Error raised when input validation fails"""
    pass

class ConfigurationError(Exception):
    """Error raised when configuration is invalid or missing"""
    pass

def retry(max_attempts: int = 3, delay: int = 1, backoff: float = 2.0, 
          exceptions: tuple = (Exception,), logger_func: Optional[Callable] = None):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay in seconds
        backoff: Backoff multiplier
        exceptions: Tuple of exceptions to catch
        logger_func: Function to call for logging retries
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            mtries, mdelay = max_attempts, delay
            
            # Try the function up to max_attempts times
            for attempt in range(1, mtries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    # Log the error
                    if logger_func:
                        logger_func(f"Retry attempt {attempt}/{max_attempts} for {func.__name__}: {str(e)}")
                    else:
                        logger.warning(f"Retry attempt {attempt}/{max_attempts} for {func.__name__}: {str(e)}")
                    
                    # Last attempt failed, raise exception
                    if attempt == mtries:
                        raise
                    
                    # Wait before next attempt
                    time.sleep(mdelay)
                    
                    # Increase the delay for next attempt
                    mdelay *= backoff
        return wrapper
    return decorator

def graceful_exit(error_code: int = 1, cleanup_func: Optional[Callable] = None):
    """
    Decorator for handling graceful exit on exceptions
    
    Args:
        error_code: Exit code to use if an error occurs
        cleanup_func: Optional function to call for cleanup before exit
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except KeyboardInterrupt:
                logger.info("Process interrupted by user")
                if cleanup_func:
                    cleanup_func()
                sys.exit(0)
            except Exception as e:
                logger.error(f"Fatal error in {func.__name__}: {str(e)}")
                logger.error(traceback.format_exc())
                if cleanup_func:
                    cleanup_func()
                sys.exit(error_code)
        return wrapper
    return decorator

def setup_logger(name: str, log_file: Optional[str] = None, 
                level: int = logging.INFO) -> logging.Logger:
    """
    Set up a logger with file and console handlers
    
    Args:
        name: Logger name
        log_file: Path to log file
        level: Logging level
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def exception_mapper(exception_map: Dict[Type[Exception], Type[Exception]]):
    """
    Decorator to map caught exceptions to custom exceptions
    
    Args:
        exception_map: Dictionary mapping source exceptions to target exceptions
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Check if this exception type should be remapped
                for source_exception, target_exception in exception_map.items():
                    if isinstance(e, source_exception):
                        raise target_exception(str(e)) from e
                # If no mapping found, re-raise the original exception
                raise
        return wrapper
    return decorator 