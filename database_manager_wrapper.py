#!/usr/bin/env python3
"""
Database Manager Wrapper for Call Center Analysis System
Provides backward compatibility for the database_manager module using new DAO classes.
"""

import logging
import os
import sqlite3
from typing import Dict, List, Any, Optional, Tuple, Generator
import pandas as pd
from contextlib import contextmanager

# Import DAOs
from dao.base_dao import BaseDAO
from dao.transcription_dao import TranscriptionDAO
from dao.analysis_dao import AnalysisResultDAO
from dao.category_dao import CategoryDAO
from dao.stats_dao import StatsDAO
from dao.config_dao import ConfigDAO
from dao.user_dao import UserDAO

# Import exceptions
from exceptions.database_exceptions import DatabaseError, RecordNotFoundError

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database Manager for the Call Center Analysis System.
    This class provides backward compatibility with the old database_manager module
    while using the new DAO layer for actual database operations.
    """
    
    def __init__(self, db_path: str = "call_analysis.db"):
        """Initialize the database manager with the path to the SQLite database"""
        self.db_path = db_path
        
        # Initialize DAO instances
        self.transcription_dao = TranscriptionDAO(db_path)
        self.analysis_dao = AnalysisResultDAO(db_path)
        self.category_dao = CategoryDAO(db_path)
        self.stats_dao = StatsDAO(db_path)
        self.config_dao = ConfigDAO(db_path)
        self.user_dao = UserDAO(db_path)
        
        # Initialize database
        self.initialize_db()
    
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connections"""
        # Use BaseDAO's connection manager
        base_dao = BaseDAO(self.db_path)
        with base_dao.get_connection() as conn:
            yield conn
    
    def initialize_db(self):
        """Create database tables if they don't exist"""
        # Each DAO creates its own tables as needed
        logger.info("Database initialized")
    
    # Transcription operations
    def import_transcriptions_from_csv(self, csv_file: str) -> int:
        """Import transcriptions from CSV file into the database"""
        return self.transcription_dao.import_from_csv(csv_file)
    
    def get_transcription(self, call_id: str) -> Optional[Dict[str, Any]]:
        """Get a transcription by call ID"""
        return self.transcription_dao.get_by_id(call_id)
    
    def get_all_transcriptions(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all transcriptions"""
        return self.transcription_dao.get_all(limit)
    
    def get_transcriptions_for_analysis(self, reanalyze: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get transcriptions that need to be analyzed"""
        return self.transcription_dao.get_for_analysis(reanalyze, limit)
    
    def save_transcription(self, data: Dict[str, Any]) -> bool:
        """Save a transcription"""
        return self.transcription_dao.save(data)
    
    def export_transcriptions_to_csv(self, csv_file: str, where_clause: Optional[str] = None) -> bool:
        """Export transcriptions to a CSV file"""
        return self.transcription_dao.export_to_csv(csv_file, where_clause)
    
    def count_transcriptions(self, where_clause: Optional[str] = None) -> int:
        """Count transcriptions in the database"""
        return self.transcription_dao.count(where_clause)
    
    # Analysis operations
    def save_analysis_result(self, result: Dict[str, Any]) -> bool:
        """Save an analysis result"""
        return self.analysis_dao.save(result)
    
    def get_analysis_result(self, call_id: str) -> Optional[Dict[str, Any]]:
        """Get an analysis result by call ID"""
        return self.analysis_dao.get_by_id(call_id)
    
    def get_all_analysis_results(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all analysis results"""
        return self.analysis_dao.get_all(limit)
    
    def get_analysis_results_by_criteria(self, criteria: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """Get analysis results by criteria"""
        return self.analysis_dao.get_by_criteria(criteria, limit)
    
    def export_analysis_results_to_csv(self, csv_file: str, where_clause: Optional[str] = None) -> bool:
        """Export analysis results to a CSV file"""
        return self.analysis_dao.export_to_csv(csv_file, where_clause)
    
    def get_analysis_statistics(self) -> Dict[str, Any]:
        """Get statistics about analysis results"""
        return self.analysis_dao.get_statistics()
    
    # Category operations
    def import_categories_from_csv(self, csv_file: str) -> Tuple[int, int]:
        """Import categories and valid combinations from CSV file"""
        return self.category_dao.import_categories_from_csv(csv_file)
    
    def get_all_categories(self) -> Dict[str, List[str]]:
        """Get all categories grouped by level"""
        return self.category_dao.get_all_categories()
    
    def get_valid_combinations(self) -> List[Dict[str, str]]:
        """Get all valid category combinations"""
        return self.category_dao.get_valid_combinations()
    
    def is_valid_combination(self, l1: str, l2: str, l3: str) -> bool:
        """Check if a category combination is valid"""
        return self.category_dao.is_valid_combination(l1, l2, l3)
    
    def get_categories_by_level(self, level: str) -> List[str]:
        """Get categories for a specific level"""
        return self.category_dao.get_categories_by_level(level)
    
    def add_category(self, level: str, name: str, description: str = "") -> bool:
        """Add a new category"""
        return self.category_dao.add_category(level, name, description)
    
    def add_valid_combination(self, l1: str, l2: str, l3: str) -> bool:
        """Add a new valid category combination"""
        return self.category_dao.add_valid_combination(l1, l2, l3)
    
    # Statistics operations
    def save_analysis_stats(self, stats: Dict[str, Any]) -> bool:
        """Save analysis run statistics"""
        return self.stats_dao.save_stats(stats)
    
    def get_recent_analysis_runs(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get information about recent analysis runs"""
        return self.stats_dao.get_recent_runs(limit)
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get summary statistics across the system"""
        return self.stats_dao.get_summary_stats()
    
    def get_performance_statistics(self, days: int = 30) -> Dict[str, Any]:
        """Get performance statistics for a specific time period"""
        return self.stats_dao.get_performance_stats(days)
    
    # Configuration operations
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a configuration value"""
        try:
            return self.config_dao.get_config(key)
        except RecordNotFoundError:
            return default
    
    def set_config(self, key: str, value: Any, description: str = None, updated_by: str = None) -> bool:
        """Set a configuration value"""
        return self.config_dao.save_config(key, value, description, updated_by)
    
    def get_all_configs(self) -> Dict[str, Any]:
        """Get all configuration settings"""
        return self.config_dao.get_all_configs()
    
    # User operations
    def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate a user"""
        return self.user_dao.authenticate_user(username, password)
    
    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get a user by ID"""
        try:
            return self.user_dao.get_user_by_id(user_id)
        except RecordNotFoundError:
            return None
    
    def create_user(self, username: str, password: str, email: str = None, 
                   first_name: str = None, last_name: str = None, 
                   role: str = 'analyst') -> Optional[str]:
        """Create a new user"""
        try:
            return self.user_dao.create_user(username, password, email, first_name, last_name, role)
        except DatabaseError:
            return None
    
    def create_session(self, user_id: str, ip_address: str = None, 
                      user_agent: str = None, duration: int = 86400) -> Optional[str]:
        """Create a new session for a user"""
        try:
            return self.user_dao.create_session(user_id, ip_address, user_agent, duration)
        except DatabaseError:
            return None
    
    def validate_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Validate a session and get associated user"""
        return self.user_dao.validate_session(session_id)
    
    def invalidate_session(self, session_id: str) -> bool:
        """Invalidate a session (logout)"""
        return self.user_dao.invalidate_session(session_id)
    
    # Direct query methods (for compatibility with legacy code)
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return the results"""
        base_dao = BaseDAO(self.db_path)
        return base_dao.execute_query(query, params)
    
    def execute_update(self, query: str, params: tuple = ()) -> int:
        """Execute a raw SQL update query and return the number of affected rows"""
        base_dao = BaseDAO(self.db_path)
        return base_dao.execute_update(query, params)
    
    def execute_batch(self, query: str, params_list: List[tuple]) -> int:
        """Execute a batch of raw SQL update queries"""
        base_dao = BaseDAO(self.db_path)
        return base_dao.execute_batch(query, params_list) 