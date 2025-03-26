#!/usr/bin/env python3
"""
Test script for Call Center Analytics System database layer integration.
This script tests the integration between the DAO layer and business logic.
"""

import os
import sys
import logging
import asyncio
from pprint import pprint
import pandas as pd
from typing import List, Dict, Any, Optional
import json

# Import Database Layer
from dao.base_dao import BaseDAO
from dao.transcription_dao import TranscriptionDAO
from dao.analysis_dao import AnalysisResultDAO
from dao.category_dao import CategoryDAO
from dao.stats_dao import StatsDAO
from dao.config_dao import ConfigDAO
from dao.user_dao import UserDAO

# Import Configuration manager
from config_manager import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("test_integration.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TestIntegration:
    """
    Test the integration between DAOs and business logic
    """
    
    def __init__(self, db_path: str = None):
        """Initialize with database path"""
        self.db_path = db_path or config.get("db_path")
        logger.info(f"Using database: {self.db_path}")
        
        # Initialize DAOs
        self.transcription_dao = TranscriptionDAO(self.db_path)
        self.analysis_dao = AnalysisResultDAO(self.db_path)
        self.category_dao = CategoryDAO(self.db_path)
        self.stats_dao = StatsDAO(self.db_path)
        self.config_dao = ConfigDAO(self.db_path)
        self.user_dao = UserDAO(self.db_path)
    
    def setup_test_data(self):
        """Load sample data for testing"""
        logger.info("Setting up test data...")
        
        # Check if we have sample data in CSV files
        transcription_csv = "call_transcriptions.csv"
        categories_csv = "categories.csv"
        
        if os.path.exists(transcription_csv):
            count = self.transcription_dao.import_from_csv(transcription_csv)
            logger.info(f"Imported {count} transcriptions from {transcription_csv}")
        else:
            logger.warning(f"Sample transcription file {transcription_csv} not found")
        
        if os.path.exists(categories_csv):
            cat_count, comb_count = self.category_dao.import_categories_from_csv(categories_csv)
            logger.info(f"Imported {cat_count} categories and {comb_count} valid combinations from {categories_csv}")
        else:
            logger.warning(f"Sample categories file {categories_csv} not found")
        
        # Set test configurations
        self.config_dao.save_config("test_mode", True, "Flag for test mode")
        self.config_dao.save_config("openai_model", "gpt-4-turbo", "OpenAI model to use")
        
        # Create test user if not exists
        try:
            users = self.user_dao.get_active_users()
            if not any(u.get('username') == 'test_analyst' for u in users):
                user_id = self.user_dao.create_user(
                    username="test_analyst",
                    password="test_password",
                    email="test@example.com",
                    role="analyst"
                )
                logger.info(f"Created test user with ID: {user_id}")
        except Exception as e:
            logger.error(f"Error creating test user: {str(e)}")
    
    def test_transcription_workflow(self):
        """Test transcription workflow"""
        logger.info("Testing transcription workflow...")
        
        # Get sample transcriptions
        transcriptions = self.transcription_dao.get_all(limit=5)
        if not transcriptions:
            logger.warning("No transcriptions found in database")
            return False
        
        logger.info(f"Found {len(transcriptions)} transcriptions")
        for t in transcriptions[:2]:  # Show first two as examples
            logger.info(f"Sample transcription: {t.get('call_id')} - {t.get('file_name')}")
            # Truncate transcription for display
            text = t.get('transcription', '')
            if text:
                logger.info(f"Preview: {text[:100]}...")
        
        # Get transcriptions for analysis
        to_analyze = self.transcription_dao.get_for_analysis(limit=5)
        logger.info(f"Found {len(to_analyze)} transcriptions to analyze")
        
        return len(transcriptions) > 0
    
    def test_category_validation(self):
        """Test category validation logic"""
        logger.info("Testing category validation...")
        
        # Get all categories
        categories = self.category_dao.get_all_categories()
        if not categories:
            logger.warning("No categories found in database")
            return False
        
        # Print category counts by level
        for level, cats in categories.items():
            logger.info(f"{level}: {len(cats)} categories")
        
        # Get valid combinations
        combinations = self.category_dao.get_valid_combinations()
        logger.info(f"Found {len(combinations)} valid combinations")
        
        # Test validation with the first valid combination
        if combinations:
            combo = combinations[0]
            is_valid = self.category_dao.is_valid_combination(
                combo.get('l1_category'), 
                combo.get('l2_category'), 
                combo.get('l3_category')
            )
            logger.info(f"Validation test - Expected: True, Actual: {is_valid}")
            
            # Test with invalid combination
            if len(combinations) > 1:
                # Mix categories from different combinations
                combo1 = combinations[0]
                combo2 = combinations[-1]
                is_invalid = self.category_dao.is_valid_combination(
                    combo1.get('l1_category'),
                    combo2.get('l2_category'),
                    combo1.get('l3_category')
                )
                logger.info(f"Invalid combination test - Expected: False, Actual: {is_invalid}")
        
        return len(categories) > 0
    
    def test_analysis_operations(self):
        """Test analysis operations"""
        logger.info("Testing analysis operations...")
        
        # Create a sample analysis result
        sample_result = {
            "call_id": f"test_call_{int(pd.Timestamp.now().timestamp())}",
            "analysis_status": "completed",
            "primary_issue_category": "Account Access",
            "specific_issue": "Password Reset",
            "issue_severity": "Medium",
            "confidence_score": 0.85,
            "issue_summary": "Customer needed assistance with password reset",
            "processing_time_ms": 1234.5,
            "model": "test-model"
        }
        
        # Save the result
        success = self.analysis_dao.save(sample_result)
        logger.info(f"Save analysis result: {'Success' if success else 'Failed'}")
        
        # Get the result back
        try:
            result = self.analysis_dao.get_by_id(sample_result["call_id"])
            logger.info(f"Retrieved analysis result: {result.get('call_id')} with score {result.get('confidence_score')}")
        except Exception as e:
            logger.error(f"Error retrieving analysis result: {str(e)}")
            result = None
        
        # Get statistics
        stats = self.analysis_dao.get_statistics()
        logger.info(f"Analysis statistics: {len(stats)} metrics found")
        for key, value in stats.items():
            if not isinstance(value, (list, dict)):
                logger.info(f"Stat: {key} = {value}")
        
        return success and result is not None
    
    def test_config_operations(self):
        """Test configuration operations"""
        logger.info("Testing configuration operations...")
        
        # Set various configuration types
        configs = {
            "string_config": "test value",
            "int_config": 12345,
            "float_config": 123.45,
            "bool_config": True,
            "list_config": ["item1", "item2", "item3"],
            "dict_config": {"key1": "value1", "key2": "value2"}
        }
        
        # Save all configs
        for key, value in configs.items():
            self.config_dao.save_config(key, value, f"Test {key}")
        
        # Get all configs
        all_configs = self.config_dao.get_all_configs()
        logger.info(f"Retrieved {len(all_configs)} configurations")
        
        # Check if we can retrieve each config with correct type
        all_correct = True
        for key, expected_value in configs.items():
            try:
                actual_value = self.config_dao.get_config(key)
                correct_type = type(actual_value) == type(expected_value)
                correct_value = actual_value == expected_value
                
                if not (correct_type and correct_value):
                    all_correct = False
                    logger.error(f"Config mismatch for {key}: expected {expected_value} ({type(expected_value).__name__}), got {actual_value} ({type(actual_value).__name__})")
                else:
                    logger.info(f"Config {key}: {actual_value} ({type(actual_value).__name__}) - Correct")
            except Exception as e:
                all_correct = False
                logger.error(f"Error retrieving config {key}: {str(e)}")
        
        return all_correct
    
    def test_user_operations(self):
        """Test user operations"""
        logger.info("Testing user operations...")
        
        # Authenticate with test user
        user = self.user_dao.authenticate_user("test_analyst", "test_password")
        if not user:
            logger.error("Authentication failed for test user")
            return False
        
        logger.info(f"Authentication successful for user: {user.get('username')}")
        
        # Create a session
        try:
            session_id = self.user_dao.create_session(
                user_id=user.get('user_id'),
                ip_address="127.0.0.1",
                user_agent="Test Browser"
            )
            logger.info(f"Created session: {session_id}")
            
            # Validate session
            valid_user = self.user_dao.validate_session(session_id)
            if valid_user:
                logger.info(f"Session validation successful for user: {valid_user.get('username')}")
            else:
                logger.error("Session validation failed")
                return False
            
            # Invalidate session
            invalidated = self.user_dao.invalidate_session(session_id)
            logger.info(f"Session invalidation: {'Success' if invalidated else 'Failed'}")
            
            # Check if session is really invalidated
            invalid_check = self.user_dao.validate_session(session_id)
            if invalid_check:
                logger.error("Session still valid after invalidation")
                return False
            
            logger.info("Session properly invalidated")
            return True
            
        except Exception as e:
            logger.error(f"Error in session management: {str(e)}")
            return False
    
    def test_stats_operations(self):
        """Test statistics operations"""
        logger.info("Testing statistics operations...")
        
        # Create sample stats
        stats = {
            "run_date": pd.Timestamp.now().isoformat(),
            "total_processed": 100,
            "successful": 90,
            "failed": 10,
            "avg_confidence": 0.85,
            "avg_processing_time": 1200.5,
            "model": "gpt-4-turbo",
            "batch_size": 10,
            "total_tokens": 50000,
            "total_cost": 12.34
        }
        
        # Save stats
        saved = self.stats_dao.save_stats(stats)
        logger.info(f"Save stats: {'Success' if saved else 'Failed'}")
        
        # Get recent runs
        runs = self.stats_dao.get_recent_runs(limit=3)
        logger.info(f"Retrieved {len(runs)} recent runs")
        
        # Get summary stats
        summary = self.stats_dao.get_summary_stats()
        logger.info(f"Summary stats: {len(summary)} metrics")
        for key, value in summary.items():
            if not isinstance(value, (list, dict)):
                logger.info(f"Summary: {key} = {value}")
        
        # Get performance stats
        perf = self.stats_dao.get_performance_stats(days=30)
        logger.info(f"Performance stats: {len(perf)} metrics")
        
        return saved and len(runs) > 0
    
    def run_all_tests(self):
        """Run all integration tests"""
        logger.info("Starting integration tests...")
        
        # Setup
        self.setup_test_data()
        
        # Run tests
        test_results = {
            "transcription_workflow": self.test_transcription_workflow(),
            "category_validation": self.test_category_validation(),
            "analysis_operations": self.test_analysis_operations(),
            "config_operations": self.test_config_operations(),
            "user_operations": self.test_user_operations(),
            "stats_operations": self.test_stats_operations(),
        }
        
        # Report results
        logger.info("Test Results:")
        all_passed = True
        for test, result in test_results.items():
            status = "PASSED" if result else "FAILED"
            logger.info(f"  {test}: {status}")
            if not result:
                all_passed = False
        
        if all_passed:
            logger.info("✅ All tests passed! The DAO layer is working correctly.")
        else:
            logger.error("❌ Some tests failed. There are issues with the DAO layer.")
        
        return all_passed

if __name__ == "__main__":
    # Run the integration tests
    tester = TestIntegration()
    success = tester.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1) 