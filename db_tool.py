#!/usr/bin/env python3
"""
Database Tool for Call Center Analysis System
Command-line tool for testing the database layer.
"""

import argparse
import logging
import sys
import json
from typing import Dict, Any, List
import os
import traceback
from pprint import pprint

# Import DAOs
from dao.base_dao import BaseDAO
from dao.transcription_dao import TranscriptionDAO
from dao.analysis_dao import AnalysisResultDAO
from dao.category_dao import CategoryDAO
from dao.stats_dao import StatsDAO
from dao.user_dao import UserDAO
from dao.config_dao import ConfigDAO

# Import configuration manager
from config_manager import config

# Import exceptions
from exceptions.database_exceptions import DatabaseError, RecordNotFoundError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("db_tool.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseTool:
    """Command-line tool for testing and administering the database layer"""
    
    def __init__(self, db_path: str = None):
        """Initialize the database tool"""
        self.db_path = db_path or config.get("db_path")
        
        # Create DAO instances
        self.transcription_dao = TranscriptionDAO(self.db_path)
        self.analysis_dao = AnalysisResultDAO(self.db_path)
        self.category_dao = CategoryDAO(self.db_path)
        self.stats_dao = StatsDAO(self.db_path)
        self.user_dao = UserDAO(self.db_path)
        self.config_dao = ConfigDAO(self.db_path)
        
        logger.info("Database tool initialized with database: {}".format(self.db_path))
    
    def list_transcriptions(self, limit: int = 10):
        """List transcriptions in the database"""
        transcriptions = self.transcription_dao.get_all(limit)
        
        print("Found {} transcriptions:".format(len(transcriptions)))
        for t in transcriptions:
            print("ID: {}, File: {}, Date: {}".format(
                t.get('call_id'), t.get('file_name'), t.get('call_date')))
            # Truncate transcription to preview
            transcription = t.get('transcription', '')
            if transcription:
                print("  Preview: {}...".format(transcription[:80]))
            print()
    
    def list_analysis_results(self, limit: int = 10):
        """List analysis results in the database"""
        results = self.analysis_dao.get_all(limit)
        
        print("Found {} analysis results:".format(len(results)))
        for r in results:
            print("ID: {}, Status: {}".format(r.get('call_id'), r.get('analysis_status')))
            print("  Primary Issue: {}".format(r.get('primary_issue_category')))
            print("  Confidence: {}".format(r.get('confidence_score')))
            print()
    
    def list_categories(self):
        """List categories in the database"""
        categories = self.category_dao.get_all_categories()
        
        print("Categories:")
        for level, cats in categories.items():
            print("{}: {}".format(level, ', '.join(cats)))
        print()
    
    def list_valid_combinations(self, limit: int = 10):
        """List valid category combinations"""
        combinations = self.category_dao.get_valid_combinations()
        
        print("Found {} valid combinations (showing {}):".format(
            len(combinations), min(limit, len(combinations))))
        for i, c in enumerate(combinations[:limit]):
            print("{}. L1: {}, L2: {}, L3: {}".format(
                i+1, c.get('l1_category'), c.get('l2_category'), c.get('l3_category')))
        print()
    
    def show_statistics(self):
        """Show system statistics"""
        stats = self.stats_dao.get_summary_stats()
        
        print("System Statistics:")
        print("Total Transcriptions: {}".format(stats.get('total_transcriptions', 0)))
        print("Total Analyzed Calls: {}".format(stats.get('total_analyzed', 0)))
        print("Completed Analyses: {}".format(stats.get('completed_analyses', 0)))
        print("Failed Analyses: {}".format(stats.get('failed_analyses', 0)))
        print("Average Confidence Score: {:.2f}".format(stats.get('avg_confidence', 0)))
        print("Average Processing Time: {:.2f} ms".format(stats.get('avg_processing_time', 0)))
        
        if 'issue_categories' in stats:
            print("\nTop Issue Categories:")
            for cat in stats['issue_categories'][:5]:
                print("  {}: {} calls".format(
                    cat.get('primary_issue_category'), cat.get('count')))
        print()
    
    def list_users(self):
        """List users in the database"""
        users = self.user_dao.get_active_users()
        
        print("Found {} active users:".format(len(users)))
        for u in users:
            print("ID: {}, Username: {}, Role: {}".format(
                u.get('user_id'), u.get('username'), u.get('role')))
            if u.get('email'):
                print("  Email: {}".format(u.get('email')))
            print("  Last Login: {}".format(u.get('last_login')))
            print()
    
    def list_configuration(self):
        """List configuration settings"""
        configs = self.config_dao.get_all_configs()
        
        print("Found {} configuration settings:".format(len(configs)))
        for key, value in configs.items():
            print("{}: {}".format(key, value))
        print()
    
    def show_system_config(self):
        """Show the merged system configuration"""
        system_config = config.get_all()
        
        print("System Configuration:")
        for key, value in sorted(system_config.items()):
            source_value, source = config.get_with_source(key)
            print("{}: {} (source: {})".format(key, value, source))
        print()
    
    def create_test_user(self, username: str, password: str):
        """Create a test user"""
        try:
            user_id = self.user_dao.create_user(
                username=username,
                password=password,
                email="{}@example.com".format(username),
                first_name="Test",
                last_name="User",
                role="analyst"
            )
            print("Created user with ID: {}".format(user_id))
        except DatabaseError as e:
            print("Error creating user: {}".format(str(e)))
    
    def test_authentication(self, username: str, password: str):
        """Test user authentication"""
        user = self.user_dao.authenticate_user(username, password)
        
        if user:
            print("Authentication successful for user: {}".format(username))
            print("User ID: {}".format(user.get('user_id')))
            print("Role: {}".format(user.get('role')))
        else:
            print("Authentication failed for user: {}".format(username))
    
    def save_config(self, key: str, value: str):
        """Save a configuration value"""
        # Try to convert value to proper type
        converted_value = value
        
        # Try as int
        try:
            converted_value = int(value)
        except ValueError:
            # Try as float
            try:
                converted_value = float(value)
            except ValueError:
                # Try as boolean
                if value.lower() in ('true', 'yes', 'y', '1'):
                    converted_value = True
                elif value.lower() in ('false', 'no', 'n', '0'):
                    converted_value = False
                # Try as JSON
                elif value.startswith('{') or value.startswith('['):
                    try:
                        converted_value = json.loads(value)
                    except json.JSONDecodeError:
                        # Keep as string
                        pass
        
        # Save the configuration
        success = self.config_dao.save_config(key, converted_value)
        
        if success:
            print("Saved configuration: {} = {}".format(key, converted_value))
        else:
            print("Failed to save configuration: {}".format(key))
    
    def run_command(self, command: str, args: List[str]):
        """Run a command with arguments"""
        if command == "list-transcriptions":
            limit = int(args[0]) if args else 10
            self.list_transcriptions(limit)
        
        elif command == "list-analysis":
            limit = int(args[0]) if args else 10
            self.list_analysis_results(limit)
        
        elif command == "list-categories":
            self.list_categories()
        
        elif command == "list-combinations":
            limit = int(args[0]) if args else 10
            self.list_valid_combinations(limit)
        
        elif command == "show-stats":
            self.show_statistics()
        
        elif command == "list-users":
            self.list_users()
        
        elif command == "list-config":
            self.list_configuration()
        
        elif command == "show-system-config":
            self.show_system_config()
        
        elif command == "create-user":
            if len(args) < 2:
                print("Error: create-user requires username and password arguments")
                return
            self.create_test_user(args[0], args[1])
        
        elif command == "test-auth":
            if len(args) < 2:
                print("Error: test-auth requires username and password arguments")
                return
            self.test_authentication(args[0], args[1])
        
        elif command == "save-config":
            if len(args) < 2:
                print("Error: save-config requires key and value arguments")
                return
            self.save_config(args[0], args[1])
        
        else:
            print("Unknown command: {}".format(command))
            print("Available commands:")
            print("  list-transcriptions [limit]")
            print("  list-analysis [limit]")
            print("  list-categories")
            print("  list-combinations [limit]")
            print("  show-stats")
            print("  list-users")
            print("  list-config")
            print("  show-system-config")
            print("  create-user <username> <password>")
            print("  test-auth <username> <password>")
            print("  save-config <key> <value>")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Database Tool for Call Center Analysis System")
    
    parser.add_argument("--db", dest="db_path", help="Path to the database file")
    parser.add_argument("command", nargs="?", help="Command to run")
    parser.add_argument("args", nargs="*", help="Command arguments")
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_args()
    
    try:
        db_tool = DatabaseTool(args.db_path)
        
        if args.command:
            db_tool.run_command(args.command, args.args)
        else:
            print("No command specified. Run with --help for usage information.")
    
    except Exception as e:
        logger.error("Error: {}".format(str(e)))
        logger.debug(traceback.format_exc())
        print("Error: {}".format(str(e)))
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 