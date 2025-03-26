#!/usr/bin/env python3
"""
Database Setup Script for Call Center Analytics System.
Creates all necessary tables and indexes for the system.
"""

import os
import sys
import sqlite3
import logging
import argparse
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database_setup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseSetup:
    """
    Sets up the database schema for the Call Center Analytics System
    """
    
    def __init__(self, db_path: str):
        """
        Initialize with database path
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        
        # Log whether we're creating a new database or using an existing one
        if not os.path.exists(db_path):
            logger.info(f"Creating new database at {db_path}")
        else:
            logger.info(f"Using existing database at {db_path}")
    
    def connect(self) -> sqlite3.Connection:
        """
        Connect to the database
        
        Returns:
            SQLite connection
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA foreign_keys = ON")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def execute_script(self, conn: sqlite3.Connection, sql_script: str) -> bool:
        """
        Execute an SQL script
        
        Args:
            conn: Database connection
            sql_script: SQL script to execute
            
        Returns:
            Success flag
        """
        try:
            conn.executescript(sql_script)
            return True
        except sqlite3.Error as e:
            logger.error(f"Error executing SQL script: {str(e)}")
            return False
    
    def table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        """
        Check if a table exists
        
        Args:
            conn: Database connection
            table_name: Name of the table to check
            
        Returns:
            True if the table exists, False otherwise
        """
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cursor.fetchone() is not None
    
    def create_tables(self) -> bool:
        """
        Create all tables for the system
        
        Returns:
            Success flag
        """
        conn = None
        try:
            conn = self.connect()
            
            # Define the SQL for creating all tables
            sql_script = """
            -- Call transcriptions table
            CREATE TABLE IF NOT EXISTS call_transcriptions (
                call_id TEXT PRIMARY KEY,
                file_name TEXT NOT NULL,
                file_path TEXT,
                file_size INTEGER,
                call_date TEXT,
                duration_seconds REAL,
                speaker_count INTEGER,
                transcription TEXT,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                analyzed BOOLEAN DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Analysis results table
            CREATE TABLE IF NOT EXISTS analysis_results (
                call_id TEXT PRIMARY KEY,
                analysis_status TEXT NOT NULL,
                primary_issue_category TEXT,
                specific_issue TEXT,
                issue_severity TEXT,
                confidence_score REAL,
                api_error TEXT,
                issue_summary TEXT,
                raw_json TEXT,
                processing_time_ms REAL,
                model TEXT,
                call_date TEXT,
                analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (call_id) REFERENCES call_transcriptions(call_id) ON DELETE CASCADE
            );
            
            -- Categories table
            CREATE TABLE IF NOT EXISTS categories (
                category_id INTEGER PRIMARY KEY AUTOINCREMENT,
                level INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                UNIQUE(level, name)
            );
            
            -- Valid category combinations
            CREATE TABLE IF NOT EXISTS valid_combinations (
                combination_id INTEGER PRIMARY KEY AUTOINCREMENT,
                l1_category TEXT NOT NULL,
                l2_category TEXT NOT NULL,
                l3_category TEXT,
                UNIQUE(l1_category, l2_category, l3_category)
            );
            
            -- Analysis stats table for batch runs
            CREATE TABLE IF NOT EXISTS analysis_stats (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TIMESTAMP NOT NULL,
                total_processed INTEGER NOT NULL,
                successful INTEGER NOT NULL,
                failed INTEGER NOT NULL,
                avg_confidence REAL,
                avg_processing_time REAL,
                model TEXT,
                batch_size INTEGER,
                total_tokens INTEGER,
                total_cost REAL,
                run_duration_seconds REAL
            );
            
            -- Configuration table
            CREATE TABLE IF NOT EXISTS config (
                config_key TEXT PRIMARY KEY,
                config_value TEXT,
                value_type TEXT,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Users table
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                email TEXT UNIQUE,
                role TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            );
            
            -- Sessions table
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                ip_address TEXT,
                user_agent TEXT,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            
            -- Audit log table
            CREATE TABLE IF NOT EXISTS audit_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                resource_type TEXT,
                resource_id TEXT,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            """
            
            # Execute the SQL script
            success = self.execute_script(conn, sql_script)
            
            if success:
                logger.info("Created all tables successfully")
            else:
                logger.error("Failed to create tables")
            
            return success
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
    
    def create_indexes(self) -> bool:
        """
        Create indexes for better query performance
        
        Returns:
            Success flag
        """
        conn = None
        try:
            conn = self.connect()
            
            # Define the SQL for creating indexes
            sql_script = """
            -- Indexes for call_transcriptions
            CREATE INDEX IF NOT EXISTS idx_call_transcriptions_call_date ON call_transcriptions(call_date);
            CREATE INDEX IF NOT EXISTS idx_call_transcriptions_analyzed ON call_transcriptions(analyzed);
            
            -- Indexes for analysis_results
            CREATE INDEX IF NOT EXISTS idx_analysis_results_status ON analysis_results(analysis_status);
            CREATE INDEX IF NOT EXISTS idx_analysis_results_category ON analysis_results(primary_issue_category);
            CREATE INDEX IF NOT EXISTS idx_analysis_results_severity ON analysis_results(issue_severity);
            CREATE INDEX IF NOT EXISTS idx_analysis_results_call_date ON analysis_results(call_date);
            CREATE INDEX IF NOT EXISTS idx_analysis_results_confidence ON analysis_results(confidence_score);
            
            -- Indexes for categories
            CREATE INDEX IF NOT EXISTS idx_categories_level ON categories(level);
            
            -- Indexes for valid_combinations
            CREATE INDEX IF NOT EXISTS idx_valid_combinations_l1 ON valid_combinations(l1_category);
            CREATE INDEX IF NOT EXISTS idx_valid_combinations_l2 ON valid_combinations(l2_category);
            
            -- Indexes for analysis_stats
            CREATE INDEX IF NOT EXISTS idx_analysis_stats_date ON analysis_stats(run_date);
            
            -- Indexes for users
            CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
            CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active);
            
            -- Indexes for sessions
            CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
            CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(is_active);
            
            -- Indexes for audit_log
            CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id);
            CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);
            CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp);
            """
            
            # Execute the SQL script
            success = self.execute_script(conn, sql_script)
            
            if success:
                logger.info("Created all indexes successfully")
            else:
                logger.error("Failed to create indexes")
            
            return success
            
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
    
    def create_admin_user(self, username: str, password: str, email: str = None) -> bool:
        """
        Create an admin user
        
        Args:
            username: Username for the admin user
            password: Password for the admin user
            email: Email for the admin user
            
        Returns:
            Success flag
        """
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Check if the user already exists
            cursor.execute("SELECT user_id FROM users WHERE username = ?", (username,))
            if cursor.fetchone():
                logger.warning(f"User {username} already exists")
                return True
            
            # Import password hashing function
            import hashlib
            
            # Hash the password (in a real implementation, use a proper password hashing library like bcrypt)
            password_hash = hashlib.sha256(password.encode()).hexdigest()
            
            # Insert the user
            cursor.execute(
                "INSERT INTO users (username, password_hash, email, role) VALUES (?, ?, ?, ?)",
                (username, password_hash, email, "admin")
            )
            
            conn.commit()
            logger.info(f"Created admin user {username}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating admin user: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def add_default_categories(self) -> bool:
        """
        Add default categories for call classification
        
        Returns:
            Success flag
        """
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Check if categories already exist
            cursor.execute("SELECT COUNT(*) FROM categories")
            if cursor.fetchone()[0] > 0:
                logger.info("Categories already exist, skipping default categories")
                return True
            
            # Define default categories
            categories = [
                # Level 1 categories
                (1, "Account Access", "Issues related to logging in or accessing accounts"),
                (1, "Billing", "Issues related to bills, payments, or charges"),
                (1, "Technical Issue", "Technical problems with systems or applications"),
                (1, "Product Information", "Questions about products or services"),
                (1, "Complaint", "Customer complaints about service or experience"),
                
                # Level 2 categories
                (2, "Login Problem", "Problems logging into accounts"),
                (2, "Password Reset", "Assistance with password resets"),
                (2, "Account Locked", "Account is locked due to security reasons"),
                (2, "Payment Issue", "Problems with payments"),
                (2, "Billing Error", "Errors on bills or statements"),
                (2, "Refund Request", "Requests for refunds"),
                (2, "App Error", "Errors in mobile or web applications"),
                (2, "System Unavailable", "Systems that are down or unreachable"),
                (2, "Feature Question", "Questions about specific features"),
                (2, "Product Comparison", "Comparing different products or plans"),
                (2, "Service Complaint", "Complaints about service quality"),
                (2, "Staff Complaint", "Complaints about staff behavior"),
                
                # Level 3 categories
                (3, "Forgotten Password", "User has forgotten their password"),
                (3, "Account Verification", "Issues verifying account identity"),
                (3, "Two-Factor Authentication", "Issues with 2FA"),
                (3, "Payment Declined", "Payment method was declined"),
                (3, "Double Charge", "Customer was charged twice"),
                (3, "Missing Credit", "Credit not applied to account"),
                (3, "App Crash", "Application crashes or freezes"),
                (3, "Data Not Loading", "Data fails to load in application"),
                (3, "Feature Not Working", "Specific feature not functioning"),
                (3, "Pricing Question", "Questions about pricing"),
                (3, "Feature Availability", "Availability of features"),
                (3, "Service Quality", "Issues with service quality"),
                (3, "Wait Time", "Complaints about wait times"),
                (3, "Employee Attitude", "Complaints about employee attitude"),
            ]
            
            # Insert categories
            for level, name, description in categories:
                cursor.execute(
                    "INSERT INTO categories (level, name, description) VALUES (?, ?, ?)",
                    (level, name, description)
                )
            
            # Define valid combinations
            combinations = [
                # Account Access combinations
                ("Account Access", "Login Problem", "Forgotten Password"),
                ("Account Access", "Login Problem", "Account Verification"),
                ("Account Access", "Password Reset", "Forgotten Password"),
                ("Account Access", "Password Reset", "Two-Factor Authentication"),
                ("Account Access", "Account Locked", "Account Verification"),
                
                # Billing combinations
                ("Billing", "Payment Issue", "Payment Declined"),
                ("Billing", "Billing Error", "Double Charge"),
                ("Billing", "Refund Request", "Double Charge"),
                ("Billing", "Billing Error", "Missing Credit"),
                
                # Technical Issue combinations
                ("Technical Issue", "App Error", "App Crash"),
                ("Technical Issue", "App Error", "Data Not Loading"),
                ("Technical Issue", "System Unavailable", "Feature Not Working"),
                
                # Product Information combinations
                ("Product Information", "Feature Question", "Feature Availability"),
                ("Product Information", "Product Comparison", "Pricing Question"),
                
                # Complaint combinations
                ("Complaint", "Service Complaint", "Service Quality"),
                ("Complaint", "Service Complaint", "Wait Time"),
                ("Complaint", "Staff Complaint", "Employee Attitude"),
            ]
            
            # Insert valid combinations
            for l1, l2, l3 in combinations:
                cursor.execute(
                    "INSERT INTO valid_combinations (l1_category, l2_category, l3_category) VALUES (?, ?, ?)",
                    (l1, l2, l3)
                )
            
            conn.commit()
            logger.info(f"Added {len(categories)} default categories and {len(combinations)} valid combinations")
            return True
            
        except Exception as e:
            logger.error(f"Error adding default categories: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def setup(self, create_admin: bool = True, add_categories: bool = True) -> bool:
        """
        Set up the database
        
        Args:
            create_admin: Whether to create an admin user
            add_categories: Whether to add default categories
            
        Returns:
            Success flag
        """
        try:
            # Step 1: Create tables
            if not self.create_tables():
                return False
            
            # Step 2: Create indexes
            if not self.create_indexes():
                return False
            
            # Step 3: Create admin user if requested
            if create_admin:
                if not self.create_admin_user("admin", "admin123", "admin@example.com"):
                    logger.warning("Failed to create admin user")
            
            # Step 4: Add default categories if requested
            if add_categories:
                if not self.add_default_categories():
                    logger.warning("Failed to add default categories")
            
            logger.info("Database setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up database: {str(e)}")
            return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Set up the database for Call Center Analytics System")
    parser.add_argument("--db-path", default="contesa.db", help="Path to the database file")
    parser.add_argument("--no-admin", action="store_true", help="Skip creating admin user")
    parser.add_argument("--no-categories", action="store_true", help="Skip adding default categories")
    parser.add_argument("--admin-username", default="admin", help="Admin username")
    parser.add_argument("--admin-password", default="admin123", help="Admin password")
    parser.add_argument("--admin-email", default="admin@example.com", help="Admin email")
    
    args = parser.parse_args()
    
    # Create database setup
    db_setup = DatabaseSetup(args.db_path)
    
    # Set up the database
    success = db_setup.setup(
        create_admin=not args.no_admin,
        add_categories=not args.no_categories
    )
    
    # If admin user creation is enabled but failed, try again with custom credentials
    if not args.no_admin and success and not db_setup.create_admin_user(
        args.admin_username, args.admin_password, args.admin_email
    ):
        logger.warning("Failed to create admin user with custom credentials")
    
    if success:
        logger.info("Database setup completed successfully")
        return 0
    else:
        logger.error("Database setup failed")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 