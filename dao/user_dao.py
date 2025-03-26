#!/usr/bin/env python3
"""
User Data Access Object for the Call Center Analysis System.
Handles database operations related to users and authentication.
"""

import logging
import hashlib
import os
import time
from typing import Dict, List, Optional, Tuple, Any
import sqlite3

from dao.base_dao import BaseDAO
from exceptions.database_exceptions import DatabaseError, RecordNotFoundError

# Configure logger
logger = logging.getLogger(__name__)

class UserDAO(BaseDAO):
    """
    Data Access Object for user management operations.
    Handles user authentication, profile management, and session tracking.
    """
    
    TABLE_NAME = "users"
    ID_FIELD = "user_id"
    
    def __init__(self, db_path: str):
        """
        Initialize the User DAO with database connection.
        
        Args:
            db_path: Path to the SQLite database
        """
        super().__init__(db_path)
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Ensure all required tables exist for user management"""
        with self.get_connection() as conn:
            # Create users table if not exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    email TEXT UNIQUE,
                    role TEXT NOT NULL DEFAULT 'analyst',
                    first_name TEXT,
                    last_name TEXT,
                    created_at INTEGER NOT NULL,
                    last_login INTEGER,
                    is_active INTEGER NOT NULL DEFAULT 1
                )
            """)
            
            # Create sessions table if not exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    ip_address TEXT,
                    user_agent TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            """)
            
            # Create user_logs table if not exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT,
                    timestamp INTEGER NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            """)
            
            conn.commit()
    
    def get_user_by_id(self, user_id: str) -> Dict[str, Any]:
        """
        Get user by ID
        
        Args:
            user_id: The user ID to look up
            
        Returns:
            Dict containing user data
            
        Raises:
            RecordNotFoundError: If the user is not found
        """
        try:
            return self.get_by_id(user_id)
        except RecordNotFoundError:
            logger.error("User not found with ID: {}".format(user_id))
            raise
    
    def get_user_by_username(self, username: str) -> Dict[str, Any]:
        """
        Get user by username
        
        Args:
            username: The username to look up
            
        Returns:
            Dict containing user data
            
        Raises:
            RecordNotFoundError: If the user is not found
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT * FROM users WHERE username = ?",
                    (username,)
                )
                
                user = cursor.fetchone()
                if not user:
                    raise RecordNotFoundError("User not found with username: {}".format(username))
                
                return dict(user)
        except sqlite3.Error as e:
            logger.error("Database error getting user by username: {}".format(str(e)))
            raise DatabaseError("Error retrieving user data") from e
    
    def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate a user with username and password
        
        Args:
            username: User's username
            password: User's password (plaintext)
            
        Returns:
            Dict containing user data if authentication successful, None otherwise
        """
        try:
            user = self.get_user_by_username(username)
            
            # Check if user is active
            if not user.get('is_active', 0):
                logger.warning("Login attempt for inactive user: {}".format(username))
                return None
            
            # Verify password
            password_hash = self._hash_password(password, username)
            if password_hash != user['password_hash']:
                logger.warning("Failed login attempt for user: {}".format(username))
                return None
            
            # Update last login time
            self._update_last_login(user['user_id'])
            logger.info("User authenticated: {}".format(username))
            
            # Log the successful login
            self.log_user_action(user['user_id'], 'login', 'Successful login')
            
            return user
        except RecordNotFoundError:
            logger.warning("Login attempt for unknown user: {}".format(username))
            return None
        except Exception as e:
            logger.error("Authentication error: {}".format(str(e)))
            return None
    
    def create_user(self, username: str, password: str, email: str = None, 
                   first_name: str = None, last_name: str = None, 
                   role: str = 'analyst') -> str:
        """
        Create a new user
        
        Args:
            username: Unique username
            password: Password (plaintext)
            email: Email address (optional)
            first_name: First name (optional)
            last_name: Last name (optional)
            role: User role (default: 'analyst')
            
        Returns:
            The newly created user ID
            
        Raises:
            DatabaseError: If there is an error creating the user
        """
        try:
            # Hash the password
            password_hash = self._hash_password(password, username)
            
            # Generate unique user ID
            user_id = self._generate_user_id()
            
            # Current timestamp
            current_time = int(time.time())
            
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO users 
                    (user_id, username, password_hash, email, role, 
                     first_name, last_name, created_at, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (user_id, username, password_hash, email, role, 
                     first_name, last_name, current_time)
                )
                conn.commit()
            
            logger.info("Created new user: {}".format(username))
            
            # Log the user creation
            self.log_user_action(user_id, 'user_created', 'New user account created')
            
            return user_id
        except sqlite3.IntegrityError as e:
            logger.error("User creation failed - integrity error: {}".format(str(e)))
            raise DatabaseError("User already exists or constraint violation") from e
        except sqlite3.Error as e:
            logger.error("User creation failed: {}".format(str(e)))
            raise DatabaseError("Error creating user") from e
    
    def update_user(self, user_id: str, updates: Dict[str, Any]) -> bool:
        """
        Update user information
        
        Args:
            user_id: User ID to update
            updates: Dictionary of fields to update (password_hash, email, role, etc.)
            
        Returns:
            Boolean indicating success
            
        Raises:
            RecordNotFoundError: If the user is not found
            DatabaseError: If there is an error updating the user
        """
        try:
            # Check if user exists
            self.get_user_by_id(user_id)
            
            # Don't allow updating user_id, username, created_at
            safe_updates = {k: v for k, v in updates.items() 
                           if k not in ('user_id', 'username', 'created_at')}
            
            if 'password' in safe_updates:
                # If password is in updates, convert to password_hash
                password = safe_updates.pop('password')
                username = self.get_user_by_id(user_id)['username']
                safe_updates['password_hash'] = self._hash_password(password, username)
            
            if not safe_updates:
                return True  # Nothing to update
            
            # Construct update query
            fields = ', '.join(["{}=?".format(k) for k in safe_updates.keys()])
            values = list(safe_updates.values())
            values.append(user_id)
            
            with self.get_connection() as conn:
                conn.execute(
                    "UPDATE users SET {} WHERE user_id = ?".format(fields),
                    values
                )
                conn.commit()
            
            logger.info("Updated user: {}".format(user_id))
            self.log_user_action(user_id, 'user_updated', 'User profile updated')
            
            return True
        except RecordNotFoundError:
            raise
        except sqlite3.Error as e:
            logger.error("User update failed: {}".format(str(e)))
            raise DatabaseError("Error updating user") from e
    
    def deactivate_user(self, user_id: str) -> bool:
        """
        Deactivate a user (soft delete)
        
        Args:
            user_id: User ID to deactivate
            
        Returns:
            Boolean indicating success
        """
        try:
            return self.update_user(user_id, {'is_active': 0})
        except (RecordNotFoundError, DatabaseError):
            return False
    
    def create_session(self, user_id: str, ip_address: str = None, 
                      user_agent: str = None, duration: int = 86400) -> str:
        """
        Create a new session for a user
        
        Args:
            user_id: User ID for the session
            ip_address: Client IP address (optional)
            user_agent: Client user agent (optional)
            duration: Session duration in seconds (default: 1 day)
            
        Returns:
            Session ID 
            
        Raises:
            DatabaseError: If there is an error creating the session
        """
        try:
            # Generate session ID
            session_id = os.urandom(16).hex()
            
            # Current timestamp
            current_time = int(time.time())
            expires_at = current_time + duration
            
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO sessions 
                    (session_id, user_id, created_at, expires_at, ip_address, user_agent)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (session_id, user_id, current_time, expires_at, ip_address, user_agent)
                )
                conn.commit()
            
            logger.info("Created new session for user: {}".format(user_id))
            
            return session_id
        except sqlite3.Error as e:
            logger.error("Session creation failed: {}".format(str(e)))
            raise DatabaseError("Error creating session") from e
    
    def validate_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Validate a session and get associated user
        
        Args:
            session_id: Session ID to validate
            
        Returns:
            User data dict if session is valid, None otherwise
        """
        try:
            current_time = int(time.time())
            
            with self.get_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT u.* FROM users u
                    JOIN sessions s ON u.user_id = s.user_id
                    WHERE s.session_id = ? AND s.expires_at > ? AND u.is_active = 1
                    """,
                    (session_id, current_time)
                )
                
                user = cursor.fetchone()
                if not user:
                    return None
                
                return dict(user)
        except sqlite3.Error as e:
            logger.error("Session validation failed: {}".format(str(e)))
            return None
    
    def invalidate_session(self, session_id: str) -> bool:
        """
        Invalidate a session (logout)
        
        Args:
            session_id: Session ID to invalidate
            
        Returns:
            Boolean indicating success
        """
        try:
            with self.get_connection() as conn:
                # Get user_id for logging
                cursor = conn.execute(
                    "SELECT user_id FROM sessions WHERE session_id = ?",
                    (session_id,)
                )
                
                row = cursor.fetchone()
                if row:
                    user_id = row[0]
                    # Delete the session
                    conn.execute(
                        "DELETE FROM sessions WHERE session_id = ?",
                        (session_id,)
                    )
                    conn.commit()
                    
                    # Log the logout
                    self.log_user_action(user_id, 'logout', 'User logged out')
                    
                    return True
                return False
        except sqlite3.Error as e:
            logger.error("Session invalidation failed: {}".format(str(e)))
            return False
    
    def cleanup_expired_sessions(self) -> int:
        """
        Remove expired sessions from the database
        
        Returns:
            Number of sessions removed
        """
        try:
            current_time = int(time.time())
            
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "DELETE FROM sessions WHERE expires_at <= ?",
                    (current_time,)
                )
                conn.commit()
                
                count = cursor.rowcount
                if count > 0:
                    logger.info("Removed {} expired sessions".format(count))
                    
                return count
        except sqlite3.Error as e:
            logger.error("Session cleanup failed: {}".format(str(e)))
            return 0
    
    def log_user_action(self, user_id: str, action: str, details: str = None) -> bool:
        """
        Log a user action for audit purposes
        
        Args:
            user_id: User ID performing the action
            action: Type of action (login, logout, data_access, etc.)
            details: Additional details about the action
            
        Returns:
            Boolean indicating success
        """
        try:
            current_time = int(time.time())
            
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO user_logs
                    (user_id, action, details, timestamp)
                    VALUES (?, ?, ?, ?)
                    """,
                    (user_id, action, details, current_time)
                )
                conn.commit()
                
                return True
        except sqlite3.Error as e:
            logger.error("Failed to log user action: {}".format(str(e)))
            return False
    
    def get_user_activity_logs(self, user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get activity logs for a specific user
        
        Args:
            user_id: User ID to get logs for
            limit: Maximum number of logs to return
            
        Returns:
            List of activity log entries
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT * FROM user_logs
                    WHERE user_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (user_id, limit)
                )
                
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error("Failed to get user activity logs: {}".format(str(e)))
            return []
    
    def _hash_password(self, password: str, salt: str) -> str:
        """
        Hash a password with a salt
        
        Args:
            password: Password to hash
            salt: Salt to use (username)
            
        Returns:
            Hashed password
        """
        # Use SHA-256 for password hashing - in real app, use better algorithm like bcrypt
        combined = (password + salt).encode('utf-8')
        return hashlib.sha256(combined).hexdigest()
    
    def _generate_user_id(self) -> str:
        """Generate a unique user ID"""
        timestamp = int(time.time() * 1000)
        random_part = os.urandom(4).hex()
        return "usr_{}_{}".format(timestamp, random_part)
    
    def _update_last_login(self, user_id: str) -> bool:
        """Update the last login timestamp for a user"""
        try:
            current_time = int(time.time())
            
            with self.get_connection() as conn:
                conn.execute(
                    "UPDATE users SET last_login = ? WHERE user_id = ?",
                    (current_time, user_id)
                )
                conn.commit()
                
                return True
        except sqlite3.Error as e:
            logger.error("Failed to update last login: {}".format(str(e)))
            return False
    
    def get_active_users(self) -> List[Dict[str, Any]]:
        """
        Get all active users
        
        Returns:
            List of active user records
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT * FROM users
                    WHERE is_active = 1
                    ORDER BY username
                    """
                )
                
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error("Failed to get active users: {}".format(str(e)))
            return []
    
    def count_active_users(self) -> int:
        """
        Count active users in the system
        
        Returns:
            Number of active users
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM users WHERE is_active = 1"
                )
                
                return cursor.fetchone()[0]
        except sqlite3.Error as e:
            logger.error("Failed to count active users: {}".format(str(e)))
            return 0 