#!/usr/bin/env python3
"""
Configuration Data Access Object
Provides database operations for system configuration settings.
"""

import logging
import json
from typing import Dict, Any, Optional, List
import sqlite3

from dao.base_dao import BaseDAO
from exceptions.database_exceptions import DatabaseError, RecordNotFoundError

# Configure logging
logger = logging.getLogger(__name__)

class ConfigDAO(BaseDAO):
    """
    Data Access Object for system configuration settings
    Handles persistent configuration stored in the database
    """
    
    TABLE_NAME = "system_config"
    ID_FIELD = "config_key"
    
    def __init__(self, db_path: str):
        """
        Initialize the Config DAO with database connection
        
        Args:
            db_path: Path to the SQLite database
        """
        super().__init__(db_path)
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        """Ensure the configuration table exists"""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_config (
                        config_key TEXT PRIMARY KEY,
                        config_value TEXT NOT NULL,
                        data_type TEXT NOT NULL,
                        description TEXT,
                        last_updated INTEGER NOT NULL,
                        updated_by TEXT
                    )
                """)
                conn.commit()
                logger.debug("Configuration table initialized")
        except sqlite3.Error as e:
            logger.error("Error creating configuration table: {}".format(str(e)))
            raise DatabaseError("Error creating configuration table") from e
    
    def get_config(self, key: str) -> Optional[Any]:
        """
        Get a configuration value by key
        
        Args:
            key: Configuration key to retrieve
            
        Returns:
            The configuration value, converted to the correct type
            
        Raises:
            RecordNotFoundError: If the configuration key is not found
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT config_value, data_type FROM {} WHERE config_key = ?".format(self.TABLE_NAME),
                    (key,)
                )
                
                row = cursor.fetchone()
                if not row:
                    raise RecordNotFoundError("Configuration key not found: {}".format(key))
                
                value, data_type = row['config_value'], row['data_type']
                return self._convert_value(value, data_type)
                
        except sqlite3.Error as e:
            logger.error("Error retrieving configuration: {}".format(str(e)))
            raise DatabaseError("Error retrieving configuration") from e
    
    def get_all_configs(self) -> Dict[str, Any]:
        """
        Get all configuration settings
        
        Returns:
            Dictionary of all configuration settings
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT config_key, config_value, data_type FROM {}".format(self.TABLE_NAME)
                )
                
                configs = {}
                for row in cursor.fetchall():
                    key = row['config_key']
                    value = self._convert_value(row['config_value'], row['data_type'])
                    configs[key] = value
                
                return configs
                
        except sqlite3.Error as e:
            logger.error("Error retrieving all configurations: {}".format(str(e)))
            raise DatabaseError("Error retrieving all configurations") from e
    
    def save_config(self, key: str, value: Any, description: str = None, updated_by: str = None) -> bool:
        """
        Save a configuration value
        
        Args:
            key: Configuration key
            value: Configuration value
            description: Optional description
            updated_by: Optional username who updated the setting
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Determine data type and convert value to string
            data_type, value_str = self._prepare_value(value)
            
            import time
            timestamp = int(time.time())
            
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO {} (config_key, config_value, data_type, description, last_updated, updated_by)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(config_key) DO UPDATE SET
                    config_value = excluded.config_value,
                    data_type = excluded.data_type,
                    description = COALESCE(excluded.description, description),
                    last_updated = excluded.last_updated,
                    updated_by = excluded.updated_by
                    """.format(self.TABLE_NAME),
                    (key, value_str, data_type, description, timestamp, updated_by)
                )
                conn.commit()
                
                logger.info("Saved configuration: {} = {}".format(key, value))
                return True
                
        except Exception as e:
            logger.error("Error saving configuration: {}".format(str(e)))
            raise DatabaseError("Error saving configuration") from e
    
    def delete_config(self, key: str) -> bool:
        """
        Delete a configuration entry
        
        Args:
            key: Configuration key to delete
            
        Returns:
            True if the entry was deleted, False if not found
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "DELETE FROM {} WHERE config_key = ?".format(self.TABLE_NAME),
                    (key,)
                )
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.info("Deleted configuration: {}".format(key))
                    return True
                else:
                    logger.warning("Configuration key not found for deletion: {}".format(key))
                    return False
                
        except sqlite3.Error as e:
            logger.error("Error deleting configuration: {}".format(str(e)))
            raise DatabaseError("Error deleting configuration") from e
    
    def get_configs_by_prefix(self, prefix: str) -> Dict[str, Any]:
        """
        Get configurations by key prefix
        
        Args:
            prefix: Key prefix to filter by
            
        Returns:
            Dictionary of matching configuration entries
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT config_key, config_value, data_type FROM {} WHERE config_key LIKE ?".format(self.TABLE_NAME),
                    (prefix + '%',)
                )
                
                configs = {}
                for row in cursor.fetchall():
                    key = row['config_key']
                    value = self._convert_value(row['config_value'], row['data_type'])
                    configs[key] = value
                
                return configs
                
        except sqlite3.Error as e:
            logger.error("Error retrieving configurations by prefix: {}".format(str(e)))
            raise DatabaseError("Error retrieving configurations by prefix") from e
    
    def save_multiple_configs(self, configs: Dict[str, Any], updated_by: str = None) -> int:
        """
        Save multiple configuration values at once
        
        Args:
            configs: Dictionary of key-value pairs to save
            updated_by: Optional username who updated the settings
            
        Returns:
            Number of configurations saved
        """
        try:
            import time
            timestamp = int(time.time())
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                saved_count = 0
                
                for key, value in configs.items():
                    data_type, value_str = self._prepare_value(value)
                    
                    cursor.execute(
                        """
                        INSERT INTO {} (config_key, config_value, data_type, last_updated, updated_by)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        data_type = excluded.data_type,
                        last_updated = excluded.last_updated,
                        updated_by = excluded.updated_by
                        """.format(self.TABLE_NAME),
                        (key, value_str, data_type, timestamp, updated_by)
                    )
                    saved_count += 1
                
                conn.commit()
                logger.info("Saved {} configuration settings".format(saved_count))
                return saved_count
                
        except Exception as e:
            logger.error("Error saving multiple configurations: {}".format(str(e)))
            raise DatabaseError("Error saving multiple configurations") from e
    
    def get_config_history(self, key: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get configuration change history for a key
        
        Args:
            key: Configuration key
            limit: Maximum number of history entries to return
            
        Returns:
            List of historical changes
        """
        try:
            with self.get_connection() as conn:
                # Check if config_history table exists, create if not
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS config_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        config_key TEXT NOT NULL,
                        old_value TEXT,
                        new_value TEXT NOT NULL,
                        changed_at INTEGER NOT NULL,
                        changed_by TEXT,
                        FOREIGN KEY(config_key) REFERENCES system_config(config_key) ON DELETE CASCADE
                    )
                """)
                
                cursor = conn.execute(
                    """
                    SELECT * FROM config_history
                    WHERE config_key = ?
                    ORDER BY changed_at DESC
                    LIMIT ?
                    """,
                    (key, limit)
                )
                
                return [dict(row) for row in cursor.fetchall()]
                
        except sqlite3.Error as e:
            logger.error("Error retrieving configuration history: {}".format(str(e)))
            raise DatabaseError("Error retrieving configuration history") from e
    
    def _prepare_value(self, value: Any) -> tuple:
        """
        Prepare a value for storage by determining its type and converting to string
        
        Args:
            value: The value to prepare
            
        Returns:
            Tuple of (data_type, value_as_string)
        """
        if value is None:
            return "null", "null"
        elif isinstance(value, bool):
            return "bool", str(value).lower()
        elif isinstance(value, int):
            return "int", str(value)
        elif isinstance(value, float):
            return "float", str(value)
        elif isinstance(value, str):
            return "str", value
        elif isinstance(value, (dict, list)):
            return "json", json.dumps(value)
        else:
            # Default to JSON for complex types
            return "json", json.dumps(str(value))
    
    def _convert_value(self, value_str: str, data_type: str) -> Any:
        """
        Convert a string value from the database to the appropriate type
        
        Args:
            value_str: The string value from the database
            data_type: The data type of the value
            
        Returns:
            The value converted to the appropriate type
        """
        if data_type == "null" or value_str == "null":
            return None
        elif data_type == "bool":
            return value_str.lower() == "true"
        elif data_type == "int":
            return int(value_str)
        elif data_type == "float":
            return float(value_str)
        elif data_type == "str":
            return value_str
        elif data_type == "json":
            try:
                return json.loads(value_str)
            except json.JSONDecodeError:
                logger.error("Error parsing JSON config value: {}".format(value_str))
                return value_str
        else:
            # Default case
            return value_str 