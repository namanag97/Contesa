#!/usr/bin/env python3
"""
Configuration Manager for Call Center Analytics System.
Handles loading, reading, and modifying configuration.
"""

import os
import json
import logging
from typing import Any, Dict, Optional, Union, List
import sqlite3

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Configuration Manager for the Call Center Analytics System
    
    Handles:
    - Loading configurations from JSON files
    - Reading configurations from database
    - Merging configurations from multiple sources
    - Providing a single point of access for all configuration
    """
    
    def __init__(self, config_file: str = None, db_path: str = None):
        """
        Initialize the configuration manager
        
        Args:
            config_file: Path to the JSON configuration file
            db_path: Path to the SQLite database
        """
        # Default configuration values
        self.config = {
            "db_path": "contesa.db",
            "log_level": "INFO",
            "clips_dir": "clips",
            "batch_size": 10,
            "openai_model": "gpt-4-turbo",
            "max_retries": 3,
            "rate_limit_rpm": 10
        }
        
        # Load from file if specified
        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)
        
        # Set database path
        self.db_path = db_path or self.config.get("db_path")
        
        # Load from environment variables
        self._load_from_env()
        
        # Load from database
        try:
            self._load_from_db()
        except Exception as e:
            logger.warning(f"Could not load config from database: {str(e)}")
    
    def _load_from_file(self, config_file: str) -> None:
        """
        Load configuration from a JSON file
        
        Args:
            config_file: Path to the JSON configuration file
        """
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                self.config.update(file_config)
                logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading configuration from {config_file}: {str(e)}")
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables"""
        # Define environment variable mappings
        env_mappings = {
            "CONTESA_DB_PATH": "db_path",
            "CONTESA_LOG_LEVEL": "log_level",
            "CONTESA_CLIPS_DIR": "clips_dir",
            "CONTESA_BATCH_SIZE": "batch_size",
            "OPENAI_API_KEY": "openai_api_key",
            "ELEVENLABS_API_KEY": "elevenlabs_api_key",
            "OPENAI_MODEL": "openai_model",
        }
        
        # Update config with environment variables
        for env_var, config_key in env_mappings.items():
            if env_var in os.environ:
                value = os.environ[env_var]
                
                # Convert to appropriate type
                if config_key in self.config and isinstance(self.config[config_key], int):
                    value = int(value)
                elif config_key in self.config and isinstance(self.config[config_key], float):
                    value = float(value)
                elif config_key in self.config and isinstance(self.config[config_key], bool):
                    value = value.lower() in ('true', 'yes', '1')
                
                self.config[config_key] = value
                logger.debug(f"Set {config_key} from environment variable {env_var}")
    
    def _load_from_db(self) -> None:
        """Load configuration from database"""
        if not os.path.exists(self.db_path):
            logger.debug(f"Database {self.db_path} does not exist, skipping config load")
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if config table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='config'")
            if not cursor.fetchone():
                logger.debug("Config table does not exist in database")
                conn.close()
                return
            
            # Get all configurations
            cursor.execute("SELECT config_key, config_value, value_type FROM config")
            rows = cursor.fetchall()
            conn.close()
            
            for key, value, value_type in rows:
                # Convert value based on type
                if value_type == 'int':
                    value = int(value)
                elif value_type == 'float':
                    value = float(value)
                elif value_type == 'bool':
                    value = value.lower() in ('true', 'yes', '1')
                elif value_type == 'json':
                    try:
                        value = json.loads(value)
                    except:
                        logger.warning(f"Failed to parse JSON for config key {key}")
                        continue
                
                self.config[key] = value
                logger.debug(f"Loaded config {key} from database")
                
        except Exception as e:
            logger.error(f"Error loading config from database: {str(e)}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any, description: str = None) -> bool:
        """
        Set a configuration value and optionally save to database
        
        Args:
            key: Configuration key
            value: Configuration value
            description: Optional description
            
        Returns:
            Success flag
        """
        self.config[key] = value
        
        # Save to database if it's available
        if self.db_path:
            try:
                return self._save_to_db(key, value, description)
            except Exception as e:
                logger.error(f"Error saving config to database: {str(e)}")
                return False
        
        return True
    
    def _save_to_db(self, key: str, value: Any, description: str = None) -> bool:
        """
        Save a configuration to database
        
        Args:
            key: Configuration key
            value: Configuration value
            description: Optional description
            
        Returns:
            Success flag
        """
        # Determine value type
        if isinstance(value, int):
            value_type = 'int'
        elif isinstance(value, float):
            value_type = 'float'
        elif isinstance(value, bool):
            value_type = 'bool'
            value = '1' if value else '0'
        elif isinstance(value, (dict, list)):
            value_type = 'json'
            value = json.dumps(value)
        else:
            value_type = 'string'
            value = str(value)
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create table if not exists
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                config_key TEXT PRIMARY KEY,
                config_value TEXT,
                value_type TEXT,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Update or insert
            cursor.execute("""
            INSERT OR REPLACE INTO config (config_key, config_value, value_type, description, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (key, value, value_type, description))
            
            conn.commit()
            logger.debug(f"Saved config {key} to database")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error saving config to database: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get all configuration values
        
        Returns:
            Dictionary of all configuration values
        """
        return self.config.copy()
    
    def load_from_dict(self, config_dict: Dict[str, Any]) -> None:
        """
        Load configuration from a dictionary
        
        Args:
            config_dict: Dictionary of configuration values
        """
        self.config.update(config_dict)
        logger.debug(f"Loaded {len(config_dict)} config values from dictionary")
    
    def save_to_file(self, file_path: str) -> bool:
        """
        Save current configuration to a file
        
        Args:
            file_path: Path where to save the configuration
            
        Returns:
            Success flag
        """
        try:
            with open(file_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"Saved configuration to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving configuration to {file_path}: {str(e)}")
            return False

# Create a singleton instance
config = ConfigManager()

def initialize_config(config_file: str = None, db_path: str = None) -> ConfigManager:
    """
    Initialize the configuration manager
    
    Args:
        config_file: Path to the JSON configuration file
        db_path: Path to the SQLite database
        
    Returns:
        ConfigManager instance
    """
    global config
    config = ConfigManager(config_file, db_path)
    return config 