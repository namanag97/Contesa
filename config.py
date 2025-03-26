#!/usr/bin/env python3
"""
Configuration module for Call Center Analysis System
Centralizes configuration settings for the entire application
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("contesa.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AppConfig:
    """Application configuration settings"""
    
    # Default paths
    DEFAULT_DB_PATH = "./call_analysis.db"
    DEFAULT_EXPORT_DIR = "./exports"
    DEFAULT_LOGS_DIR = "./logs"
    DEFAULT_BACKUPS_DIR = "./backups"
    DEFAULT_CLIPS_DIR = "./clips"
    
    # API settings
    OPENAI_MODEL = "gpt-4o"
    MAX_RETRIES = 3
    BATCH_SIZE = 10
    MAX_CONCURRENT = 3
    
    @classmethod
    def get_db_path(cls) -> str:
        """Get database path from environment variable or use default"""
        return os.environ.get("CALL_ANALYZER_DB_PATH", cls.DEFAULT_DB_PATH)
    
    @classmethod
    def get_export_dir(cls) -> str:
        """Get export directory from environment variable or use default"""
        export_dir = os.environ.get("CALL_ANALYZER_EXPORT_DIR", cls.DEFAULT_EXPORT_DIR)
        os.makedirs(export_dir, exist_ok=True)
        return export_dir
    
    @classmethod
    def get_logs_dir(cls) -> str:
        """Get logs directory from environment variable or use default"""
        logs_dir = os.environ.get("CALL_ANALYZER_LOGS_DIR", cls.DEFAULT_LOGS_DIR)
        os.makedirs(logs_dir, exist_ok=True)
        return logs_dir
    
    @classmethod
    def get_backups_dir(cls) -> str:
        """Get backups directory from environment variable or use default"""
        backups_dir = os.environ.get("CALL_ANALYZER_BACKUPS_DIR", cls.DEFAULT_BACKUPS_DIR)
        os.makedirs(backups_dir, exist_ok=True)
        return backups_dir
    
    @classmethod
    def get_clips_dir(cls) -> str:
        """Get audio clips directory from environment variable or use default"""
        clips_dir = os.environ.get("CALL_ANALYZER_CLIPS_DIR", cls.DEFAULT_CLIPS_DIR)
        os.makedirs(clips_dir, exist_ok=True)
        return clips_dir
    
    @classmethod
    def get_openai_model(cls) -> str:
        """Get OpenAI model from environment variable or use default"""
        return os.environ.get("OPENAI_MODEL", cls.OPENAI_MODEL)
    
    @classmethod
    def get_api_settings(cls) -> Dict[str, Any]:
        """Get API settings"""
        return {
            "model": cls.get_openai_model(),
            "max_retries": int(os.environ.get("MAX_RETRIES", cls.MAX_RETRIES)),
            "batch_size": int(os.environ.get("BATCH_SIZE", cls.BATCH_SIZE)),
            "max_concurrent": int(os.environ.get("MAX_CONCURRENT", cls.MAX_CONCURRENT))
        }
    
    @classmethod
    def get_api_keys(cls) -> Dict[str, str]:
        """Get API keys from environment variables"""
        return {
            "openai": os.environ.get("OPENAI_API_KEY", ""),
            "elevenlabs": os.environ.get("ELEVENLABS_API_KEY", "")
        }

# Create directories on import
AppConfig.get_export_dir()
AppConfig.get_logs_dir()
AppConfig.get_backups_dir()