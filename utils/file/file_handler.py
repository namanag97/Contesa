#!/usr/bin/env python3
"""
File Handling Utilities
Provides standardized file operations.
"""

import os
import glob
import logging
import shutil
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

class FileHandler:
    """Utilities for handling files"""
    
    @staticmethod
    def ensure_directory(directory_path: str) -> bool:
        """
        Ensure a directory exists, creating it if necessary
        
        Args:
            directory_path: Path to the directory
            
        Returns:
            True if directory exists or was created, False otherwise
        """
        try:
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
                logger.info(f"Created directory: {directory_path}")
            return True
        except Exception as e:
            logger.error(f"Error creating directory {directory_path}: {str(e)}")
            return False
    
    @staticmethod
    def get_files_by_extension(directory_path: str, extensions: List[str]) -> List[str]:
        """
        Get list of files with specified extensions
        
        Args:
            directory_path: Path to search
            extensions: List of file extensions (e.g., ['.csv', '.txt'])
            
        Returns:
            List of file paths
        """
        try:
            if not os.path.exists(directory_path):
                logger.warning(f"Directory not found: {directory_path}")
                return []
            
            files = []
            for ext in extensions:
                # Ensure extension starts with a dot
                if not ext.startswith('.'):
                    ext = f'.{ext}'
                    
                files.extend(glob.glob(os.path.join(directory_path, f"*{ext}")))
            
            logger.info(f"Found {len(files)} files with extensions {extensions} in {directory_path}")
            return files
        except Exception as e:
            logger.error(f"Error finding files in {directory_path}: {str(e)}")
            return []
    
    @staticmethod
    def create_backup(file_path: str, backup_dir: Optional[str] = None) -> Optional[str]:
        """
        Create a backup of a file
        
        Args:
            file_path: Path to the file to backup
            backup_dir: Optional directory for backups, defaults to a 'backups' folder in the same directory
            
        Returns:
            Path to backup file or None if backup failed
        """
        try:
            if not os.path.exists(file_path):
                logger.warning(f"File not found, cannot create backup: {file_path}")
                return None
                
            # Create timestamp for unique backup filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Determine backup directory
            if backup_dir is None:
                backup_dir = os.path.join(os.path.dirname(file_path), "backups")
                
            # Ensure backup directory exists
            FileHandler.ensure_directory(backup_dir)
            
            # Create backup filename
            filename = os.path.basename(file_path)
            base_name, extension = os.path.splitext(filename)
            backup_filename = f"{base_name}_{timestamp}{extension}"
            backup_path = os.path.join(backup_dir, backup_filename)
            
            # Copy file to backup location
            shutil.copy2(file_path, backup_path)
            logger.info(f"Created backup of {file_path} at {backup_path}")
            
            return backup_path
        except Exception as e:
            logger.error(f"Error creating backup of {file_path}: {str(e)}")
            return None
    
    @staticmethod
    def safe_write_csv(df: pd.DataFrame, file_path: str, create_backup: bool = True) -> bool:
        """
        Safely write DataFrame to CSV with backup
        
        Args:
            df: DataFrame to write
            file_path: Path to output file
            create_backup: Whether to create backup of existing file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create backup if file exists and backup is requested
            if os.path.exists(file_path) and create_backup:
                FileHandler.create_backup(file_path)
            
            # Create temporary file
            temp_file = f"{file_path}.temp"
            
            # Write to temporary file first
            df.to_csv(temp_file, index=False)
            
            # Rename temporary file to final name (atomic operation)
            if os.path.exists(file_path):
                os.replace(temp_file, file_path)
            else:
                os.rename(temp_file, file_path)
                
            logger.info(f"Successfully wrote DataFrame with {len(df)} records to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error writing CSV to {file_path}: {str(e)}")
            
            # Try emergency save if regular save failed
            try:
                emergency_file = f"emergency_{os.path.basename(file_path)}"
                df.to_csv(emergency_file, index=False)
                logger.warning(f"Emergency save to {emergency_file}")
            except Exception as emergency_error:
                logger.critical(f"Emergency save also failed: {str(emergency_error)}")
                
            return False
    
    @staticmethod
    def load_json(file_path: str) -> Optional[Dict[str, Any]]:
        """
        Load JSON file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Parsed JSON data or None if loading failed
        """
        try:
            if not os.path.exists(file_path):
                logger.warning(f"JSON file not found: {file_path}")
                return None
                
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
            logger.info(f"Successfully loaded JSON from {file_path}")
            return data
        except Exception as e:
            logger.error(f"Error loading JSON from {file_path}: {str(e)}")
            return None
    
    @staticmethod
    def save_json(data: Dict[str, Any], file_path: str, pretty: bool = True) -> bool:
        """
        Save data to JSON file
        
        Args:
            data: Data to save
            file_path: Path to output file
            pretty: Whether to format JSON with indentation
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Write JSON to file
            with open(file_path, 'w', encoding='utf-8') as file:
                if pretty:
                    json.dump(data, file, indent=2, ensure_ascii=False)
                else:
                    json.dump(data, file, ensure_ascii=False)
                    
            logger.info(f"Successfully saved JSON to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving JSON to {file_path}: {str(e)}")
            return False 