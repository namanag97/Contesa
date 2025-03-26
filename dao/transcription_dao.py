#!/usr/bin/env python3
"""
Data Access Object (DAO) for call transcriptions.
Provides database operations for transcriptions.
"""

import sqlite3
import logging
import json
from typing import List, Dict, Any, Optional
import os
import csv
from datetime import datetime

logger = logging.getLogger(__name__)

class TranscriptionDAO:
    """DAO for call transcriptions table"""
    
    TABLE_NAME = "call_transcriptions"
    ID_FIELD = "call_id"
    
    def __init__(self, db_path: str):
        """
        Initialize with database path
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        
        if not os.path.exists(os.path.dirname(self.db_path)):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get a database connection
        
        Returns:
            SQLite connection
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_all(self, limit: int = 100, offset: int = 0, analyzed_only: bool = False) -> List[Dict[str, Any]]:
        """
        Get all transcriptions with pagination
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            analyzed_only: Whether to return only analyzed transcriptions
            
        Returns:
            List of transcription dictionaries
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"SELECT * FROM {self.TABLE_NAME}"
            
            if analyzed_only:
                query += " WHERE analyzed = 1"
                
            query += f" ORDER BY import_date DESC LIMIT ? OFFSET ?"
            
            cursor.execute(query, (limit, offset))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error retrieving transcriptions: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_by_id(self, call_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a transcription by ID
        
        Args:
            call_id: Call ID
            
        Returns:
            Transcription dictionary or None if not found
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"SELECT * FROM {self.TABLE_NAME} WHERE {self.ID_FIELD} = ?"
            
            cursor.execute(query, (call_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
            
        except Exception as e:
            logger.error(f"Error retrieving transcription {call_id}: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()
    
    def save(self, transcription_data: Dict[str, Any]) -> bool:
        """
        Save a transcription to the database (insert or update)
        
        Args:
            transcription_data: Transcription data
            
        Returns:
            Success flag
        """
        conn = None
        try:
            # Make sure required fields are present
            if self.ID_FIELD not in transcription_data:
                raise ValueError(f"Missing required field: {self.ID_FIELD}")
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Check if record exists
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE {self.ID_FIELD} = ?", 
                (transcription_data[self.ID_FIELD],)
            )
            
            exists = cursor.fetchone()[0] > 0
            
            # Prepare fields for insert/update
            fields = [
                "call_id", "file_name", "file_path", "file_size", "call_date",
                "duration_seconds", "speaker_count", "transcription", "analyzed"
            ]
            
            if exists:
                # Update existing record
                set_clause = ", ".join([f"{field} = ?" for field in fields if field in transcription_data])
                set_clause += ", last_updated = CURRENT_TIMESTAMP"
                
                query = f"UPDATE {self.TABLE_NAME} SET {set_clause} WHERE {self.ID_FIELD} = ?"
                
                values = [transcription_data[field] for field in fields if field in transcription_data]
                values.append(transcription_data[self.ID_FIELD])
                
                cursor.execute(query, values)
            else:
                # Insert new record
                available_fields = [field for field in fields if field in transcription_data]
                
                placeholders = ", ".join(["?"] * len(available_fields))
                fields_str = ", ".join(available_fields)
                
                query = f"INSERT INTO {self.TABLE_NAME} ({fields_str}) VALUES ({placeholders})"
                
                values = [transcription_data[field] for field in available_fields]
                
                cursor.execute(query, values)
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error saving transcription: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def mark_as_analyzed(self, call_id: str, analyzed: bool = True) -> bool:
        """
        Mark a transcription as analyzed
        
        Args:
            call_id: Call ID
            analyzed: Analyzed flag
            
        Returns:
            Success flag
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"UPDATE {self.TABLE_NAME} SET analyzed = ?, last_updated = CURRENT_TIMESTAMP WHERE {self.ID_FIELD} = ?"
            
            cursor.execute(query, (1 if analyzed else 0, call_id))
            
            conn.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"Error marking transcription {call_id} as analyzed: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def import_from_csv(self, csv_file: str) -> tuple:
        """
        Import transcriptions from a CSV file
        
        Args:
            csv_file: Path to CSV file
            
        Returns:
            Tuple of (success_count, error_count)
        """
        conn = None
        success_count = 0
        error_count = 0
        
        try:
            conn = self._get_connection()
            
            with open(csv_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    try:
                        # Make sure required field is present
                        if self.ID_FIELD not in row:
                            logger.error(f"Missing required field {self.ID_FIELD} in row: {row}")
                            error_count += 1
                            continue
                        
                        # Save the transcription
                        if self.save(row):
                            success_count += 1
                        else:
                            error_count += 1
                            
                    except Exception as e:
                        logger.error(f"Error importing row: {str(e)}")
                        error_count += 1
            
            return (success_count, error_count)
            
        except Exception as e:
            logger.error(f"Error importing transcriptions: {str(e)}")
            return (success_count, error_count)
        finally:
            if conn:
                conn.close()
    
    def export_to_csv(self, csv_file: str, analyzed_only: bool = False) -> bool:
        """
        Export transcriptions to a CSV file
        
        Args:
            csv_file: Path to CSV file
            analyzed_only: Whether to export only analyzed transcriptions
            
        Returns:
            Success flag
        """
        try:
            # Get all transcriptions
            transcriptions = self.get_all(limit=1000000, analyzed_only=analyzed_only)
            
            if not transcriptions:
                logger.warning("No transcriptions to export")
                return False
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(csv_file)), exist_ok=True)
            
            # Write to CSV
            with open(csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=transcriptions[0].keys())
                writer.writeheader()
                writer.writerows(transcriptions)
            
            logger.info(f"Exported {len(transcriptions)} transcriptions to {csv_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting transcriptions: {str(e)}")
            return False
    
    def get_unanalyzed(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get unanalyzed transcriptions
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of transcription dictionaries
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"SELECT * FROM {self.TABLE_NAME} WHERE analyzed = 0 LIMIT ?"
            
            cursor.execute(query, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error retrieving unanalyzed transcriptions: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()
    
    def count_all(self) -> int:
        """
        Count all transcriptions
        
        Returns:
            Total count
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"SELECT COUNT(*) FROM {self.TABLE_NAME}"
            
            cursor.execute(query)
            
            return cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error counting transcriptions: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()
    
    def count_analyzed(self) -> int:
        """
        Count analyzed transcriptions
        
        Returns:
            Analyzed count
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE analyzed = 1"
            
            cursor.execute(query)
            
            return cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error counting analyzed transcriptions: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()
    
    def delete(self, call_id: str) -> bool:
        """
        Delete a transcription
        
        Args:
            call_id: Call ID
            
        Returns:
            Success flag
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"DELETE FROM {self.TABLE_NAME} WHERE {self.ID_FIELD} = ?"
            
            cursor.execute(query, (call_id,))
            
            conn.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"Error deleting transcription {call_id}: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close() 