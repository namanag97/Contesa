#!/usr/bin/env python3
"""
Data Access Object (DAO) for analysis results.
Provides database operations for analysis results.
"""

import sqlite3
import logging
import json
from typing import List, Dict, Any, Optional
import os
import csv
from datetime import datetime

logger = logging.getLogger(__name__)

class AnalysisResultDAO:
    """DAO for analysis_results table"""
    
    TABLE_NAME = "analysis_results"
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
    
    def get_all(self, limit: int = 100, offset: int = 0, completed_only: bool = False) -> List[Dict[str, Any]]:
        """
        Get all analysis results with pagination
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            completed_only: Whether to return only completed analyses
            
        Returns:
            List of analysis result dictionaries
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"SELECT * FROM {self.TABLE_NAME}"
            
            if completed_only:
                query += " WHERE analysis_status = 'completed'"
                
            query += f" ORDER BY analysis_date DESC LIMIT ? OFFSET ?"
            
            cursor.execute(query, (limit, offset))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error retrieving analysis results: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_by_id(self, call_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an analysis result by ID
        
        Args:
            call_id: Call ID
            
        Returns:
            Analysis result dictionary or None if not found
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
            logger.error(f"Error retrieving analysis result {call_id}: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()
    
    def save(self, analysis_data: Dict[str, Any]) -> bool:
        """
        Save an analysis result to the database (insert or update)
        
        Args:
            analysis_data: Analysis result data
            
        Returns:
            Success flag
        """
        conn = None
        try:
            # Make sure required fields are present
            if self.ID_FIELD not in analysis_data:
                raise ValueError(f"Missing required field: {self.ID_FIELD}")
            
            # Extract JSON data if any
            raw_json = None
            if "raw_json" not in analysis_data:
                # Extract fields that should go in the raw_json field
                json_fields = {}
                for key in list(analysis_data.keys()):
                    if key not in [
                        "call_id", "analysis_status", "primary_issue_category", 
                        "specific_issue", "issue_severity", "confidence_score", 
                        "api_error", "issue_summary", "processing_time_ms", 
                        "model", "call_date", "analysis_date", "raw_json"
                    ]:
                        json_fields[key] = analysis_data.pop(key)
                
                if json_fields:
                    raw_json = json.dumps(json_fields)
            
            # If we have data for raw_json, add it to analysis_data
            if raw_json:
                analysis_data["raw_json"] = raw_json
            
            # Extract fields from raw_json if they're available
            if "raw_json" in analysis_data and isinstance(analysis_data["raw_json"], str):
                try:
                    json_data = json.loads(analysis_data["raw_json"])
                    
                    # Extract primary category
                    if "primary_issue_category" not in analysis_data and "issue_classification" in json_data:
                        analysis_data["primary_issue_category"] = json_data["issue_classification"].get("primary_category")
                    
                    # Extract specific issue
                    if "specific_issue" not in analysis_data and "issue_classification" in json_data:
                        analysis_data["specific_issue"] = json_data["issue_classification"].get("specific_issue")
                    
                    # Extract severity
                    if "issue_severity" not in analysis_data and "issue_classification" in json_data:
                        analysis_data["issue_severity"] = json_data["issue_classification"].get("severity")
                    
                    # Extract issue summary if not already present
                    if "issue_summary" not in analysis_data and "issue_summary" in json_data:
                        analysis_data["issue_summary"] = json_data["issue_summary"]
                except Exception as e:
                    logger.warning(f"Error parsing raw_json: {str(e)}")
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Check if record exists
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE {self.ID_FIELD} = ?", 
                (analysis_data[self.ID_FIELD],)
            )
            
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                # Update existing record
                fields = []
                values = []
                
                for key, value in analysis_data.items():
                    if key != self.ID_FIELD:
                        fields.append(f"{key} = ?")
                        values.append(value)
                
                set_clause = ", ".join(fields)
                
                query = f"UPDATE {self.TABLE_NAME} SET {set_clause} WHERE {self.ID_FIELD} = ?"
                
                values.append(analysis_data[self.ID_FIELD])
                
                cursor.execute(query, values)
            else:
                # Insert new record
                fields = list(analysis_data.keys())
                placeholders = ", ".join(["?"] * len(fields))
                
                query = f"INSERT INTO {self.TABLE_NAME} ({', '.join(fields)}) VALUES ({placeholders})"
                
                values = [analysis_data[field] for field in fields]
                
                cursor.execute(query, values)
            
            conn.commit()
            
            # Update transcription analyzed status
            self._update_transcription_analyzed(conn, analysis_data[self.ID_FIELD])
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving analysis result: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def _update_transcription_analyzed(self, conn: sqlite3.Connection, call_id: str) -> None:
        """
        Update the analyzed status of a transcription
        
        Args:
            conn: Database connection
            call_id: Call ID
        """
        try:
            cursor = conn.cursor()
            
            query = "UPDATE call_transcriptions SET analyzed = 1, last_updated = CURRENT_TIMESTAMP WHERE call_id = ?"
            
            cursor.execute(query, (call_id,))
            
            conn.commit()
        except Exception as e:
            logger.error(f"Error updating transcription analyzed status: {str(e)}")
    
    def get_by_criteria(self, criteria: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get analysis results by criteria
        
        Args:
            criteria: Dictionary of criteria
            limit: Maximum number of records to return
            
        Returns:
            List of analysis result dictionaries
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            where_clauses = []
            values = []
            
            for key, value in criteria.items():
                if key in ["primary_issue_category", "specific_issue", "issue_severity", "analysis_status"]:
                    where_clauses.append(f"{key} = ?")
                    values.append(value)
            
            if not where_clauses:
                return self.get_all(limit=limit)
            
            query = f"SELECT * FROM {self.TABLE_NAME} WHERE {' AND '.join(where_clauses)} ORDER BY analysis_date DESC LIMIT ?"
            
            values.append(limit)
            
            cursor.execute(query, values)
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error retrieving analysis results by criteria: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about analysis results
        
        Returns:
            Dictionary of statistics
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            statistics = {}
            
            # Count of analyzed calls
            cursor.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME}")
            statistics["total_analyzed"] = cursor.fetchone()[0]
            
            # Count of completed analyses
            cursor.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE analysis_status = 'completed'")
            statistics["completed_analyses"] = cursor.fetchone()[0]
            
            # Count of failed analyses
            cursor.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE analysis_status = 'failed'")
            statistics["failed_analyses"] = cursor.fetchone()[0]
            
            # Average confidence score
            cursor.execute(f"SELECT AVG(confidence_score) FROM {self.TABLE_NAME} WHERE confidence_score IS NOT NULL")
            statistics["avg_confidence_score"] = cursor.fetchone()[0] or 0
            
            # Primary issue categories breakdown
            cursor.execute(f"""
                SELECT primary_issue_category, COUNT(*) as count 
                FROM {self.TABLE_NAME} 
                WHERE primary_issue_category IS NOT NULL 
                GROUP BY primary_issue_category 
                ORDER BY count DESC
            """)
            
            statistics["primary_categories"] = [dict(row) for row in cursor.fetchall()]
            
            # Issue severity breakdown
            cursor.execute(f"""
                SELECT issue_severity, COUNT(*) as count 
                FROM {self.TABLE_NAME} 
                WHERE issue_severity IS NOT NULL 
                GROUP BY issue_severity 
                ORDER BY count DESC
            """)
            
            statistics["severity_breakdown"] = [dict(row) for row in cursor.fetchall()]
            
            # Average processing time
            cursor.execute(f"SELECT AVG(processing_time_ms) FROM {self.TABLE_NAME} WHERE processing_time_ms IS NOT NULL")
            statistics["avg_processing_time_ms"] = cursor.fetchone()[0] or 0
            
            return statistics
            
        except Exception as e:
            logger.error(f"Error retrieving analysis statistics: {str(e)}")
            return {}
        finally:
            if conn:
                conn.close()
    
    def import_from_csv(self, csv_file: str) -> tuple:
        """
        Import analysis results from a CSV file
        
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
                        
                        # Save the analysis result
                        if self.save(row):
                            success_count += 1
                        else:
                            error_count += 1
                            
                    except Exception as e:
                        logger.error(f"Error importing row: {str(e)}")
                        error_count += 1
            
            return (success_count, error_count)
            
        except Exception as e:
            logger.error(f"Error importing analysis results: {str(e)}")
            return (success_count, error_count)
        finally:
            if conn:
                conn.close()
    
    def export_to_csv(self, csv_file: str, completed_only: bool = False) -> bool:
        """
        Export analysis results to a CSV file
        
        Args:
            csv_file: Path to CSV file
            completed_only: Whether to export only completed analyses
            
        Returns:
            Success flag
        """
        try:
            # Get all analysis results
            results = self.get_all(limit=1000000, completed_only=completed_only)
            
            if not results:
                logger.warning("No analysis results to export")
                return False
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(csv_file)), exist_ok=True)
            
            # Write to CSV
            with open(csv_file, 'w', newline='') as f:
                # Determine fields to export
                fields = list(results[0].keys())
                
                # Remove raw_json field if present
                if "raw_json" in fields:
                    fields.remove("raw_json")
                
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                
                for result in results:
                    # Remove raw_json field if present
                    if "raw_json" in result:
                        del result["raw_json"]
                    
                    writer.writerow(result)
            
            logger.info(f"Exported {len(results)} analysis results to {csv_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting analysis results: {str(e)}")
            return False
    
    def delete(self, call_id: str) -> bool:
        """
        Delete an analysis result
        
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
            logger.error(f"Error deleting analysis result {call_id}: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close() 