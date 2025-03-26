#!/usr/bin/env python3
"""
Data Access Object (DAO) for analytics statistics.
Provides database operations for storing and retrieving analytics statistics.
"""

import sqlite3
import logging
import json
from typing import List, Dict, Any, Optional
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class StatsDAO:
    """DAO for analysis_stats table"""
    
    TABLE_NAME = "analysis_stats"
    
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
    
    def save_stats(self, stats_data: Dict[str, Any]) -> bool:
        """
        Save statistics to the database
        
        Args:
            stats_data: Statistics data
            
        Returns:
            Success flag
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Ensure run_date is present
            if "run_date" not in stats_data:
                stats_data["run_date"] = datetime.now().isoformat()
            
            # Build query dynamically based on the provided fields
            fields = list(stats_data.keys())
            placeholders = ", ".join(["?"] * len(fields))
            
            query = f"INSERT INTO {self.TABLE_NAME} ({', '.join(fields)}) VALUES ({placeholders})"
            
            values = [stats_data[field] for field in fields]
            
            cursor.execute(query, values)
            conn.commit()
            
            logger.info(f"Saved statistics for run on {stats_data.get('run_date')}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving statistics: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def get_recent_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent analysis runs
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of recent run statistics
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = f"SELECT * FROM {self.TABLE_NAME} ORDER BY run_date DESC LIMIT ?"
            
            cursor.execute(query, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error retrieving recent runs: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """
        Get summary statistics
        
        Returns:
            Dictionary of summary statistics
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            summary = {}
            
            # Last run info
            cursor.execute(f"SELECT * FROM {self.TABLE_NAME} ORDER BY run_date DESC LIMIT 1")
            last_run = cursor.fetchone()
            if last_run:
                summary["last_run"] = dict(last_run)
            
            # Total runs
            cursor.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME}")
            summary["total_runs"] = cursor.fetchone()[0]
            
            # Total processed
            cursor.execute(f"SELECT SUM(total_processed) FROM {self.TABLE_NAME}")
            summary["total_processed"] = cursor.fetchone()[0] or 0
            
            # Total successful
            cursor.execute(f"SELECT SUM(successful) FROM {self.TABLE_NAME}")
            summary["total_successful"] = cursor.fetchone()[0] or 0
            
            # Total failed
            cursor.execute(f"SELECT SUM(failed) FROM {self.TABLE_NAME}")
            summary["total_failed"] = cursor.fetchone()[0] or 0
            
            # Average processing time
            cursor.execute(f"SELECT AVG(avg_processing_time) FROM {self.TABLE_NAME} WHERE avg_processing_time IS NOT NULL")
            summary["avg_processing_time"] = cursor.fetchone()[0] or 0
            
            # Average success rate
            if summary["total_processed"] > 0:
                summary["success_rate"] = (summary["total_successful"] / summary["total_processed"]) * 100
            else:
                summary["success_rate"] = 0
            
            # Total run time
            cursor.execute(f"SELECT SUM(run_duration_seconds) FROM {self.TABLE_NAME} WHERE run_duration_seconds IS NOT NULL")
            summary["total_run_time"] = cursor.fetchone()[0] or 0
            
            return summary
            
        except Exception as e:
            logger.error(f"Error retrieving summary statistics: {str(e)}")
            return {}
        finally:
            if conn:
                conn.close()
    
    def get_performance_stats(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """
        Get performance statistics over time
        
        Args:
            start_date: Start date in ISO format
            end_date: End date in ISO format
            
        Returns:
            Dictionary of performance statistics
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            performance = {}
            
            # Build query with optional date range
            where_clause = ""
            params = []
            
            if start_date:
                where_clause = " WHERE run_date >= ?"
                params.append(start_date)
                
                if end_date:
                    where_clause += " AND run_date <= ?"
                    params.append(end_date)
            elif end_date:
                where_clause = " WHERE run_date <= ?"
                params.append(end_date)
            
            # Get all runs in date range
            query = f"SELECT * FROM {self.TABLE_NAME}{where_clause} ORDER BY run_date"
            
            cursor.execute(query, params)
            runs = [dict(row) for row in cursor.fetchall()]
            
            if not runs:
                return {"message": "No data available for the specified date range"}
            
            # Calculate trends
            performance["runs"] = runs
            performance["total_runs"] = len(runs)
            
            # Processing time trend
            performance["processing_time_trend"] = [
                {"date": run["run_date"], "avg_processing_time": run["avg_processing_time"]}
                for run in runs if "avg_processing_time" in run and run["avg_processing_time"] is not None
            ]
            
            # Success rate trend
            performance["success_rate_trend"] = [
                {
                    "date": run["run_date"], 
                    "success_rate": (run["successful"] / run["total_processed"]) * 100 if run["total_processed"] > 0 else 0
                }
                for run in runs if "successful" in run and "total_processed" in run
            ]
            
            # Duration trend
            performance["duration_trend"] = [
                {"date": run["run_date"], "duration": run["run_duration_seconds"]}
                for run in runs if "run_duration_seconds" in run and run["run_duration_seconds"] is not None
            ]
            
            return performance
            
        except Exception as e:
            logger.error(f"Error retrieving performance statistics: {str(e)}")
            return {"error": str(e)}
        finally:
            if conn:
                conn.close() 