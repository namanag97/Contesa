#!/usr/bin/env python3
"""
Statistics Data Access Object
Provides database operations for analytics statistics.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from dao.base_dao import BaseDAO
from utils.error.error_handler import DatabaseError, exception_mapper

# Configure logging
logger = logging.getLogger(__name__)

class StatsDAO(BaseDAO):
    """Data Access Object for statistics operations"""
    
    TABLE_NAME = "analysis_stats"
    
    def save_stats(self, stats: Dict[str, Any]) -> bool:
        """
        Save analysis run statistics
        
        Args:
            stats: Dictionary of statistics to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # If run_date not provided, use current timestamp
            if 'run_date' not in stats:
                stats['run_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            # Build query dynamically
            fields = []
            placeholders = []
            values = []
            
            for field, value in stats.items():
                fields.append(field)
                placeholders.append('?')
                values.append(value)
            
            fields_str = ', '.join(fields)
            placeholders_str = ', '.join(placeholders)
            
            # Insert into analysis_stats table
            query = """
            INSERT INTO {} ({})
            VALUES ({})
            """.format(self.TABLE_NAME, fields_str, placeholders_str)
            
            self.execute_update(query, tuple(values))
            logger.info("Saved analysis run statistics")
            return True
                
        except Exception as e:
            logger.error("Error saving analysis statistics: {}".format(str(e)))
            raise DatabaseError("Error saving analysis statistics: {}".format(str(e)))
    
    def get_recent_runs(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get information about recent analysis runs
        
        Args:
            limit: Maximum number of runs to return
            
        Returns:
            List of recent run statistics
        """
        try:
            query = """
            SELECT * FROM {}
            ORDER BY run_date DESC
            LIMIT {}
            """.format(self.TABLE_NAME, limit)
            
            return self.execute_query(query)
                
        except Exception as e:
            logger.error("Error getting recent runs: {}".format(str(e)))
            raise DatabaseError("Error getting recent runs: {}".format(str(e)))
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """
        Get summary statistics across the system
        
        Returns:
            Dictionary of statistics
        """
        try:
            stats = {}
            
            # Total transcriptions
            transcriptions_query = "SELECT COUNT(*) as count FROM transcriptions"
            transcriptions_result = self.execute_query(transcriptions_query)
            stats['total_transcriptions'] = transcriptions_result[0]['count'] if transcriptions_result else 0
            
            # Total analyzed calls
            analyzed_query = "SELECT COUNT(*) as count FROM analysis_results"
            analyzed_result = self.execute_query(analyzed_query)
            stats['total_analyzed'] = analyzed_result[0]['count'] if analyzed_result else 0
            
            # Completed analyses
            completed_query = "SELECT COUNT(*) as count FROM analysis_results WHERE analysis_status = 'completed'"
            completed_result = self.execute_query(completed_query)
            stats['completed_analyses'] = completed_result[0]['count'] if completed_result else 0
            
            # Failed analyses
            failed_query = "SELECT COUNT(*) as count FROM analysis_results WHERE analysis_status = 'failed'"
            failed_result = self.execute_query(failed_query)
            stats['failed_analyses'] = failed_result[0]['count'] if failed_result else 0
            
            # Average confidence score
            confidence_query = "SELECT AVG(confidence_score) as avg FROM analysis_results"
            confidence_result = self.execute_query(confidence_query)
            stats['avg_confidence'] = confidence_result[0]['avg'] if confidence_result else 0
            
            # Average processing time
            time_query = "SELECT AVG(processing_time_ms) as avg FROM analysis_results"
            time_result = self.execute_query(time_query)
            stats['avg_processing_time'] = time_result[0]['avg'] if time_result else 0
            
            # Primary issue category breakdown
            category_query = """
            SELECT primary_issue_category, COUNT(*) as count 
            FROM analysis_results 
            WHERE primary_issue_category IS NOT NULL 
            GROUP BY primary_issue_category
            ORDER BY count DESC
            """
            stats['issue_categories'] = self.execute_query(category_query)
            
            # Issue severity breakdown
            severity_query = """
            SELECT issue_severity, COUNT(*) as count 
            FROM analysis_results 
            WHERE issue_severity IS NOT NULL 
            GROUP BY issue_severity
            ORDER BY count DESC
            """
            stats['issue_severity'] = self.execute_query(severity_query)
            
            # Analysis over time
            time_trend_query = """
            SELECT DATE(analysis_timestamp) as date, COUNT(*) as count
            FROM analysis_results
            GROUP BY DATE(analysis_timestamp)
            ORDER BY date DESC
            LIMIT 30
            """
            stats['analysis_trend'] = self.execute_query(time_trend_query)
            
            # Average stats from all runs
            runs_query = """
            SELECT 
                AVG(total_processed) as avg_processed,
                AVG(avg_confidence) as avg_confidence,
                AVG(avg_processing_time) as avg_processing_time,
                SUM(total_tokens) as total_tokens,
                SUM(total_cost) as total_cost
            FROM {}
            """.format(self.TABLE_NAME)
            runs_result = self.execute_query(runs_query)
            
            if runs_result:
                stats.update(runs_result[0])
            
            return stats
                
        except Exception as e:
            logger.error("Error getting summary statistics: {}".format(str(e)))
            raise DatabaseError("Error getting summary statistics: {}".format(str(e)))
    
    def get_performance_stats(self, days: int = 30) -> Dict[str, Any]:
        """
        Get performance statistics for a specific time period
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary of performance statistics
        """
        try:
            stats = {}
            
            # Processing time trend
            time_query = """
            SELECT 
                DATE(analysis_timestamp) as date, 
                AVG(processing_time_ms) as avg_time,
                COUNT(*) as count
            FROM analysis_results
            WHERE analysis_timestamp >= date('now', '-{} days')
            GROUP BY DATE(analysis_timestamp)
            ORDER BY date
            """.format(days)
            
            stats['processing_time_trend'] = self.execute_query(time_query)
            
            # Confidence score trend
            confidence_query = """
            SELECT 
                DATE(analysis_timestamp) as date, 
                AVG(confidence_score) as avg_confidence,
                COUNT(*) as count
            FROM analysis_results
            WHERE analysis_timestamp >= date('now', '-{} days')
            GROUP BY DATE(analysis_timestamp)
            ORDER BY date
            """.format(days)
            
            stats['confidence_trend'] = self.execute_query(confidence_query)
            
            # Error rate trend
            error_query = """
            SELECT 
                DATE(analysis_timestamp) as date, 
                SUM(CASE WHEN analysis_status = 'failed' THEN 1 ELSE 0 END) as failed_count,
                COUNT(*) as total_count,
                (CAST(SUM(CASE WHEN analysis_status = 'failed' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) as error_rate
            FROM analysis_results
            WHERE analysis_timestamp >= date('now', '-{} days')
            GROUP BY DATE(analysis_timestamp)
            ORDER BY date
            """.format(days)
            
            stats['error_trend'] = self.execute_query(error_query)
            
            return stats
                
        except Exception as e:
            logger.error("Error getting performance statistics: {}".format(str(e)))
            raise DatabaseError("Error getting performance statistics: {}".format(str(e))) 