#!/usr/bin/env python3
"""
Analysis Result Data Access Object
Provides database operations for analysis results.
"""

import os
import logging
from typing import List, Dict, Any, Optional
import pandas as pd

from dao.base_dao import BaseDAO
from utils.error.error_handler import DatabaseError, exception_mapper
from utils.file.file_handler import FileHandler

# Configure logging
logger = logging.getLogger(__name__)

class AnalysisResultDAO(BaseDAO):
    """Data Access Object for analysis result operations"""
    
    TABLE_NAME = "analysis_results"
    ID_FIELD = "call_id"
    
    def get_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all analysis results
        
        Args:
            limit: Optional limit for results
            
        Returns:
            List of analysis result records
        """
        query = "SELECT * FROM {} ORDER BY analysis_timestamp DESC".format(self.TABLE_NAME)
        if limit:
            query += " LIMIT {}".format(limit)
        
        return self.execute_query(query)
    
    def get_by_id(self, call_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an analysis result by call_id
        
        Args:
            call_id: The call ID to retrieve
            
        Returns:
            Analysis result record or None if not found
        """
        return super().get_by_id(self.TABLE_NAME, self.ID_FIELD, call_id)
    
    def save(self, result: Dict[str, Any]) -> bool:
        """
        Save or update an analysis result
        
        Args:
            result: Dictionary containing analysis result data
            
        Returns:
            True if successful, False otherwise
        """
        # Make sure call_id is present
        if 'call_id' not in result:
            logger.error("Cannot save analysis result: call_id is missing")
            return False
            
        return self.insert_or_update(self.TABLE_NAME, result, self.ID_FIELD)
    
    def import_from_csv(self, csv_file: str) -> int:
        """
        Import analysis results from CSV file
        
        Args:
            csv_file: Path to the CSV file
            
        Returns:
            Number of imported/updated records
        """
        try:
            if not os.path.exists(csv_file):
                logger.warning("CSV file not found: {}".format(csv_file))
                return 0
                
            # Read CSV file
            df = pd.read_csv(csv_file)
            imported_count = 0
            
            # Process each row
            for _, row in df.iterrows():
                try:
                    # Ensure call_id is present
                    call_id = row.get('call_id')
                    if not call_id:
                        continue
                    
                    # Convert row to dictionary
                    record = row.to_dict()
                    
                    # Save record
                    if self.save(record):
                        imported_count += 1
                        
                except Exception as e:
                    logger.warning("Error importing analysis result {}: {}".format(
                        row.get('call_id', 'unknown'), str(e)))
            
            logger.info("Imported {} analysis results from {}".format(imported_count, csv_file))
            return imported_count
            
        except Exception as e:
            logger.error("Error importing analysis results from CSV: {}".format(str(e)))
            raise DatabaseError("Error importing analysis results from CSV: {}".format(str(e)))
    
    def export_to_csv(self, csv_file: str, where_clause: Optional[str] = None) -> bool:
        """
        Export analysis results to CSV file
        
        Args:
            csv_file: Path to the output CSV file
            where_clause: Optional WHERE clause for filtering
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = "SELECT * FROM {}".format(self.TABLE_NAME)
            if where_clause:
                query += " WHERE {}".format(where_clause)
            
            # Execute query to get data
            results = self.execute_query(query)
            
            # Convert to DataFrame and write to CSV
            if results:
                df = pd.DataFrame(results)
                return FileHandler.safe_write_csv(df, csv_file)
            else:
                logger.warning("No analysis results to export")
                return False
                
        except Exception as e:
            logger.error("Error exporting analysis results to CSV: {}".format(str(e)))
            raise DatabaseError("Error exporting analysis results to CSV: {}".format(str(e)))
    
    def get_by_criteria(self, criteria: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get analysis results by criteria
        
        Args:
            criteria: Dictionary of field/value pairs to filter by
            limit: Maximum number of results
            
        Returns:
            List of matching analysis result records
        """
        # Build query
        query_parts = ["SELECT * FROM {}".format(self.TABLE_NAME)]
        where_clauses = []
        params = []
        
        # Add criteria
        for field, value in criteria.items():
            where_clauses.append("{} = ?".format(field))
            params.append(value)
        
        # Combine WHERE clauses if any
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
        
        # Add limit and order
        query_parts.append("ORDER BY analysis_timestamp DESC")
        query_parts.append("LIMIT {}".format(limit))
        
        # Build final query
        query = " ".join(query_parts)
        
        return self.execute_query(query, tuple(params))
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about analysis results
        
        Returns:
            Dictionary of statistics
        """
        stats = {}
        
        # Total analyzed calls
        count_query = "SELECT COUNT(*) as count FROM {}".format(self.TABLE_NAME)
        count_results = self.execute_query(count_query)
        stats['total_analyzed'] = count_results[0]['count'] if count_results else 0
        
        # Completed analyses
        completed_query = "SELECT COUNT(*) as count FROM {} WHERE analysis_status = 'completed'".format(self.TABLE_NAME)
        completed_results = self.execute_query(completed_query)
        stats['completed_analyses'] = completed_results[0]['count'] if completed_results else 0
        
        # Failed analyses
        failed_query = "SELECT COUNT(*) as count FROM {} WHERE analysis_status = 'failed'".format(self.TABLE_NAME)
        failed_results = self.execute_query(failed_query)
        stats['failed_analyses'] = failed_results[0]['count'] if failed_results else 0
        
        # Average confidence score
        confidence_query = "SELECT AVG(confidence_score) as avg FROM {}".format(self.TABLE_NAME)
        confidence_results = self.execute_query(confidence_query)
        stats['avg_confidence'] = confidence_results[0]['avg'] if confidence_results else 0
        
        # Primary issue category breakdown
        category_query = """
        SELECT primary_issue_category, COUNT(*) as count 
        FROM {} 
        WHERE primary_issue_category IS NOT NULL 
        GROUP BY primary_issue_category
        ORDER BY count DESC
        """.format(self.TABLE_NAME)
        stats['issue_categories'] = self.execute_query(category_query)
        
        return stats 