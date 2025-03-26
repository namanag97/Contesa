#!/usr/bin/env python3
"""
Data Access Layer for Call Center Analysis System
Provides standardized access to database operations and entities.
"""

import os
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import hashlib

# Import the connection pool
from db_connection_pool import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_access.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TranscriptionDAO:
    """Data Access Object for transcription operations"""
    
    def __init__(self, db_path: str):
        """Initialize the DAO with database path"""
        self.db_path = db_path
    
    def get_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all transcriptions"""
        try:
            query = "SELECT * FROM transcriptions ORDER BY import_timestamp DESC"
            if limit:
                query += f" LIMIT {limit}"
                
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting transcriptions: {str(e)}")
            return []
    
    def get_by_id(self, call_id: str) -> Optional[Dict[str, Any]]:
        """Get a transcription by call_id"""
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM transcriptions WHERE call_id = ?", (call_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting transcription {call_id}: {str(e)}")
            return None
    
    def get_for_analysis(self, reanalyze: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get transcriptions that need to be analyzed"""
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                if reanalyze:
                    # Get all transcriptions
                    query = '''
                    SELECT t.call_id, t.file_name, t.call_date, t.duration_seconds, t.transcription
                    FROM transcriptions t
                    WHERE t.transcription IS NOT NULL AND t.transcription != ""
                    ORDER BY t.import_timestamp DESC
                    '''
                    if limit:
                        query += f" LIMIT {limit}"
                    
                    cursor.execute(query)
                else:
                    # Get only transcriptions that haven't been successfully analyzed
                    query = '''
                    SELECT t.call_id, t.file_name, t.call_date, t.duration_seconds, t.transcription
                    FROM transcriptions t
                    LEFT JOIN analysis_results a ON t.call_id = a.call_id
                    WHERE t.transcription IS NOT NULL AND t.transcription != "" 
                    AND (a.call_id IS NULL OR a.analysis_status != 'completed')
                    ORDER BY t.import_timestamp DESC
                    '''
                    if limit:
                        query += f" LIMIT {limit}"
                    
                    cursor.execute(query)
                
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting transcriptions for analysis: {str(e)}")
            return []
    
    def import_from_csv(self, csv_file: str) -> int:
        """Import transcriptions from CSV file"""
        try:
            if not os.path.exists(csv_file):
                logger.warning(f"CSV file not found: {csv_file}")
                return 0
                
            # Read CSV file
            df = pd.read_csv(csv_file)
            imported_count = 0
            
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Process each row
                for _, row in df.iterrows():
                    file_name = row.get('file_name', '')
                    transcription = row.get('transcription', '')
                    
                    # Skip invalid transcriptions
                    if not isinstance(transcription, str) or not transcription.strip() or transcription.startswith("ERROR:"):
                        continue
                    
                    # Extract call date from filename if available
                    call_date = row.get('call_date') or row.get('file_date') or self._extract_date_from_filename(file_name)
                    
                    # Get duration if available
                    duration = row.get('duration_seconds', 0)
                    
                    # Generate a simple hash for change detection
                    hash_value = hashlib.md5(transcription.encode()).hexdigest()
                    
                    # Check if this file already exists with the same hash
                    cursor.execute("SELECT hash_value FROM transcriptions WHERE call_id = ?", (file_name,))
                    result = cursor.fetchone()
                    
                    if result and result['hash_value'] == hash_value:
                        # Same file, same content - skip
                        continue
                    
                    # Insert or update
                    try:
                        cursor.execute('''
                        INSERT INTO transcriptions 
                        (call_id, file_name, call_date, duration_seconds, transcription, hash_value)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(call_id) DO UPDATE SET
                        transcription = excluded.transcription,
                        hash_value = excluded.hash_value,
                        import_timestamp = datetime('now')
                        ''', (file_name, file_name, call_date, duration, transcription, hash_value))
                        imported_count += 1
                    except Exception as e:
                        logger.warning(f"Error importing transcription {file_name}: {str(e)}")
                
                conn.commit()
                logger.info(f"Imported {imported_count} new/updated transcriptions from {csv_file}")
                return imported_count
        except Exception as e:
            logger.error(f"Error importing transcriptions from CSV: {str(e)}")
            return 0
    
    def _extract_date_from_filename(self, filename: str) -> str:
        """
        Extract date from filename using common patterns
        
        Args:
            filename: The filename to extract date from
            
        Returns:
            Extracted date in YYYY-MM-DD format or today's date if no pattern found
        """
        try:
            import re
            
            # Common date patterns to check
            patterns = [
                # YYYY-MM-DD
                r'(\d{4}-\d{2}-\d{2})',
                # DD-MM-YYYY
                r'(\d{2}-\d{2}-\d{4})',
                # MM-DD-YYYY
                r'(\d{2}-\d{2}-\d{4})',
                # YYYYMMDD
                r'(\d{8})',
                # DD-MM-YY
                r'(\d{2}-\d{2}-\d{2})',
                # Filename pattern like "+917204075551 28-02-2025,12-32-09.aac"
                r'(\d{2}-\d{2}-\d{4}),\d{2}-\d{2}-\d{2}'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, filename)
                if match:
                    date_str = match.group(1)
                    
                    # Handle YYYYMMDD format
                    if len(date_str) == 8 and date_str.isdigit():
                        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    
                    # Handle DD-MM-YYYY format
                    if re.match(r'\d{2}-\d{2}-\d{4}', date_str):
                        day, month, year = date_str.split('-')
                        return f"{year}-{month}-{day}"
                    
                    # Handle MM-DD-YYYY format (US date format)
                    if re.match(r'\d{2}-\d{2}-\d{4}', date_str) and int(date_str.split('-')[0]) <= 12:
                        month, day, year = date_str.split('-')
                        return f"{year}-{month}-{day}"
                    
                    # Handle DD-MM-YY format
                    if re.match(r'\d{2}-\d{2}-\d{2}', date_str):
                        day, month, year = date_str.split('-')
                        # Assume 20xx for years
                        full_year = f"20{year}"
                        return f"{full_year}-{month}-{day}"
                    
                    # Already in YYYY-MM-DD format
                    return date_str
            
            # No pattern matched
            logger.debug(f"No date pattern found in filename: {filename}")
            return datetime.now().strftime('%Y-%m-%d')
            
        except Exception as e:
            logger.error(f"Error extracting date from filename {filename}: {str(e)}")
            return datetime.now().strftime('%Y-%m-%d')

class AnalysisResultDAO:
    """Data Access Object for analysis result operations"""
    
    def __init__(self, db_path: str):
        """Initialize the DAO with database path"""
        self.db_path = db_path
    
    def get_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all analysis results"""
        try:
            query = "SELECT * FROM analysis_results ORDER BY analysis_timestamp DESC"
            if limit:
                query += f" LIMIT {limit}"
                
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting analysis results: {str(e)}")
            return []
    
    def get_by_id(self, call_id: str) -> Optional[Dict[str, Any]]:
        """Get an analysis result by call_id"""
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM analysis_results WHERE call_id = ?", (call_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting analysis result {call_id}: {str(e)}")
            return None
    
    def save(self, result: Dict[str, Any]) -> bool:
        """Save an analysis result"""
        try:
            call_id = result.get('call_id')
            if not call_id:
                logger.error("Cannot save analysis result: call_id is missing")
                return False
            
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get table columns
                cursor.execute("PRAGMA table_info(analysis_results)")
                column_names = [row['name'] for row in cursor.fetchall()]
                
                # Filter fields that exist in the table
                fields = []
                placeholders = []
                values = []
                update_fields = []
                
                for field, value in result.items():
                    if field in column_names:
                        fields.append(field)
                        placeholders.append('?')
                        values.append(value)
                        update_fields.append(f"{field} = excluded.{field}")
                
                fields_str = ', '.join(fields)
                placeholders_str = ', '.join(placeholders)
                update_fields_str = ', '.join(update_fields)
                
                # Insert or update
                query = f'''
                INSERT INTO analysis_results 
                ({fields_str})
                VALUES ({placeholders_str})
                ON CONFLICT(call_id) DO UPDATE SET
                {update_fields_str}
                '''
                cursor.execute(query, values)
                conn.commit()
                
                logger.info(f"Saved analysis result for call_id: {call_id}")
                return True
        except Exception as e:
            logger.error(f"Error saving analysis result: {str(e)}")
            return False
    
    def import_from_csv(self, csv_file: str) -> int:
        """Import analysis results from CSV file"""
        try:
            if not os.path.exists(csv_file):
                logger.warning(f"CSV file not found: {csv_file}")
                return 0
                
            # Read CSV file
            df = pd.read_csv(csv_file)
            imported_count = 0
            
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get table columns
                cursor.execute("PRAGMA table_info(analysis_results)")
                column_names = [row['name'] for row in cursor.fetchall()]
                
                # Process each row
                for _, row in df.iterrows():
                    call_id = row.get('call_id', '')
                    if not call_id:
                        continue
                    
                    # Extract fields dynamically from DataFrame
                    field_dict = row.to_dict()
                    
                    # Build query dynamically based on available columns
                    fields = []
                    placeholders = []
                    values = []
                    update_fields = []
                    
                    for field, value in field_dict.items():
                        if field != 'id' and field in column_names:  # Skip the id field
                            fields.append(field)
                            placeholders.append('?')
                            values.append(value)
                            update_fields.append(f"{field} = excluded.{field}")
                    
                    fields_str = ', '.join(fields)
                    placeholders_str = ', '.join(placeholders)
                    update_fields_str = ', '.join(update_fields)
                    
                    # Insert or update
                    try:
                        query = f'''
                        INSERT INTO analysis_results 
                        ({fields_str})
                        VALUES ({placeholders_str})
                        ON CONFLICT(call_id) DO UPDATE SET
                        {update_fields_str}
                        '''
                        cursor.execute(query, values)
                        imported_count += 1
                    except Exception as e:
                        logger.warning(f"Error importing analysis result {call_id}: {str(e)}")
                
                conn.commit()
                logger.info(f"Imported {imported_count} analysis results from {csv_file}")
                return imported_count
        except Exception as e:
            logger.error(f"Error importing analysis results from CSV: {str(e)}")
            return 0
    
    def export_to_csv(self, csv_file: str, where_clause: Optional[str] = None) -> bool:
        """Export analysis results to CSV file"""
        try:
            query = "SELECT * FROM analysis_results"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            with get_db_connection(self.db_path) as conn:
                df = pd.read_sql_query(query, conn)
                df.to_csv(csv_file, index=False)
                
                logger.info(f"Exported {len(df)} analysis results to {csv_file}")
                return True
        except Exception as e:
            logger.error(f"Error exporting analysis results to CSV: {str(e)}")
            return False

class StatsDAO:
    """Data Access Object for statistics operations"""
    
    def __init__(self, db_path: str):
        """Initialize the DAO with database path"""
        self.db_path = db_path
    
    def save_stats(self, stats: Dict[str, Any]) -> bool:
        """Save analysis run statistics"""
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
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
                query = f'''
                INSERT INTO analysis_stats 
                ({fields_str})
                VALUES ({placeholders_str})
                '''
                cursor.execute(query, values)
                conn.commit()
                
                logger.info(f"Saved analysis run statistics")
                return True
                
        except Exception as e:
            logger.error(f"Error saving analysis statistics: {str(e)}")
            return False
            
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics"""
        try:
            stats = {}
            
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total transcriptions
                cursor.execute("SELECT COUNT(*) as count FROM transcriptions")
                stats['total_transcriptions'] = cursor.fetchone()['count']
                
                # Total analyzed calls
                cursor.execute("SELECT COUNT(*) as count FROM analysis_results")
                stats['total_analyzed'] = cursor.fetchone()['count']
                
                # Completed analyses
                cursor.execute("SELECT COUNT(*) as count FROM analysis_results WHERE analysis_status = 'completed'")
                stats['completed_analyses'] = cursor.fetchone()['count']
                
                # Failed analyses
                cursor.execute("SELECT COUNT(*) as count FROM analysis_results WHERE analysis_status = 'failed'")
                stats['failed_analyses'] = cursor.fetchone()['count']
                
                # Average confidence score
                cursor.execute("SELECT AVG(confidence_score) as avg FROM analysis_results")
                stats['avg_confidence'] = cursor.fetchone()['avg']
                
                # Average processing time
                cursor.execute("SELECT AVG(processing_time_ms) as avg FROM analysis_results")
                stats['avg_processing_time'] = cursor.fetchone()['avg']
                
                # Primary issue category breakdown
                cursor.execute("""
                SELECT primary_issue_category, COUNT(*) as count 
                FROM analysis_results 
                WHERE primary_issue_category IS NOT NULL 
                GROUP BY primary_issue_category
                ORDER BY count DESC
                """)
                stats['issue_categories'] = [dict(row) for row in cursor.fetchall()]
                
                # Issue severity breakdown
                cursor.execute("""
                SELECT issue_severity, COUNT(*) as count 
                FROM analysis_results 
                WHERE issue_severity IS NOT NULL 
                GROUP BY issue_severity
                ORDER BY count DESC
                """)
                stats['issue_severity'] = [dict(row) for row in cursor.fetchall()]
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting summary statistics: {str(e)}")
            return {}
            
    def get_recent_runs(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get information about recent analysis runs"""
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(f"""
                SELECT * FROM analysis_stats
                ORDER BY run_date DESC
                LIMIT {limit}
                """)
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting recent runs: {str(e)}")
            return []