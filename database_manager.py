# ------------------------------
# Database Configuration
# ------------------------------
import sqlite3
import os
import logging
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Generator, Tuple
import pandas as pd


try:
    from call_analysis import logger
except ImportError:
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("db_analysis.log"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
class DatabaseManager:
    """Manages database operations for the Call Center Analysis System"""
    
    def __init__(self, db_path: str = "call_analysis.db"):
        """Initialize the database manager with the path to the SQLite database"""
        self.db_path = db_path
        self.initialize_db()
    
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connections"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            # Return dictionary-like rows
            conn.row_factory = sqlite3.Row
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def initialize_db(self):
        """Create database tables if they don't exist"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create transcriptions table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS transcriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT UNIQUE NOT NULL,
                    file_name TEXT NOT NULL,
                    call_date TEXT,
                    duration_seconds INTEGER,
                    transcription TEXT,
                    import_timestamp TEXT DEFAULT (datetime('now')),
                    hash_value TEXT,
                    notes TEXT
                )
                ''')
                
                # Create analysis_results table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS analysis_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT UNIQUE NOT NULL,
                    call_date TEXT,
                    analysis_status TEXT NOT NULL,
                    api_error TEXT,
                    primary_issue_category TEXT,
                    specific_issue TEXT,
                    issue_status TEXT,
                    issue_severity TEXT,
                    caller_type TEXT,
                    experience_level TEXT,
                    caller_intent TEXT,
                    system_portal TEXT,
                    device_information TEXT,
                    error_messages TEXT,
                    feature_involved TEXT,
                    issue_preconditions TEXT,
                    action_sequence TEXT,
                    failure_point TEXT,
                    expected_vs_actual TEXT,
                    issue_frequency TEXT,
                    attempted_solutions TEXT,
                    resolution_steps TEXT,
                    knowledge_gap_identified TEXT,
                    issue_description_quote TEXT,
                    impact_statement_quote TEXT,
                    issue_summary TEXT,
                    confidence_score REAL,
                    analysis_timestamp TEXT DEFAULT (datetime('now')),
                    processing_time_ms REAL,
                    model TEXT,
                    note TEXT,
                    FOREIGN KEY (call_id) REFERENCES transcriptions(call_id)
                )
                ''')
                
                # Create categories table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    category_name TEXT NOT NULL,
                    parent_id INTEGER,
                    description TEXT,
                    UNIQUE(level, category_name)
                )
                ''')
                
                # Create valid_combinations table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS valid_combinations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    l1_category TEXT NOT NULL,
                    l2_category TEXT NOT NULL,
                    l3_category TEXT NOT NULL,
                    UNIQUE(l1_category, l2_category, l3_category)
                )
                ''')
                
                # Create stats table for tracking analysis metrics
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS analysis_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_date TEXT DEFAULT (datetime('now')),
                    total_processed INTEGER,
                    successful INTEGER,
                    failed INTEGER,
                    avg_confidence REAL,
                    avg_processing_time REAL,
                    model TEXT,
                    batch_size INTEGER,
                    total_tokens INTEGER,
                    total_cost REAL
                )
                ''')
                
                # Create indexes for faster queries
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_transcriptions_call_id ON transcriptions(call_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_transcriptions_call_date ON transcriptions(call_date)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_analysis_call_id ON analysis_results(call_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_analysis_category ON analysis_results(primary_issue_category)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_analysis_confidence ON analysis_results(confidence_score)')
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {str(e)}")
            raise
    
    def import_transcriptions_from_csv(self, csv_file: str) -> int:
        """Import transcriptions from CSV file into the database"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            imported_count = 0
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Process each row
                for _, row in df.iterrows():
                    file_name = row.get('file_name', '')
                    transcription = row.get('transcription', '')
                    
                    # Skip invalid transcriptions
                    if not isinstance(transcription, str) or not transcription.strip() or transcription.startswith("ERROR:"):
                        continue
                    
                    # Extract call date from filename
                    call_date = TextProcessor.extract_date_from_filename(file_name)
                    
                    # Get duration if available
                    duration = row.get('duration_seconds', 0)
                    
                    # Generate a simple hash for change detection
                    import hashlib
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
                    except sqlite3.Error as e:
                        logger.warning(f"Error importing transcription {file_name}: {str(e)}")
                
                conn.commit()
                logger.info(f"Imported {imported_count} new/updated transcriptions from {csv_file}")
                return imported_count
                
        except Exception as e:
            logger.error(f"Error importing transcriptions from CSV: {str(e)}")
            raise
    
    def import_categories_from_csv(self, csv_file: str) -> Tuple[int, int]:
        """Import categories and valid combinations from CSV file"""
        if not os.path.exists(csv_file):
            logger.warning(f"Categories file {csv_file} not found")
            return (0, 0)
        
        try:
            # Use CategoryManager to load categories from CSV
            category_manager = CategoryManager(csv_file)
            categories = category_manager.load_categories()
            
            categories_count = 0
            combinations_count = 0
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Clear existing categories and combinations
                cursor.execute("DELETE FROM categories")
                cursor.execute("DELETE FROM valid_combinations")
                
                # Insert L1 categories
                for category in categories.options.get('L1', []):
                    cursor.execute('''
                    INSERT INTO categories (level, category_name, description)
                    VALUES (?, ?, ?)
                    ''', ('L1', category, 'Imported from CSV'))
                    categories_count += 1
                
                # Insert L2 categories
                for category in categories.options.get('L2', []):
                    cursor.execute('''
                    INSERT INTO categories (level, category_name, description)
                    VALUES (?, ?, ?)
                    ''', ('L2', category, 'Imported from CSV'))
                    categories_count += 1
                
                # Insert L3 categories
                for category in categories.options.get('L3', []):
                    cursor.execute('''
                    INSERT INTO categories (level, category_name, description)
                    VALUES (?, ?, ?)
                    ''', ('L3', category, 'Imported from CSV'))
                    categories_count += 1
                
                # Insert valid combinations
                for combo in categories.valid_combinations:
                    l1 = combo.get('L1', '')
                    l2 = combo.get('L2', '')
                    l3 = combo.get('L3', '')
                    
                    if l1 and l2 and l3:
                        cursor.execute('''
                        INSERT INTO valid_combinations (l1_category, l2_category, l3_category)
                        VALUES (?, ?, ?)
                        ''', (l1, l2, l3))
                        combinations_count += 1
                
                conn.commit()
                logger.info(f"Imported {categories_count} categories and {combinations_count} valid combinations")
                return (categories_count, combinations_count)
                
        except Exception as e:
            logger.error(f"Error importing categories from CSV: {str(e)}")
            raise
    
    def import_analysis_results_from_csv(self, csv_file: str) -> int:
        """Import analysis results from CSV file into the database"""
        if not os.path.exists(csv_file):
            logger.warning(f"Analysis results file {csv_file} not found")
            return 0
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            imported_count = 0
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
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
                        if field != 'id':  # Skip the id field
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
                    except sqlite3.Error as e:
                        logger.warning(f"Error importing analysis result {call_id}: {str(e)}")
                
                conn.commit()
                logger.info(f"Imported {imported_count} analysis results from {csv_file}")
                return imported_count
                
        except Exception as e:
            logger.error(f"Error importing analysis results from CSV: {str(e)}")
            raise
    
    def get_transcriptions_for_analysis(self, limit: int = None, reanalyze: bool = False) -> List[Dict[str, Any]]:
        """Get transcriptions that need to be analyzed"""
        try:
            with self.get_connection() as conn:
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
                
                # Convert rows to dictionaries
                results = []
                for row in cursor.fetchall():
                    results.append(dict(row))
                
                logger.info(f"Found {len(results)} transcriptions for analysis")
                return results
                
        except sqlite3.Error as e:
            logger.error(f"Error retrieving transcriptions for analysis: {str(e)}")
            return []
    
    def save_analysis_result(self, result: Dict[str, Any]) -> bool:
        """Save a single analysis result to the database"""
        try:
            call_id = result.get('call_id')
            if not call_id:
                logger.error("Cannot save analysis result: call_id is missing")
                return False
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Remove any fields that don't exist in the database
                fields = []
                placeholders = []
                values = []
                update_fields = []
                
                # Get table columns
                cursor.execute("PRAGMA table_info(analysis_results)")
                column_names = [row['name'] for row in cursor.fetchall()]
                
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
    
    def get_analysis_results(self, criteria: Dict[str, Any] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve analysis results with optional filtering criteria"""
        try:
            query = "SELECT * FROM analysis_results WHERE 1=1"
            params = []
            
            # Add filtering criteria if provided
            if criteria:
                for field, value in criteria.items():
                    query += f" AND {field} = ?"
                    params.append(value)
            
            # Add limit
            query += f" ORDER BY analysis_timestamp DESC LIMIT {limit}"
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                results = []
                for row in cursor.fetchall():
                    results.append(dict(row))
                
                return results
                
        except Exception as e:
            logger.error(f"Error retrieving analysis results: {str(e)}")
            return []
    
    def save_stats(self, stats: Dict[str, Any]) -> bool:
        """Save analysis run statistics"""
        try:
            with self.get_connection() as conn:
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
    
    def export_to_csv(self, table_name: str, output_file: str, where_clause: str = None) -> bool:
        """Export a database table to CSV file"""
        try:
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            with self.get_connection() as conn:
                df = pd.read_sql_query(query, conn)
                df.to_csv(output_file, index=False)
                
                logger.info(f"Exported {len(df)} records from {table_name} to {output_file}")
                return True
                
        except Exception as e:
            logger.error(f"Error exporting {table_name} to CSV: {str(e)}")
            return False
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get summary statistics from the database"""
        try:
            stats = {}
            
            with self.get_connection() as conn:
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