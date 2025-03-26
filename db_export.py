#!/usr/bin/env python3
"""
Database Export Utility for Call Center Analysis System
Allows exporting data from the database in various formats and configurations.
"""

import pandas as pd
import os
import json
import csv
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Import centralized configuration and database connection
from config import AppConfig
from db_connection_pool import get_db_connection

# Configure logging
logger = logging.getLogger(__name__)

class ExportConfig:
    """Export utility configuration"""
    
    @classmethod
    def get_output_path(cls, prefix: str, format_type: str) -> str:
        """Generate a timestamped output file path"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_dir = AppConfig.get_export_dir()
        return os.path.join(export_dir, f"{prefix}_{timestamp}.{format_type}")

def export_query_results(query: str, output_file: str, params=None, format_type: str = 'csv') -> str:
    """
    Export query results to a file
    
    Args:
        query: SQL query to execute
        output_file: Path to output file
        params: Query parameters (optional)
        format_type: Output format ('csv' or 'json')
        
    Returns:
        Path to the output file if successful, None otherwise
    """
    if params is None:
        params = []
        
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
            
            if df.empty:
                logger.warning("Query returned no results")
                return None
                
            if format_type.lower() == 'csv':
                df.to_csv(output_file, index=False)
            elif format_type.lower() == 'json':
                df.to_json(output_file, orient='records', indent=2)
            else:
                logger.error(f"Unsupported format: {format_type}")
                return None
                
            logger.info(f"Exported {len(df)} records to {output_file}")
            print(f"Exported {len(df)} records to {output_file}")
            return output_file
            
    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}")
        print(f"Error exporting data: {str(e)}")
        return None

def export_to_csv(table_name: str, where_clause=None, output_file=None) -> str:
    """Export a table to CSV"""
    if not output_file:
        output_file = ExportConfig.get_output_path(table_name, "csv")
    
    # Create query
    query = f"SELECT * FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    
    return export_query_results(query, output_file, format_type='csv')

def export_to_json(table_name: str, where_clause=None, output_file=None) -> str:
    """Export a table to JSON"""
    if not output_file:
        output_file = ExportConfig.get_output_path(table_name, "json")
    
    # Create query
    query = f"SELECT * FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    
    return export_query_results(query, output_file, format_type='json')

def export_call_with_analysis(call_id: str, output_format: str = 'json') -> str:
    """
    Export a specific call with its analysis
    
    Args:
        call_id: The call ID to export
        output_format: Output format ('csv' or 'json')
        
    Returns:
        Path to the output file if successful, None otherwise
    """
    output_file = ExportConfig.get_output_path(f"call_{call_id}", output_format)
    
    query = """
    SELECT t.*, a.*
    FROM transcriptions t
    LEFT JOIN analysis_results a ON t.call_id = a.call_id
    WHERE t.call_id = ?
    """
    
    return export_query_results(
        query=query,
        output_file=output_file,
        params=[call_id],
        format_type=output_format
    )

def export_date_range(start_date: str, end_date: str, output_format: str = 'csv') -> str:
    """
    Export calls and analyses within a date range
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_format: Output format ('csv' or 'json')
        
    Returns:
        Path to the output file if successful, None otherwise
    """
    output_file = ExportConfig.get_output_path(f"calls_{start_date}_to_{end_date}", output_format)
    
    query = """
    SELECT t.call_id, t.call_date, t.duration_seconds, 
           a.primary_issue_category, a.specific_issue, a.issue_status, 
           a.caller_type, a.issue_severity, a.system_portal, a.confidence_score
    FROM transcriptions t
    LEFT JOIN analysis_results a ON t.call_id = a.call_id
    WHERE t.call_date BETWEEN ? AND ?
    """
    
    return export_query_results(
        query=query,
        output_file=output_file,
        params=[start_date, end_date],
        format_type=output_format
    )

def export_issue_summary(output_format: str = 'csv') -> str:
    """
    Export a summary of issue categories
    
    Args:
        output_format: Output format ('csv' or 'json')
        
    Returns:
        Path to the output file if successful, None otherwise
    """
    output_file = ExportConfig.get_output_path("issue_summary", output_format)
    
    query = """
    SELECT 
        primary_issue_category,
        COUNT(*) as count,
        AVG(confidence_score) as avg_confidence,
        COUNT(CASE WHEN issue_severity = 'Critical' THEN 1 END) as critical_count,
        COUNT(CASE WHEN issue_severity = 'High' THEN 1 END) as high_count,
        COUNT(CASE WHEN issue_severity = 'Medium' THEN 1 END) as medium_count,
        COUNT(CASE WHEN issue_severity = 'Low' THEN 1 END) as low_count
    FROM 
        analysis_results
    WHERE 
        primary_issue_category IS NOT NULL
    GROUP BY 
        primary_issue_category
    ORDER BY 
        count DESC
    """
    
    return export_query_results(
        query=query,
        output_file=output_file,
        format_type=output_format
    )

def export_custom_query(query: str, output_format: str = 'csv') -> str:
    """
    Export results of a custom SQL query
    
    Args:
        query: SQL query to execute
        output_format: Output format ('csv' or 'json')
        
    Returns:
        Path to the output file if successful, None otherwise
    """
    output_file = ExportConfig.get_output_path("custom_query", output_format)
    
    return export_query_results(
        query=query,
        output_file=output_file,
        format_type=output_format
    )

def main():
    """Command-line interface entry point"""
    parser = argparse.ArgumentParser(description="Database Export Utility")
    
    # Create subparsers for different export types
    subparsers = parser.add_subparsers(dest="command", help="Export command")
    
    # Table export parser
    table_parser = subparsers.add_parser("table", help="Export a full table")
    table_parser.add_argument("table", choices=["transcriptions", "analysis_results", "categories"], 
                             help="Table to export")
    table_parser.add_argument("--where", help="WHERE clause for filtering")
    table_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    table_parser.add_argument("--output", help="Output file path (optional)")
    
    # Call export parser
    call_parser = subparsers.add_parser("call", help="Export a specific call with its analysis")
    call_parser.add_argument("call_id", help="Call ID to export")
    call_parser.add_argument("--format", choices=["csv", "json"], default="json", help="Output format")
    call_parser.add_argument("--output", help="Output file path (optional)")
    
    # Date range parser
    date_parser = subparsers.add_parser("dates", help="Export calls within a date range")
    date_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    date_parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    date_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    date_parser.add_argument("--output", help="Output file path (optional)")
    
    # Issue summary parser
    summary_parser = subparsers.add_parser("issues", help="Export issue category summary")
    summary_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    summary_parser.add_argument("--output", help="Output file path (optional)")
    
    # Custom query parser
    query_parser = subparsers.add_parser("query", help="Run a custom SQL query")
    query_parser.add_argument("query", help="SQL query to execute")
    query_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    query_parser.add_argument("--output", help="Output file path (optional)")
    
    # Database configuration
    parser.add_argument("--db-path", help="Database path (overrides default and environment variable)")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set database path if provided
    if args.db_path:
        os.environ["CALL_ANALYZER_DB_PATH"] = args.db_path
    
    # Execute command
    if args.command == "table":
        if args.format == "csv":
            export_to_csv(args.table, args.where, args.output)
        else:
            export_to_json(args.table, args.where, args.output)
    
    elif args.command == "call":
        export_call_with_analysis(args.call_id, args.format)
    
    elif args.command == "dates":
        export_date_range(args.start_date, args.end_date, args.format)
    
    elif args.command == "issues":
        export_issue_summary(args.format)
    
    elif args.command == "query":
        export_custom_query(args.query, args.format)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()