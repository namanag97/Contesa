#!/usr/bin/env python3
"""
Database Export Utility for Call Center Analysis System
Allows exporting data from the database in various formats and configurations.
"""

import sqlite3
import pandas as pd
import os
import json
import csv
import argparse
from datetime import datetime
from contextlib import contextmanager

# Database path - change this to your database path
DB_PATH = "/Users/namanagarwal/voice call/call_analysis.db"

# Export directory
EXPORT_DIR = "/Users/namanagarwal/voice call/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

@contextmanager
def get_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        if conn:
            conn.close()

def export_to_csv(table_name, where_clause=None, output_file=None):
    """Export a table to CSV"""
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(EXPORT_DIR, f"{table_name}_{timestamp}.csv")
    
    # Create query
    query = f"SELECT * FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn)
            df.to_csv(output_file, index=False)
            print(f"Exported {len(df)} records to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        return None

def export_to_json(table_name, where_clause=None, output_file=None):
    """Export a table to JSON"""
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(EXPORT_DIR, f"{table_name}_{timestamp}.json")
    
    # Create query
    query = f"SELECT * FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn)
            df.to_json(output_file, orient='records', indent=2)
            print(f"Exported {len(df)} records to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error exporting to JSON: {e}")
        return None

def export_call_with_analysis(call_id, output_format='json'):
    """Export a specific call with its analysis"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(EXPORT_DIR, f"call_{call_id}_{timestamp}.{output_format}")
    
    query = """
    SELECT t.*, a.*
    FROM transcriptions t
    LEFT JOIN analysis_results a ON t.call_id = a.call_id
    WHERE t.call_id = ?
    """
    
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=[call_id])
            
            if df.empty:
                print(f"No data found for call_id: {call_id}")
                return None
            
            if output_format == 'json':
                df.to_json(output_file, orient='records', indent=2)
            elif output_format == 'csv':
                df.to_csv(output_file, index=False)
            else:
                print(f"Unsupported format: {output_format}")
                return None
                
            print(f"Exported call {call_id} to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error exporting call: {e}")
        return None

def export_date_range(start_date, end_date, output_format='csv'):
    """Export calls and analyses within a date range"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(EXPORT_DIR, f"calls_{start_date}_to_{end_date}_{timestamp}.{output_format}")
    
    query = """
    SELECT t.call_id, t.call_date, t.duration_seconds, 
           a.primary_issue_category, a.specific_issue, a.issue_status, 
           a.caller_type, a.issue_severity, a.system_portal, a.confidence_score
    FROM transcriptions t
    LEFT JOIN analysis_results a ON t.call_id = a.call_id
    WHERE t.call_date BETWEEN ? AND ?
    """
    
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=[start_date, end_date])
            
            if df.empty:
                print(f"No data found for date range: {start_date} to {end_date}")
                return None
            
            if output_format == 'json':
                df.to_json(output_file, orient='records', indent=2)
            elif output_format == 'csv':
                df.to_csv(output_file, index=False)
            else:
                print(f"Unsupported format: {output_format}")
                return None
                
            print(f"Exported {len(df)} calls from {start_date} to {end_date} to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error exporting date range: {e}")
        return None

def export_issue_summary(output_format='csv'):
    """Export a summary of issue categories"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(EXPORT_DIR, f"issue_summary_{timestamp}.{output_format}")
    
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
    
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                print("No issue data found")
                return None
            
            if output_format == 'json':
                df.to_json(output_file, orient='records', indent=2)
            elif output_format == 'csv':
                df.to_csv(output_file, index=False)
            else:
                print(f"Unsupported format: {output_format}")
                return None
                
            print(f"Exported issue summary to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error exporting issue summary: {e}")
        return None

def export_custom_query(query, output_format='csv'):
    """Export results of a custom SQL query"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(EXPORT_DIR, f"custom_query_{timestamp}.{output_format}")
    
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                print("Query returned no results")
                return None
            
            if output_format == 'json':
                df.to_json(output_file, orient='records', indent=2)
            elif output_format == 'csv':
                df.to_csv(output_file, index=False)
            else:
                print(f"Unsupported format: {output_format}")
                return None
                
            print(f"Exported {len(df)} records to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error executing custom query: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Database Export Utility")
    
    # Create subparsers for different export types
    subparsers = parser.add_subparsers(dest="command", help="Export command")
    
    # Table export parser
    table_parser = subparsers.add_parser("table", help="Export a full table")
    table_parser.add_argument("table", choices=["transcriptions", "analysis_results", "categories"], 
                             help="Table to export")
    table_parser.add_argument("--where", help="WHERE clause for filtering")
    table_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    
    # Call export parser
    call_parser = subparsers.add_parser("call", help="Export a specific call with its analysis")
    call_parser.add_argument("call_id", help="Call ID to export")
    call_parser.add_argument("--format", choices=["csv", "json"], default="json", help="Output format")
    
    # Date range parser
    date_parser = subparsers.add_parser("dates", help="Export calls within a date range")
    date_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    date_parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    date_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    
    # Issue summary parser
    summary_parser = subparsers.add_parser("issues", help="Export issue category summary")
    summary_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    
    # Custom query parser
    query_parser = subparsers.add_parser("query", help="Run a custom SQL query")
    query_parser.add_argument("query", help="SQL query to execute")
    query_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    
    args = parser.parse_args()
    
    if args.command == "table":
        if args.format == "csv":
            export_to_csv(args.table, args.where)
        else:
            export_to_json(args.table, args.where)
    
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