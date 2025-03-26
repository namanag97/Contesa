# Project Structure

This document provides an overview of the Call Center Analytics System's file organization and component relationships.

## Directory Structure

```
call-center-analytics/
├── README.md                      # Project overview and setup instructions
├── IMPROVEMENTS.md                # Improvement checklist
├── requirements.txt               # Python dependencies
├── .env                           # Environment variables (not in version control)
│
├── transcribe_calls.py            # Audio transcription module
├── call_analysis.py               # Transcript analysis module
├── database_manager.py            # Database operations module
├── analyze_with_db.py             # Database-integrated analysis
├── db_export.py                   # Database export utility
│
├── logs/                          # Log files directory
│   ├── transcription_*.log        # Transcription session logs
│   └── call_analysis.log          # Analysis logs
│
├── exports/                       # Data exports directory
│   ├── transcriptions_*.csv       # Exported transcription data
│   └── analysis_*.csv             # Exported analysis data
│
├── backups/                       # Backup files directory
│   ├── transcriptions_*.csv       # Transcription data backups
│   └── analysis_*.csv             # Analysis data backups
│
└── data/                          # Data files
    ├── call_transcriptions.csv    # Transcription results
    ├── analysis_results.csv       # Analysis results
    ├── categories.csv             # Category definitions
    └── call_analysis.db           # SQLite database
```

## Component Relationships

```
                    ┌───────────────────┐
                    │    Audio Files    │
                    └─────────┬─────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────┐
│             transcribe_calls.py                 │
│  - Reads audio files                            │
│  - Transcribes using ElevenLabs API             │
│  - Saves results to CSV                         │
└──────────────────────┬──────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────┐
│             call_analysis.py                    │
│  - Reads transcriptions                         │
│  - Analyzes using OpenAI API                    │
│  - Extracts structured information              │
│  - Saves results to CSV                         │
└────────┬──────────────────────────┬─────────────┘
         │                          │
         ▼                          ▼
┌─────────────────────┐  ┌───────────────────────────┐
│  analyze_with_db.py │  │    database_manager.py    │
│  - DB integration   │◄─┤  - Manages SQLite DB      │
│  - Full workflow    │  │  - Handles data operations │
└──────────┬──────────┘  └───────────────┬───────────┘
           │                             │
           ▼                             ▼
┌─────────────────────┐  ┌───────────────────────────┐
│    db_export.py     │  │    call_analysis.db       │
│  - Exports data     │◄─┤  - Stores all data        │
│  - Various formats  │  │  - Structured storage     │
└─────────────────────┘  └───────────────────────────┘
```

## Module Descriptions

### transcribe_calls.py
The transcription module is responsible for converting audio files to text. It handles batch processing of audio files, interacts with the ElevenLabs API, and maintains detailed logs of the transcription process.

### call_analysis.py
The analysis module processes transcribed text to extract meaningful information. It uses OpenAI's models to analyze call transcripts, identify issues, categorize them, and extract structured data about the call. It implements batch processing, error handling, and comprehensive result formatting.

### database_manager.py
This module manages all database operations including schema initialization, data import/export, and query functionality. It provides an abstraction layer over SQLite operations and ensures data integrity throughout the system.

### analyze_with_db.py
This integration module combines the analysis capabilities with database operations. It provides a streamlined workflow that reads from and writes to the database directly, rather than using intermediate CSV files.

### db_export.py
The export utility provides flexible data export options from the database. It supports various formats (CSV, JSON), filtering criteria, and customized exports for reporting purposes.

## Data Flow

1. **Audio Files → Transcription**
   - Audio files are processed in batches
   - ElevenLabs API converts speech to text
   - Results are saved to CSV with metadata

2. **Transcription → Analysis**
   - Transcripts are read from CSV or database
   - OpenAI models extract structured information
   - Results are saved with confidence scores and metadata

3. **Analysis → Database**
   - Analysis results are stored in SQLite
   - Relationships between data are maintained
   - Statistics and metadata are tracked

4. **Database → Exports**
   - Query results are exported in various formats
   - Filtering and customization options are available
   - Reports can be generated for different needs

## Development Workflow

For enhancing the system, follow this workflow:

1. **New Feature Development**
   - Add the feature to the appropriate module
   - Update database schema if necessary
   - Add tests for the new functionality
   - Update documentation

2. **Data Model Changes**
   - Modify the database_manager.py schema initialization
   - Add migration code for existing databases
   - Update related data access methods
   - Test with sample data

3. **API Integration Updates**
   - For ElevenLabs: Update the setup_environment function
   - For OpenAI: Update the OpenAIClient class
   - Test with sample API responses
   - Update error handling for new scenarios

4. **Testing**
   - Test new functionality with sample data
   - Verify database integrity after changes
   - Check for regression issues
   - Validate export functionality

The modular design of the system allows for component-level updates without affecting the entire system.