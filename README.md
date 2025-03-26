# Call Center Analytics System

A comprehensive solution for analyzing call center recordings in financial services.

## Overview

The Call Center Analytics System is designed to automatically transcribe, analyze, and categorize customer service call recordings. It helps financial institutions identify recurring issues, track customer sentiment, and improve service quality through data-driven insights.

## Features

- **Audio Transcription**: Convert .aac call recordings to text using ElevenLabs API
- **AI-Powered Analysis**: Analyze transcripts to identify customer issues, categorize problems, and extract key insights
- **Comprehensive Database**: Store and manage transcriptions, analysis results, and statistics
- **Issue Categorization**: Hierarchical categorization of customer issues with validation
- **Rich Reporting**: Generate statistics and trends on call patterns and issue occurrence
- **Security**: Role-based access control and audit logging

## System Architecture

The system consists of several modular components:

- **Database Layer**: DAO classes for database interactions
- **API Clients**: Interfaces to ElevenLabs and OpenAI APIs
- **Processors**: Core processing logic for transcription and analysis
- **Configuration System**: Flexible configuration from files, environment, and database
- **Command-line Tools**: Scripts for processing calls and managing the system

## Getting Started

### Prerequisites

- Python 3.8+
- SQLite (for data storage)
- OpenAI API key
- ElevenLabs API key

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/contesa.git
   cd contesa
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```
   export OPENAI_API_KEY=your_openai_key
   export ELEVENLABS_API_KEY=your_elevenlabs_key
   export CONTESA_DB_PATH=path/to/database.db
   export CONTESA_CLIPS_DIR=path/to/audio/clips
   ```

4. Set up the database:
   ```
   python setup_database.py
   ```

### Usage

#### Processing Call Recordings

To process audio files from a directory:

```
python process_clips.py --clips-dir /path/to/clips --batch-size 5
```

This will:
1. Find all .aac files in the specified directory
2. Transcribe each file using ElevenLabs
3. Analyze the transcription with OpenAI
4. Store results in the database

#### Running Integration Tests

To validate the system components work together:

```
python test_dao_integration.py
```

This will run a series of tests to validate the DAOs and their integration with the business logic.

## Database Structure

The system uses SQLite with the following key tables:

- `call_transcriptions`: Stores transcribed call data
- `analysis_results`: Stores AI analysis of calls
- `categories`: Stores hierarchical issue categories
- `valid_combinations`: Defines valid category combinations
- `analysis_stats`: Tracks batch processing statistics
- `users`: Manages system users
- `sessions`: Tracks user sessions
- `config`: Stores system configuration

## Configuration

The system is configurable through multiple methods (in order of precedence):

1. Environment variables
2. Database configuration
3. Config file
4. Default values

Key configuration options:

- `db_path`: Path to SQLite database
- `clips_dir`: Directory containing audio clips
- `batch_size`: Number of clips to process in a batch
- `openai_model`: Model to use for analysis
- `max_retries`: Maximum retry attempts for API calls

## Extending the System

### Adding New Categories

1. Update the `categories` table with new entries
2. Add valid combinations to the `valid_combinations` table

### Adding New Analysis Features

1. Modify the prompt in `process_clips.py` to extract additional information
2. Update the `analysis_results` table schema to store new fields

## Troubleshooting

- **API Connection Issues**: Check API keys and network connectivity
- **Database Errors**: Verify database path and permissions
- **Processing Errors**: Check log files for detailed error messages

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- OpenAI for their powerful GPT models
- ElevenLabs for their speech-to-text capabilities