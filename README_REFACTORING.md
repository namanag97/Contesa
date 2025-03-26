# Call Center Analytics System: Refactoring Guide

## Overview

We are improving the architecture of the Call Center Analytics System to make it more maintainable, scalable, and robust. This guide provides information about the changes being made and how to work with the new codebase structure.

## Directory Structure

The new structure organizes code into logical modules:

```
call-center-analytics/
├── api/                    # API client modules
│   ├── clients/            # External API clients
│   │   ├── openai_client.py
│   │   └── elevenlabs_client.py
├── utils/                  # Utility modules
│   ├── text/               # Text processing utilities
│   ├── file/               # File handling utilities
│   └── error/              # Error handling utilities
├── models/                 # Data models
├── dao/                    # Data Access Objects
│   ├── base_dao.py         # Base DAO class
│   ├── db_connection_pool.py # Database connection management
│   ├── analysis_dao.py     # Analysis results DAO
│   ├── category_dao.py     # Category management DAO
│   ├── stats_dao.py        # Statistics DAO
│   ├── user_dao.py         # User management DAO
│   └── ...                 # Other DAOs
├── exceptions/             # Custom exceptions
│   └── database_exceptions.py # Database-related exceptions
├── config/                 # Configuration
├── tests/                  # Tests
├── call_analysis.py        # Main analysis module
├── transcribe_calls.py     # Transcription module
├── analyze_with_db.py      # Integrated analysis
├── db_export.py            # Database export utility
└── config.py               # Central configuration (to be enhanced)
```

## Changes Made

### 1. API Clients

We've extracted API client code into separate modules:

- **OpenAI Client**: `api/clients/openai_client.py`
  - Handles interaction with OpenAI APIs
  - Provides retry and error handling

- **ElevenLabs Client**: `api/clients/elevenlabs_client.py`
  - Handles interaction with ElevenLabs speech-to-text API
  - Standardizes transcription results

### 2. Utility Modules

We've created standardized utility modules:

- **Text Processor**: `utils/text/text_processor.py`
  - Text chunking for API limits
  - Date extraction from filenames
  - Text cleaning utilities

- **File Handler**: `utils/file/file_handler.py`
  - Safe file operations with backups
  - Directory and file management
  - CSV and JSON handling

- **Error Handler**: `utils/error/error_handler.py`
  - Standardized exception classes
  - Retry mechanism with exponential backoff
  - Graceful exit handling

### 3. Database Access Layer (DAO)

We've implemented a robust Data Access Object (DAO) layer to separate database operations from business logic:

- **BaseDAO**: `dao/base_dao.py`
  - Generic CRUD operations
  - Consistent error handling
  - Transaction support

- **Connection Pool**: `dao/db_connection_pool.py`
  - Efficient database connection management
  - Connection reuse and pooling
  - Configurable timeout and pool size settings

- **Specialized DAOs**:
  - **AnalysisResultDAO**: `dao/analysis_dao.py`
    - Retrieve and save analysis results
    - Import/export capabilities
    - Statistical queries

  - **CategoryDAO**: `dao/category_dao.py`
    - Category management 
    - Valid combination validation
    - Import from CSV

  - **StatsDAO**: `dao/stats_dao.py`
    - Performance statistics
    - Summary statistics
    - Run history tracking

  - **UserDAO**: `dao/user_dao.py`
    - User authentication and management
    - Session tracking
    - User activity logging

### 4. Configuration Management

We've enhanced the configuration system to be more flexible and manageable:

- **Centralized Configuration**: `config_manager.py`
  - Unified configuration from multiple sources
  - Prioritized loading (environment variables → database → defaults)
  - Configuration validation

- **Database Configuration**: `dao/config_dao.py`
  - Persistent configuration storage in database
  - Type-aware storage and retrieval
  - Configuration history tracking

- **Configuration Usage**:
  ```python
  from config_manager import config
  
  # Get configuration values
  db_path = config.get("db_path")
  batch_size = config.get("batch_size", 10)  # With default
  
  # Set configuration values
  config.set("openai_model", "gpt-4o", persist=True)
  
  # Validate configuration
  errors = config.validate()
  if errors:
      logger.error("Configuration errors: {}".format(errors))
  ```

## Using the New Modules

### Import Statements

Update imports to use the new modules:

```python
# Before
from call_analysis import OpenAIClient
from call_analysis import TextProcessor

# After
from api.clients.openai_client import OpenAIClient
from utils.text.text_processor import TextProcessor
```

### Error Handling

Use the new error handling utilities:

```python
from utils.error.error_handler import retry, APIError

@retry(max_attempts=3, exceptions=(APIError,))
def call_api():
    # API call that might fail
    pass
```

### File Operations

Use the file handler for consistent file operations:

```python
from utils.file.file_handler import FileHandler

# Get audio files
files = FileHandler.get_files_by_extension(input_dir, [".aac", ".mp3"])

# Save results safely
FileHandler.safe_write_csv(results_df, output_path)
```

### Database Operations

Use the DAO classes for database operations:

```python
from dao.analysis_dao import AnalysisResultDAO

# Initialize the DAO
analysis_dao = AnalysisResultDAO('path/to/database.db')

# Get analysis results
results = analysis_dao.get_by_criteria({
    "confidence_score_min": 0.8,
    "date_from": "2023-01-01",
    "date_to": "2023-01-31"
})

# Get statistics
stats = analysis_dao.get_statistics()
```

## Refactoring Progress

### Completed:
- ✅ API Client extraction
- ✅ Utility modules
- ✅ Database layer (DAO) implementation
- ✅ Error handling standardization

### In Progress:
- 🔄 Business logic layer implementation
- 🔄 Configuration system enhancement

### Planned:
- ⬜ Testing infrastructure
- ⬜ REST API layer
- ⬜ Frontend improvements

## Working with the Refactored Code

When working with the refactored code:

1. **Check the imports**: Make sure to use the new module paths
2. **Use provided utilities**: Leverage the standardized utilities instead of custom implementations
3. **Follow patterns**: When adding new code, follow the established patterns
4. **Add tests**: Create tests for new functionality in the `/tests` directory
5. **Use DAO layer**: Don't access the database directly, use the appropriate DAO

## Next Steps

The next phases of refactoring will focus on:

1. **Service layer**: Creating service classes to encapsulate business logic
2. **Configuration**: Enhancing the configuration system
3. **Testing**: Adding comprehensive tests
4. **API layer**: Creating a REST API for the system

## Questions & Help

If you have questions about the refactoring or need help adapting your code to the new structure, please contact the architecture team.

---

*This document will be updated as the refactoring progresses.* 