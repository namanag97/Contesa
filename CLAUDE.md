# Contesa - Call Center Analytics System

## Build/Test/Lint Commands

```bash
# Run transcription process
python transcribe_calls.py

# Run analysis on transcribed calls
python call_analysis.py --transcriptions call_transcriptions.csv --output analysis_results.csv

# Run database-integrated analysis
python analyze_with_db.py 

# Export data from database
python db_export.py [command] [options]
```

## Code Style Guidelines

### General Formatting
- Use 4 spaces for indentation
- Max line length: 100 characters
- Follow PEP 8 conventions

### Imports
- Group imports in blocks: standard library, third-party, local modules
- Use absolute imports for clarity
- Avoid wildcard imports

### Type Annotations
- Use type hints for function parameters and return values
- Use Optional[] for potentially null values
- Use Dict, List, etc. from typing module

### Error Handling
- Use try/except blocks with specific exception types
- Log exceptions with descriptive messages
- Implement graceful error recovery when possible

### Naming Conventions
- snake_case for functions, methods, variables
- PascalCase for classes
- UPPER_CASE for constants
- Descriptive, intention-revealing names

### Documentation
- Use docstrings for modules, classes and functions
- Follow Google-style docstring format
- Keep docstrings updated when changing code

### Database Operations
- Use context managers for database connections
- Use parameterized queries to prevent SQL injection
- Implement proper transaction management