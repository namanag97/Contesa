# Call Center Analytics System: Refactoring Plan

## Completed Tasks

We've made significant progress on the first phase of our architecture improvement plan:

1. **Created modular directory structure**:
   - Created `/api`, `/utils`, `/models`, `/dao`, `/config`, and `/tests` directories
   - Added proper `__init__.py` files to make them Python packages

2. **Extracted API clients**:
   - Created `api/clients/openai_client.py` with the OpenAI client implementation
   - Created `api/clients/elevenlabs_client.py` with the ElevenLabs client implementation

3. **Refactored shared utilities**:
   - Created `utils/text/text_processor.py` with text manipulation utilities
   - Created `utils/file/file_handler.py` with file operation utilities
   - Created `utils/error/error_handler.py` with error handling utilities

## Next Steps

### 1. Update Import Statements in Existing Files

We need to update the import statements in the main files to use our newly created modules:

- [ ] Update imports in `call_analysis.py`
- [ ] Update imports in `transcribe_calls.py`
- [ ] Update imports in `analyze_with_db.py`

### 2. Database Layer Improvement

Focus on the database layer improvements next:

- [ ] Create DAO (Data Access Object) classes:
  - [ ] Create `dao/transcription_dao.py`
  - [ ] Create `dao/analysis_dao.py`
  - [ ] Create `dao/category_dao.py`

- [ ] Move database queries from `database_manager.py` to the DAO classes
- [ ] Standardize error handling across all database operations
- [ ] Enhance connection pooling with configurable settings

### 3. Configuration Management

Enhance the configuration system:

- [ ] Centralize all configuration in `config.py`
- [ ] Add environment-specific configurations
- [ ] Implement configuration validation
- [ ] Add schema validation for configuration values

### 4. Testing

Establish basic testing framework:

- [ ] Set up pytest structure in `/tests` directory
- [ ] Create unit tests for utility functions
- [ ] Create mock database for testing DAO classes

## Implementation Strategy

To minimize disruption while refactoring, we'll follow these steps for each module:

1. **Create new module** with the extracted functionality
2. **Update imports** in one file at a time
3. **Test functionality** to ensure no regression
4. **Remove old code** once the new implementation is working properly

## Using the New Modules

Here are examples of how to use the new modules in the existing codebase:

### OpenAI Client

```python
from api.clients.openai_client import OpenAIClient

# Initialize client
openai_client = OpenAIClient(model="gpt-4o", max_retries=3)

# Use client
result = await openai_client.analyze_transcript(messages, call_id)
```

### ElevenLabs Client

```python
from api.clients.elevenlabs_client import ElevenLabsClient

# Initialize client
elevenlabs_client = ElevenLabsClient()

# Transcribe audio
result = elevenlabs_client.transcribe_audio(audio_data, language_code="hin")
```

### Text Processor

```python
from utils.text.text_processor import TextProcessor

# Use text processor utilities
chunks = TextProcessor.chunk_text(transcript, max_length=8000)
date = TextProcessor.extract_date_from_filename(file_name)
```

### File Handler

```python
from utils.file.file_handler import FileHandler

# Use file handler utilities
files = FileHandler.get_files_by_extension(input_dir, [".aac", ".mp3", ".wav"])
FileHandler.safe_write_csv(results_df, output_path)
```

### Error Handler

```python
from utils.error.error_handler import retry, graceful_exit, APIError

# Use retry decorator
@retry(max_attempts=3, delay=1, backoff=2.0, exceptions=(APIError,))
def call_api():
    # API call here
    pass

# Use graceful exit decorator
@graceful_exit(cleanup_func=save_checkpoint)
def main():
    # Main function here
    pass
```

## Timeline

- **Week 3-4**: Complete database layer improvements
- **Week 5-6**: Implement configuration management enhancements
- **Week 7-8**: Set up testing framework and add unit tests

This plan will guide our continued refactoring efforts as we work to improve the architecture of the Call Center Analytics System. 