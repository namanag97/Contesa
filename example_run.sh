#!/bin/bash
# Example script for setting up and running the Call Center Analytics System

# Set environment variables (in a real setup, these would be in your environment or .env file)
export OPENAI_API_KEY="your_openai_api_key" 
export ELEVENLABS_API_KEY="your_elevenlabs_api_key"
export CONTESA_DB_PATH="./contesa.db"
export CONTESA_CLIPS_DIR="./clips"
export CONTESA_LOG_LEVEL="INFO"

# Step 1: Make sure directories exist
echo "Setting up directories..."
mkdir -p clips exports logs

# Step 2: Set up the database
echo "Setting up database..."
python setup_database.py --admin-username admin --admin-password secure_password

# Step 3: Add test audio file if none exists
if [ ! -f "./clips/test_call.aac" ]; then
    echo "No test audio files found, add .aac files to the clips directory"
    echo "In a real setup, you would copy your .aac files to the clips directory"
    echo "Example: cp /path/to/your/audio/files/*.aac ./clips/"
fi

# Step 4: Process clip files
echo "Processing audio clips (if any)..."
python process_clips.py --batch-size 5

# Step 5: Run integration tests
echo "Running integration tests..."
python test_dao_integration.py

# Step 6: Display help info
echo ""
echo "== Call Center Analytics System Quick Help =="
echo "- Add .aac files to the 'clips' directory"
echo "- Run 'python process_clips.py' to process them"
echo "- Access data with the DAO classes in Python"
echo "- Example:"
echo "    from dao.transcription_dao import TranscriptionDAO"
echo "    from dao.analysis_dao import AnalysisResultDAO"
echo "    tDAO = TranscriptionDAO('./contesa.db')"
echo "    aDAO = AnalysisResultDAO('./contesa.db')"
echo "    transcriptions = tDAO.get_all(limit=5)"
echo "    analysis = aDAO.get_statistics()"
echo ""
echo "For more information, refer to the README.md file." 