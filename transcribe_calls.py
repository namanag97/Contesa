import os
import csv
import glob
import pandas as pd
import signal
import sys
from dotenv import load_dotenv
from elevenlabs.client import ElevenLabs
from datetime import datetime, timedelta
from pydub import AudioSegment
from tqdm import tqdm
import logging
import threading
import time

# Configure logging with rotating file handler
import logging.handlers
from pathlib import Path

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Configure session-based logging
log_filename = f"transcription_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = logs_dir / log_filename

# Set up rotating file handler to prevent log files from growing too large
file_handler = logging.handlers.RotatingFileHandler(
    log_path,
    maxBytes=10_000_000,  # 10MB
    backupCount=10
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        file_handler,
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global variables for tracking progress
session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
session_log_file = f"transcription_session_{session_id}.log"
total_files = 0
current_file_index = 0
current_batch = 0
total_batches = 0
start_time = None
is_exiting = False
progress_thread = None

def setup_environment():
    """Set up environment variables and initialize the ElevenLabs client."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get API key
        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            raise ValueError("ELEVENLABS_API_KEY environment variable not found")
        
        # Initialize ElevenLabs client
        client = ElevenLabs(api_key=api_key)
        return client
    except Exception as e:
        logger.error(f"Failed to set up environment: {str(e)}")
        return None

def get_audio_files(folder_path, supported_extensions=None):
    """Get all audio files with supported extensions from the folder."""
    if supported_extensions is None:
        supported_extensions = [".aac", ".mp3", ".wav", ".m4a"]
    
    try:
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder {folder_path} not found")
        
        files = []
        for ext in supported_extensions:
            files.extend(glob.glob(os.path.join(folder_path, f"*{ext}")))
        
        logger.info(f"Found {len(files)} audio files in {folder_path}")
        return files
    except Exception as e:
        logger.error(f"Error finding audio files: {str(e)}")
        return []

def get_already_transcribed_files(csv_path):
    """Get a list of file names that have already been transcribed."""
    try:
        if not os.path.exists(csv_path):
            logger.info(f"No existing transcriptions file found at {csv_path}")
            return []
        
        df = pd.read_csv(csv_path)
        already_transcribed = df['file_name'].tolist()
        logger.info(f"Found {len(already_transcribed)} already transcribed files")
        return already_transcribed
    except Exception as e:
        logger.error(f"Error reading existing CSV: {str(e)}")
        return []

def transcribe_audio(client, file_path, language_code="hin"):
    """Transcribe the audio file using ElevenLabs API."""
    global current_file_index
    
    try:
        file_name = os.path.basename(file_path)
        logger.info(f"Transcribing: {file_name} ({current_file_index + 1}/{total_files})")
        
        # Get file metadata
        file_date = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d')
        
        # Load the audio file
        audio = AudioSegment.from_file(file_path)
        duration = len(audio) / 1000  # Duration in seconds
        
        # Convert to WAV in memory
        audio_data = audio.export(format="wav")
        
        # Transcribe the audio
        transcription = client.speech_to_text.convert(
            file=audio_data,
            model_id="scribe_v1",
            tag_audio_events=True,
            language_code=language_code,
            diarize=True,
        )
        
        # Create a structured result
        result = {
            "file_name": file_name,
            "file_date": file_date,
            "duration_seconds": duration,
            "transcription": transcription.text,
            "speakers": len(set(segment.speaker for segment in transcription.segments)) if hasattr(transcription, 'segments') else 1
        }
        
        logger.info(f"Successfully transcribed {file_name} ({duration:.1f} sec)")
        current_file_index += 1
        return result
    
    except Exception as e:
        logger.error(f"Error transcribing {file_path}: {str(e)}")
        current_file_index += 1
        return {
            "file_name": os.path.basename(file_path),
            "file_date": datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d'),
            "duration_seconds": 0,
            "transcription": f"ERROR: {str(e)}",
            "speakers": 0
        }

def save_transcriptions(results, csv_path):
    """Save transcription results to CSV file with version control and protection against data loss."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        # Create backup of existing file before making any changes
        if os.path.exists(csv_path):
            backup_dir = os.path.join(os.path.dirname(csv_path), "backups")
            os.makedirs(backup_dir, exist_ok=True)
            
            backup_file = os.path.join(
                backup_dir, 
                f"{os.path.basename(csv_path).split('.')[0]}_{timestamp}.csv"
            )
            
            try:
                # Create a backup copy of the original file
                pd.read_csv(csv_path).to_csv(backup_file, index=False)
                logger.info(f"Created backup of existing data at {backup_file}")
            except Exception as backup_error:
                logger.error(f"Failed to create backup of existing file: {str(backup_error)}")
                # Don't proceed if we can't backup existing data
                return False
            
            # Load existing data
            try:
                existing_df = pd.read_csv(csv_path)
                new_df = pd.DataFrame(results)
                
                # Check for duplicates before combining
                duplicate_files = set(existing_df['file_name']).intersection(set(new_df['file_name']))
                if duplicate_files:
                    logger.warning(f"Found {len(duplicate_files)} duplicate files. Using new transcriptions for these files.")
                    # Remove duplicates from existing data
                    existing_df = existing_df[~existing_df['file_name'].isin(duplicate_files)]
                
                # Combine with existing data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            except Exception as e:
                logger.error(f"Error processing existing data: {str(e)}")
                return False
        else:
            combined_df = pd.DataFrame(results)
        
        # Write to a temporary file first to prevent corruption
        temp_file = f"{csv_path}.temp"
        combined_df.to_csv(temp_file, index=False)
        
        # If temporary write succeeds, rename to final file
        if os.path.exists(temp_file):
            if os.path.exists(csv_path):
                os.replace(temp_file, csv_path)  # Atomic operation
            else:
                os.rename(temp_file, csv_path)
            
            logger.info(f"Successfully saved {len(results)} new transcriptions. Total records: {len(combined_df)}")
            return True
        else:
            logger.error("Failed to write temporary file")
            return False
            
    except Exception as e:
        logger.error(f"Error saving transcriptions to CSV: {str(e)}")
        
        # Emergency backup - save new transcriptions to a separate file
        try:
            error_backup_path = f"new_transcriptions_{timestamp}.csv"
            pd.DataFrame(results).to_csv(error_backup_path, index=False)
            logger.info(f"Created emergency backup of new transcriptions at {error_backup_path}")
        except Exception as backup_error:
            logger.critical(f"Failed to create emergency backup: {str(backup_error)}")
        
        return False

def estimate_completion_time(processed, total, elapsed_time):
    """Estimate the remaining time to complete all transcriptions."""
    if processed == 0 or elapsed_time == 0:
        return "Calculating..."
    
    rate = processed / elapsed_time
    remaining_files = total - processed
    estimated_seconds = remaining_files / rate if rate > 0 else float('inf')
    
    if estimated_seconds == float('inf'):
        return "Unknown"
    
    estimated_completion = datetime.now() + timedelta(seconds=estimated_seconds)
    
    if estimated_seconds < 60:
        return f"About {int(estimated_seconds)} seconds (ETA: {estimated_completion.strftime('%H:%M:%S')})"
    elif estimated_seconds < 3600:
        return f"About {int(estimated_seconds / 60)} minutes (ETA: {estimated_completion.strftime('%H:%M:%S')})"
    else:
        hours = estimated_seconds / 3600
        return f"About {hours:.1f} hours (ETA: {estimated_completion.strftime('%Y-%m-%d %H:%M:%S')})"

def print_progress():
    """Function to periodically print progress information."""
    global is_exiting, start_time, current_file_index, total_files, current_batch, total_batches
    
    while not is_exiting:
        if start_time is not None and current_file_index > 0:
            elapsed_time = (datetime.now() - start_time).total_seconds()
            estimated_remaining = estimate_completion_time(current_file_index, total_files, elapsed_time)
            
            progress_msg = (
                f"\nProgress update: {current_file_index}/{total_files} files processed "
                f"({current_file_index/total_files*100:.1f}%)\n"
                f"Batch: {current_batch}/{total_batches}\n"
                f"Elapsed time: {timedelta(seconds=int(elapsed_time))}\n"
                f"Estimated remaining: {estimated_remaining}\n"
            )
            
            print(progress_msg)
            with open(session_log_file, 'a') as f:
                f.write(f"\n--- PROGRESS UPDATE {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")
                f.write(progress_msg)
                
        time.sleep(30)  # Update every 30 seconds

def signal_handler(sig, frame):
    """Handle keyboard interrupts and termination signals with a graceful exit."""
    global is_exiting
    
    if is_exiting:
        print("\nForced exit! Some data may be lost.")
        sys.exit(1)
    
    is_exiting = True
    print("\n\nReceived termination signal. Finishing current transcription and saving progress...")
    print("Press Ctrl+C again to force exit (not recommended - data may be lost)")
    
    # Continue execution - the main loop will check is_exiting and save progress

def save_checkpoint(batch_results, output_csv, batch_num, force=False):
    """Save the current progress as a checkpoint."""
    if not batch_results and not force:
        return
    
    # Save to main output if possible
    if batch_results:
        success = save_transcriptions(batch_results, output_csv)
        if not success:
            # Save to checkpoint file if main save fails
            checkpoint_file = f"checkpoint_{session_id}_batch_{batch_num}.csv"
            try:
                pd.DataFrame(batch_results).to_csv(checkpoint_file, index=False)
                logger.info(f"Saved checkpoint to {checkpoint_file}")
            except Exception as e:
                logger.critical(f"Failed to save checkpoint: {str(e)}")
    
    # Update session log with checkpoint information
    with open(session_log_file, 'a') as f:
        f.write(f"\n=== CHECKPOINT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        f.write(f"Files processed: {current_file_index}/{total_files}\n")
        f.write(f"Batch: {batch_num}/{total_batches}\n")
        if start_time:
            elapsed = (datetime.now() - start_time).total_seconds()
            f.write(f"Elapsed time: {timedelta(seconds=int(elapsed))}\n")
            
            if current_file_index > 0:
                est_remaining = estimate_completion_time(current_file_index, total_files, elapsed)
                f.write(f"Estimated remaining: {est_remaining}\n")
        
        if is_exiting:
            f.write("PROCESS INTERRUPTED BY USER - PARTIAL COMPLETION\n")

def main():
    """Main function to transcribe audio files and save results to CSV."""
    global start_time, total_files, current_file_index, current_batch, total_batches, progress_thread, is_exiting
    
    # Set up signal handlers for graceful exit
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Configuration
    INPUT_FOLDER = os.getenv("INPUT_FOLDER", "/Users/namanagarwal/voice call/clips")
    OUTPUT_CSV = os.getenv("OUTPUT_CSV", "/Users/namanagarwal/voice call/call_transcriptions.csv")
    LANGUAGE_CODE = os.getenv("LANGUAGE_CODE", "hin")  # Default to Hindi
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))  # Process in batches to save progress frequently
    
    # Create a session identifier for this run
    logger.info(f"Starting transcription process - Session ID: {session_id}")
    
    # Start progress tracking thread
    progress_thread = threading.Thread(target=print_progress, daemon=True)
    progress_thread.start()
    
    try:
        # Setup environment and client
        client = setup_environment()
        if not client:
            logger.error("Failed to initialize ElevenLabs client. Exiting.")
            return
        
        # Get audio files
        audio_files = get_audio_files(INPUT_FOLDER)
        if not audio_files:
            logger.warning(f"No audio files found in {INPUT_FOLDER}")
            return
        
        # Get already transcribed files
        already_transcribed = get_already_transcribed_files(OUTPUT_CSV)
        
        # Filter out files that have already been transcribed
        files_to_transcribe = [f for f in audio_files if os.path.basename(f) not in already_transcribed]
        logger.info(f"Found {len(files_to_transcribe)} new files to transcribe")
        
        if not files_to_transcribe:
            logger.info("No new files to transcribe")
            return
        
        # Set global counters for progress tracking
        total_files = len(files_to_transcribe)
        current_file_index = 0
        total_batches = (total_files - 1) // BATCH_SIZE + 1
        
        # Create session log file to track progress
        with open(session_log_file, 'w') as f:
            f.write(f"Session started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Files to transcribe: {len(files_to_transcribe)}\n")
            f.write(f"Total batches: {total_batches} (batch size: {BATCH_SIZE})\n\n")
        
        # Start timing
        start_time = datetime.now()
        
        # Process files in batches to save progress periodically
        total_processed = 0
        total_successful = 0
        total_failed = 0
        
        for i in range(0, len(files_to_transcribe), BATCH_SIZE):
            # Check if exit was requested
            if is_exiting:
                logger.info("Exit requested. Stopping after current batch.")
                break
                
            batch_files = files_to_transcribe[i:i+BATCH_SIZE]
            current_batch = i//BATCH_SIZE + 1
            logger.info(f"Processing batch {current_batch}/{total_batches} ({len(batch_files)} files)")
            
            # Process batch
            batch_results = []
            batch_successful = 0
            batch_failed = 0
            
            for file_path in tqdm(batch_files, desc=f"Batch {current_batch}/{total_batches}"):
                # Check if exit was requested during processing
                if is_exiting:
                    logger.info("Exit requested. Finishing current file and saving progress.")
                    # Continue with current file to complete it, then break the loop
                
                try:
                    result = transcribe_audio(client, file_path, LANGUAGE_CODE)
                    if result:
                        batch_results.append(result)
                        if "ERROR:" not in result["transcription"]:
                            batch_successful += 1
                            # Log success
                            with open(session_log_file, 'a') as f:
                                f.write(f"SUCCESS: {os.path.basename(file_path)}\n")
                        else:
                            batch_failed += 1
                            # Log failure
                            with open(session_log_file, 'a') as f:
                                f.write(f"FAILED: {os.path.basename(file_path)} - {result['transcription']}\n")
                except Exception as e:
                    logger.error(f"Unhandled exception processing {file_path}: {str(e)}")
                    batch_failed += 1
                    # Log exception
                    with open(session_log_file, 'a') as f:
                        f.write(f"EXCEPTION: {os.path.basename(file_path)} - {str(e)}\n")
                
                # If exit requested after processing current file, break
                if is_exiting and total_processed < total_files - 1:
                    break
            
            # Update totals
            total_processed += len(batch_files)
            total_successful += batch_successful
            total_failed += batch_failed
            
            # Save batch results
            if batch_results or is_exiting:
                logger.info(f"Saving batch results: {len(batch_results)} transcriptions")
                save_checkpoint(batch_results, OUTPUT_CSV, current_batch, force=is_exiting)
            
            # Log batch summary
            logger.info(f"Batch {current_batch} complete: {batch_successful} successful, {batch_failed} failed")
            with open(session_log_file, 'a') as f:
                f.write(f"\nBatch {current_batch} summary: {batch_successful} successful, {batch_failed} failed\n\n")
            
            # If exiting, don't continue to next batch
            if is_exiting:
                break
        
        # Calculate elapsed time
        elapsed = datetime.now() - start_time
        elapsed_str = str(timedelta(seconds=int(elapsed.total_seconds())))
        
        # Log final summary
        logger.info(f"Transcription session {session_id} completed")
        logger.info(f"Total files processed: {total_processed}/{total_files}")
        logger.info(f"Successfully transcribed: {total_successful}")
        logger.info(f"Failed transcriptions: {total_failed}")
        logger.info(f"Total time: {elapsed_str}")
        
        with open(session_log_file, 'a') as f:
            f.write(f"\n=== FINAL SUMMARY ===\n")
            f.write(f"Session completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total files processed: {total_processed}/{total_files}")
            if is_exiting:
                f.write(" (PARTIAL - Process was interrupted)")
            f.write("\n")
            f.write(f"Successfully transcribed: {total_successful}\n")
            f.write(f"Failed transcriptions: {total_failed}\n")
            f.write(f"Total time: {elapsed_str}\n")
        
        if total_failed > 0:
            logger.warning(f"Some transcriptions failed. See {session_log_file} for details.")
        elif total_processed == total_files:
            logger.info("All transcriptions completed successfully.")
        else:
            logger.info("Process was interrupted before completion.")
            
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {str(e)}", exc_info=True)
        
        # Try to save emergency log
        with open(f"emergency_log_{session_id}.txt", 'w') as f:
            f.write(f"EMERGENCY LOG - CRITICAL ERROR at {datetime.now()}\n")
            f.write(f"Error: {str(e)}\n")
            f.write(f"Files processed: {current_file_index}/{total_files}\n")
            f.write(f"Current batch: {current_batch}/{total_batches}\n")
        
        return 1
    finally:
        # Signal the progress thread to exit
        is_exiting = True
        if progress_thread and progress_thread.is_alive():
            progress_thread.join(timeout=2)
            
        logger.info("Transcription process completed. Exit successful.")
        return 0

if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = main()
    except Exception as e:
        logger.critical(f"Unhandled exception in script: {str(e)}", exc_info=True)
    finally:
        sys.exit(exit_code)