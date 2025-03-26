#!/usr/bin/env python3
"""
Call Center Clip Processor
Processes .aac audio files from the clips directory, transcribes them,
analyzes the content, and stores everything in the database.
"""

import os
import sys
import logging
import asyncio
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import glob
import json

# Import Database Layer
from dao.transcription_dao import TranscriptionDAO
from dao.analysis_dao import AnalysisResultDAO
from dao.stats_dao import StatsDAO

# Import Configuration manager 
from config_manager import config

# Import API clients
from api.clients.elevenlabs_client import ElevenLabsClient
from api.clients.openai_client import OpenAIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"processing_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ClipProcessor:
    """Process call center audio clips end-to-end"""
    
    def __init__(self, clips_dir: str = None, batch_size: int = None):
        """
        Initialize the clip processor
        
        Args:
            clips_dir: Directory containing audio clips
            batch_size: Number of clips to process in a batch
        """
        # Get configuration
        self.clips_dir = clips_dir or config.get("clips_dir")
        self.batch_size = batch_size or config.get("batch_size", 10)
        self.db_path = config.get("db_path")
        
        # Initialize DAOs
        self.transcription_dao = TranscriptionDAO(self.db_path)
        self.analysis_dao = AnalysisResultDAO(self.db_path)
        self.stats_dao = StatsDAO(self.db_path)
        
        # Initialize API clients
        try:
            self.elevenlabs_client = ElevenLabsClient()
            self.openai_client = OpenAIClient(
                model=config.get("openai_model", "gpt-4-turbo"),
                max_retries=config.get("max_retries", 3)
            )
            logger.info("API clients initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize API clients: {str(e)}")
            raise
        
        # Stats for this processing session
        self.stats = {
            "total_files": 0,
            "transcription_success": 0,
            "transcription_failed": 0,
            "analysis_success": 0,
            "analysis_failed": 0,
            "start_time": time.time(),
            "model": config.get("openai_model", "gpt-4-turbo"),
            "total_tokens": 0,
            "total_cost": 0.0,
            "processing_times": [],
        }
    
    def find_audio_files(self) -> List[str]:
        """
        Find audio files in the clips directory
        
        Returns:
            List of audio file paths
        """
        # Create the clips directory if it doesn't exist
        os.makedirs(self.clips_dir, exist_ok=True)
        
        # Find all .aac files
        file_pattern = os.path.join(self.clips_dir, "*.aac")
        files = glob.glob(file_pattern)
        
        logger.info(f"Found {len(files)} .aac files in {self.clips_dir}")
        return files
    
    def extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from the file path
        
        Args:
            file_path: Path to the audio file
            
        Returns:
            Dictionary of metadata
        """
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        # Extract date from filename if possible
        # Expected format: call_YYYYMMDD_HHMMSS.aac or similar
        date_str = None
        try:
            import re
            date_match = re.search(r'(\d{8})[_-]?(\d{6})?', file_name)
            if date_match:
                if date_match.group(2):
                    # Format includes time
                    date_str = f"{date_match.group(1)}_{date_match.group(2)}"
                else:
                    # Format is just date
                    date_str = date_match.group(1)
        except Exception as e:
            logger.warning(f"Failed to extract date from filename {file_name}: {str(e)}")
        
        return {
            "file_name": file_name,
            "file_path": file_path,
            "file_size": file_size,
            "call_date": date_str,
            "call_id": file_name,  # Use filename as call_id
        }
    
    async def transcribe_file(self, file_path: str) -> Dict[str, Any]:
        """
        Transcribe an audio file
        
        Args:
            file_path: Path to the audio file
            
        Returns:
            Dictionary with transcription and metadata
        """
        metadata = self.extract_metadata(file_path)
        
        try:
            logger.info(f"Transcribing {metadata['file_name']}...")
            
            # Read the audio file
            with open(file_path, 'rb') as audio_file:
                audio_data = audio_file.read()
            
            # Transcribe using ElevenLabs
            # In a real implementation, we would use the async version of this
            # or run it in a thread pool for better performance
            transcription_result = self.elevenlabs_client.transcribe_audio(
                audio_data=audio_data,
                language_code="en",  # Use appropriate language code
                model_id="scribe_v1"  # Use appropriate model ID
            )
            
            if transcription_result.get("status") == "error":
                logger.error(f"Transcription failed: {transcription_result.get('error')}")
                metadata["transcription"] = f"ERROR: {transcription_result.get('error')}"
                self.stats["transcription_failed"] += 1
                return metadata
            
            # Extract transcription text
            transcription_text = transcription_result.get("text", "")
            if not transcription_text:
                logger.warning(f"Empty transcription for {metadata['file_name']}")
                metadata["transcription"] = "ERROR: Empty transcription"
                self.stats["transcription_failed"] += 1
                return metadata
            
            # Calculate estimated duration from segments if available
            if "segments" in transcription_result and transcription_result["segments"]:
                segments = transcription_result["segments"]
                if segments:
                    last_segment = segments[-1]
                    duration_seconds = last_segment.get("end", 0)
                    metadata["duration_seconds"] = duration_seconds
            
            # Add transcription to metadata
            metadata["transcription"] = transcription_text
            metadata["speaker_count"] = transcription_result.get("speaker_count", 1)
            
            logger.info(f"Transcription completed: {len(transcription_text)} characters")
            self.stats["transcription_success"] += 1
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error transcribing {file_path}: {str(e)}")
            metadata["transcription"] = f"ERROR: {str(e)}"
            self.stats["transcription_failed"] += 1
            return metadata
    
    def save_transcription(self, transcription_data: Dict[str, Any]) -> bool:
        """
        Save transcription to database
        
        Args:
            transcription_data: Transcription data
            
        Returns:
            Success flag
        """
        try:
            success = self.transcription_dao.save(transcription_data)
            logger.info(f"Saved transcription for {transcription_data['file_name']}: {success}")
            return success
        except Exception as e:
            logger.error(f"Error saving transcription: {str(e)}")
            return False
    
    async def analyze_transcription(self, transcription_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a transcription
        
        Args:
            transcription_data: Transcription data
            
        Returns:
            Analysis results
        """
        call_id = transcription_data.get("call_id")
        transcription = transcription_data.get("transcription", "")
        
        # Skip if there was an error in transcription
        if transcription.startswith("ERROR:"):
            logger.warning(f"Skipping analysis for {call_id} due to transcription error")
            return {
                "call_id": call_id,
                "analysis_status": "failed",
                "api_error": "Transcription error",
                "issue_summary": "Could not analyze due to transcription error."
            }
        
        # Skip if transcription is empty
        if not transcription or len(transcription.strip()) < 10:
            logger.warning(f"Skipping analysis for {call_id} due to empty/short transcription")
            return {
                "call_id": call_id,
                "analysis_status": "failed",
                "api_error": "Empty or too short transcription",
                "issue_summary": "The transcription was empty or too short to analyze."
            }
        
        try:
            logger.info(f"Analyzing transcription for {call_id}...")
            
            # Create analysis prompt
            system_message = "You are an expert call center analyst for financial services who returns structured analysis in JSON format."
            prompt = f"""
            Please analyze this call center conversation transcript and extract the following information:
            
            TRANSCRIPT:
            {transcription}
            
            INSTRUCTIONS:
            1. Identify the main issue the customer is calling about.
            2. Categorize the issue (Account Access, Transaction Issue, Product Question, etc).
            3. Determine the severity of the issue (Low, Medium, High).
            4. Extract any relevant technical context (device, system, portal).
            5. Identify what solutions were attempted and their outcome.
            6. Determine if there were any knowledge gaps from the agent.
            7. Note the overall sentiment of the interaction.
            
            Return your analysis as a JSON object with the following structure:
            {{
              "issue_classification": {{
                "primary_category": "string",
                "specific_issue": "string",
                "issue_status": "Resolved|Partially Resolved|Unresolved",
                "severity": "Low|Medium|High"
              }},
              "technical_context": {{
                "caller_type": "string",
                "experience_level": "Beginner|Intermediate|Advanced",
                "system_portal": "string",
                "device_information": "string"
              }},
              "issue_details": {{
                "error_messages": "string or null",
                "feature_involved": "string",
                "issue_preconditions": "string",
                "failure_point": "string",
                "attempted_solutions": "string",
                "resolution_steps": "string or null"
              }},
              "key_quotes": {{
                "issue_description_quote": "string",
                "impact_statement_quote": "string or null"
              }},
              "issue_summary": "string",
              "agent_assessment": {{
                "knowledge_gap_identified": "string or null",
                "sentiment": "Positive|Neutral|Negative"
              }}
            }}
            
            Do not include explanations outside the JSON structure. Only return valid JSON.
            """
            
            messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt}
            ]
            
            # Track processing time
            start_time = time.time()
            
            # Analyze using OpenAI
            result = await self.openai_client.analyze_transcript(messages, call_id)
            
            # Calculate processing time
            processing_time = (time.time() - start_time) * 1000  # convert to ms
            self.stats["processing_times"].append(processing_time)
            
            # Check for API errors
            if "api_error" in result:
                logger.error(f"Analysis API error for {call_id}: {result['api_error']}")
                result["analysis_status"] = "failed"
                self.stats["analysis_failed"] += 1
            else:
                # Add additional metadata
                result["analysis_status"] = "completed"
                result["processing_time_ms"] = processing_time
                result["model"] = config.get("openai_model", "gpt-4-turbo")
                result["call_date"] = transcription_data.get("call_date")
                self.stats["analysis_success"] += 1
                
                # Track token usage if available
                if "token_usage" in result:
                    self.stats["total_tokens"] += result["token_usage"].get("total_tokens", 0)
                
                # Update cost statistics if available
                if "cost" in result:
                    self.stats["total_cost"] += result["cost"]
            
            logger.info(f"Analysis completed for {call_id} in {processing_time:.2f} ms")
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing {call_id}: {str(e)}")
            return {
                "call_id": call_id,
                "analysis_status": "failed",
                "api_error": str(e),
                "issue_summary": f"Analysis failed due to error: {str(e)}"
            }
    
    def save_analysis(self, analysis_data: Dict[str, Any]) -> bool:
        """
        Save analysis to database
        
        Args:
            analysis_data: Analysis data
            
        Returns:
            Success flag
        """
        try:
            # Make sure required fields are present
            if "call_id" not in analysis_data:
                logger.error("Cannot save analysis without call_id")
                return False
            
            success = self.analysis_dao.save(analysis_data)
            logger.info(f"Saved analysis for {analysis_data['call_id']}: {success}")
            return success
        except Exception as e:
            logger.error(f"Error saving analysis: {str(e)}")
            return False
    
    def save_processing_stats(self) -> bool:
        """
        Save processing statistics to database
        
        Returns:
            Success flag
        """
        try:
            # Calculate derived statistics
            total_time = time.time() - self.stats["start_time"]
            avg_processing_time = 0
            if self.stats["processing_times"]:
                avg_processing_time = sum(self.stats["processing_times"]) / len(self.stats["processing_times"])
            
            stats_record = {
                "run_date": datetime.now().isoformat(),
                "total_processed": self.stats["total_files"],
                "successful": self.stats["analysis_success"],
                "failed": self.stats["analysis_failed"],
                "avg_processing_time": avg_processing_time,
                "model": self.stats["model"],
                "batch_size": self.batch_size,
                "total_tokens": self.stats["total_tokens"],
                "total_cost": self.stats["total_cost"],
                "run_duration_seconds": total_time
            }
            
            success = self.stats_dao.save_stats(stats_record)
            logger.info(f"Saved processing stats: {success}")
            return success
        except Exception as e:
            logger.error(f"Error saving processing stats: {str(e)}")
            return False
    
    async def process_file(self, file_path: str) -> tuple:
        """
        Process a single audio file end-to-end
        
        Args:
            file_path: Path to the audio file
            
        Returns:
            Tuple of (transcription_success, analysis_success)
        """
        # Step 1: Transcribe the file
        transcription_data = await self.transcribe_file(file_path)
        
        # Step 2: Save transcription to database
        transcription_success = self.save_transcription(transcription_data)
        
        # Step 3: Analyze the transcription
        analysis_data = await self.analyze_transcription(transcription_data)
        
        # Step 4: Save analysis to database
        analysis_success = self.save_analysis(analysis_data)
        
        return (transcription_success, analysis_success)
    
    async def process_files(self, files: List[str]) -> Dict[str, Any]:
        """
        Process a list of audio files
        
        Args:
            files: List of file paths
            
        Returns:
            Processing statistics
        """
        self.stats["total_files"] = len(files)
        
        logger.info(f"Starting to process {len(files)} files in batches of {self.batch_size}")
        
        # Process files in batches
        for i in range(0, len(files), self.batch_size):
            batch = files[i:i+self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1}/{(len(files)-1)//self.batch_size + 1} ({len(batch)} files)")
            
            # Process each file in the batch concurrently
            tasks = [self.process_file(file) for file in batch]
            await asyncio.gather(*tasks)
            
            logger.info(f"Completed batch {i//self.batch_size + 1}")
        
        # Save final statistics
        self.save_processing_stats()
        
        logger.info(f"Processing completed. Success rate: {self.stats['analysis_success']}/{self.stats['total_files']} ({self.stats['analysis_success']/self.stats['total_files']*100:.1f}%)")
        
        return self.stats
    
    async def run(self) -> Dict[str, Any]:
        """
        Run the full processing pipeline
        
        Returns:
            Processing statistics
        """
        # Find audio files
        files = self.find_audio_files()
        
        if not files:
            logger.warning(f"No .aac files found in {self.clips_dir}")
            return self.stats
        
        # Process all files
        await self.process_files(files)
        
        return self.stats

def main():
    """Main entry point"""
    try:
        # Parse command line arguments
        import argparse
        parser = argparse.ArgumentParser(description="Process call center audio clips")
        parser.add_argument("--clips-dir", help="Directory containing audio clips")
        parser.add_argument("--batch-size", type=int, help="Number of clips to process in a batch")
        parser.add_argument("--db-path", help="Path to the database file")
        args = parser.parse_args()
        
        # Override config with command line arguments
        if args.db_path:
            config.set("db_path", args.db_path)
        if args.clips_dir:
            config.set("clips_dir", args.clips_dir)
        if args.batch_size:
            config.set("batch_size", args.batch_size)
        
        # Create processor
        processor = ClipProcessor(
            clips_dir=config.get("clips_dir"),
            batch_size=config.get("batch_size", 10)
        )
        
        # Run the processor
        asyncio.run(processor.run())
        
        return 0
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main()) 