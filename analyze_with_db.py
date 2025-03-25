#!/usr/bin/env python3
"""
Call Center Transcript Analysis System with Database Integration
Uses SQLite database for more robust data storage and querying capabilities.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime

# Import your existing database manager
from database_manager import DatabaseManager

# Import your existing analysis system components
# Update this path if needed to import from your original script
sys.path.append('/Users/namanagarwal/voice call/')
from call_analysis import (
    Config, TextProcessor, CategoryManager, PromptGenerator, 
    OpenAIClient, ResultFormatter, GracefulExit, 
    configure_logging, logger
)

# -----------------------------
# Updated Data Manager
# -----------------------------
class DbDataManager:
    def __init__(self, transcriptions_path, analysis_path, db_manager):
        self.transcriptions_path = transcriptions_path
        self.analysis_path = analysis_path
        self.date_based_path = self._generate_date_based_path(analysis_path)
        self.db_manager = db_manager
    
    def _generate_date_based_path(self, base_path):
        """Generate a date-based path for analysis results"""
        directory = os.path.dirname(base_path)
        filename = os.path.basename(base_path)
        basename, ext = os.path.splitext(filename)
        
        # Create date-based filename
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H%M%S")
        date_filename = f"{basename}_{date_str}_{time_str}{ext}"
        
        # Combine with directory
        if directory:
            return os.path.join(directory, date_filename)
        else:
            return date_filename
    
    def load_transcriptions(self):
        """Load transcriptions from CSV into database"""
        try:
            if os.path.exists(self.transcriptions_path):
                imported_count = self.db_manager.import_transcriptions_from_csv(self.transcriptions_path)
                logger.info(f"Imported/updated {imported_count} transcriptions from CSV to database")
            return None  # Return None as we'll fetch data directly from DB later
        except Exception as e:
            logger.error(f"Error importing transcriptions to database: {str(e)}")
            return None
    
    def load_analysis_results(self):
        """Import existing analysis results to database"""
        try:
            if os.path.exists(self.analysis_path):
                imported_count = self.db_manager.import_analysis_results_from_csv(self.analysis_path)
                logger.info(f"Imported {imported_count} analysis results from CSV to database")
            return None  # Return None as we'll fetch data directly from DB later
        except Exception as e:
            logger.error(f"Error importing analysis results to database: {str(e)}")
            return None
    
    def save_analysis_results(self, results):
        """Save analysis results to database and optionally to CSV"""
        # Count successful saves
        success_count = 0
        
        # For a DataFrame, convert to records
        if hasattr(results, 'to_dict'):
            items = results.to_dict('records')
        else:
            items = results
            
        # Save each result to database
        for result in items:
            if self.db_manager.save_analysis_result(result):
                success_count += 1
        
        logger.info(f"Saved {success_count} analysis results to database")
        
        # Export updated data to CSV
        self.db_manager.export_to_csv('analysis_results', self.analysis_path)
        self.db_manager.export_to_csv('analysis_results', self.date_based_path)
        
        logger.info(f"Exported analysis results to {self.analysis_path} and {self.date_based_path}")
    
    def get_transcriptions_for_analysis(self, reanalyze=False):
        """Get transcriptions that need to be analyzed from the database"""
        return self.db_manager.get_transcriptions_for_analysis(reanalyze=reanalyze)
    
    def get_date_based_path(self):
        """Return the date-based file path"""
        return self.date_based_path

# -----------------------------
# Database-Aware Analysis Service
# -----------------------------
class DbCallAnalysisService:
    def __init__(self, config, db_manager):
        self.config = config
        self.text_processor = TextProcessor()
        self.result_formatter = ResultFormatter(self.text_processor)
        self.category_manager = CategoryManager(config.categories_file)
        self.openai_client = OpenAIClient(
            model=config.openai_model,
            max_retries=config.max_retries
        )
        self.db_manager = db_manager
        self.data_manager = DbDataManager(
            transcriptions_path=config.transcriptions_csv,
            analysis_path=config.analysis_csv,
            db_manager=db_manager
        )
        self.batch_size = config.batch_size
        self.graceful_exit = GracefulExit()
        
        # Statistics tracking
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'total_tokens': 0,
            'total_cost': 0.0,
            'avg_confidence': 0.0,
            'avg_processing_time': 0.0,
            'model': config.openai_model,
            'batch_size': config.batch_size
        }
    
    def prepare_batch_prompts(self, transcriptions):
        """Prepare prompts for each valid transcript"""
        messages_list = []
        valid_items = []
        
        for item in transcriptions:
            # Skip invalid or empty transcriptions
            if not item.get('transcription') or not isinstance(item['transcription'], str) or str(item['transcription']).startswith("ERROR:"):
                continue
            
            text = item['transcription']
            file_name = item['file_name']
            duration = item.get('duration_seconds', 0)
            
            # Create analysis prompt
            prompt = PromptGenerator.generate_analysis_prompt(
                transcript=text,
                file_name=file_name,
                duration=duration,
                text_processor=self.text_processor
            )
            
            # Create message list format for OpenAI
            messages = [
                {"role": "system", "content": "You are an expert call center analyst for financial services who returns structured analysis in JSON format."},
                {"role": "user", "content": prompt}
            ]
            
            messages_list.append(messages)
            valid_items.append(item)
        
        return messages_list, valid_items
    
    async def analyze_batch(self, batch):
        """Process a batch of transcriptions"""
        messages_list, valid_items = self.prepare_batch_prompts(batch)
        
        if not messages_list:
            logger.info("No valid transcriptions in this batch to analyze")
            return []
        
        results = []
        
        # Process each item individually
        for i, (messages, item) in enumerate(zip(messages_list, valid_items)):
            logger.info(f"Processing item {i+1}/{len(valid_items)}: {item['file_name']}")
            
            # Print progress to console
            print(f"🔍 Analyzing call {i+1}/{len(valid_items)}: {item['file_name']}", end="\r")
            
            # Track processing time
            start_time = datetime.now().timestamp()
            
            # Process the transcript
            result = await self.openai_client.analyze_transcript(messages, item['file_name'])
            
            # Add processing time information
            processing_time = (datetime.now().timestamp() - start_time) * 1000  # convert to ms
            result["processing_time"] = processing_time
            
            # Add note if using partial transcript
            chunks = self.text_processor.chunk_text(item['transcription'])
            if len(chunks) > 1:
                result["note"] = f"Analysis based on partial transcription ({len(chunks[0])}/{len(item['transcription'])} chars)"
            
            # Format the result with the formatter
            formatted_result = self.result_formatter.format_analysis_result(result, item['file_name'])
            
            # Add model information
            formatted_result['model'] = self.config.openai_model
            
            # Output completion status with error handling
            if "api_error" in formatted_result:
                print(f"⚠️  Partial analysis for {item['file_name']}: API error but some data recovered")
            elif "error" in formatted_result:
                print(f"❌ Failed: {item['file_name']} - {formatted_result.get('error')[:50]}...")
            else:
                # Calculate confidence score
                confidence = formatted_result.get('confidence_score', 0)
                
                # Use emoji based on confidence
                emoji = "✅" if confidence >= 80 else "⚠️" if confidence >= 50 else "❓"
                
                # Get the primary issue if available
                primary_issue = formatted_result.get('primary_issue_category', 'Unknown')
                specific_issue = formatted_result.get('specific_issue', '')
                
                # Print status with confidence score
                print(f"{emoji} {item['file_name']} analyzed (confidence: {confidence:.1f}%) - {primary_issue}: {specific_issue[:40]}")
            
            # Update statistics
            self.stats['total_processed'] += 1
            if formatted_result.get('analysis_status') == 'completed':
                self.stats['successful'] += 1
            else:
                self.stats['failed'] += 1
            
            if 'confidence_score' in formatted_result:
                self.stats['avg_confidence'] = ((self.stats['avg_confidence'] * (self.stats['total_processed'] - 1)) + 
                                             formatted_result['confidence_score']) / self.stats['total_processed']
            
            if 'processing_time_ms' in formatted_result:
                self.stats['avg_processing_time'] = ((self.stats['avg_processing_time'] * (self.stats['total_processed'] - 1)) + 
                                                  formatted_result['processing_time_ms']) / self.stats['total_processed']
            
            results.append(formatted_result)
        
        # Clear the progress line
        print(" " * 100, end="\r")
        
        return results
    
    async def process_transcriptions(self, reanalyze=False):
        """Process all transcriptions in batches"""
        # Load data into database
        self.data_manager.load_transcriptions()
        self.data_manager.load_analysis_results()
        
        # Get transcriptions that need analysis
        transcriptions_to_analyze = self.data_manager.get_transcriptions_for_analysis(reanalyze)
        
        if not transcriptions_to_analyze:
            logger.info("No transcriptions to analyze")
            return
        
        logger.info(f"Found {len(transcriptions_to_analyze)} transcriptions to analyze")
        
        # Process in batches
        total_batches = (len(transcriptions_to_analyze) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(transcriptions_to_analyze), self.batch_size):
            if self.graceful_exit.should_exit():
                logger.info("Exit requested, stopping after current batch")
                break
                
            batch = transcriptions_to_analyze[i:i + self.batch_size]
            logger.info(f"Processing batch {i // self.batch_size + 1}/{total_batches} ({len(batch)} items)")
            
            # Process this batch
            batch_results = await self.analyze_batch(batch)
            
            # Save batch results
            if batch_results:
                self.data_manager.save_analysis_results(batch_results)
                logger.info(f"Saved batch {i // self.batch_size + 1}/{total_batches}")
                
        # Save run statistics
        self.db_manager.save_stats(self.stats)
        logger.info("Saved run statistics to database")
    
    async def run(self, reanalyze=False):
        """Main entry point to run the analysis process"""
        logger.info("=" * 50)
        logger.info("CALL ANALYZER WITH DATABASE".center(50))
        logger.info("=" * 50)
        
        if reanalyze:
            logger.info("REANALYSIS MODE: Analyzing all calls")
        
        start_time = datetime.now().timestamp()
        
        # Process all transcriptions
        await self.process_transcriptions(reanalyze)
        
        # Calculate and display execution statistics
        elapsed_time = datetime.now().timestamp() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        # Display completion banner
        print("\n" + "=" * 70)
        print(" ANALYSIS COMPLETE ".center(70, "="))
        print(f" Time elapsed: {minutes} minutes, {seconds} seconds ".center(70))
        
        # Get stats from database
        stats = self.db_manager.get_summary_statistics()
        
        print("\nAnalysis Summary:")
        print(f"- Total transcriptions: {stats.get('total_transcriptions', 'N/A')}")
        print(f"- Total analyzed: {stats.get('total_analyzed', 'N/A')}")
        print(f"- Completed analyses: {stats.get('completed_analyses', 'N/A')}")
        print(f"- Failed analyses: {stats.get('failed_analyses', 'N/A')}")
        
        if 'avg_confidence' in stats and stats['avg_confidence']:
            print(f"- Average confidence: {stats['avg_confidence']:.2f}%")
        
        print(f"\n💾 Results stored in database: {self.db_manager.db_path}")
        print(f"📄 Results also exported to: {self.config.analysis_csv}")
        print(f"📑 Date-based copy: {self.data_manager.get_date_based_path()}")
        
        logger.info("=" * 50)
        logger.info("PROCESS COMPLETE".center(50))
        logger.info("=" * 50)

# -----------------------------
# Main Function
# -----------------------------
# -----------------------------
# Main Function
# -----------------------------
async def main():
    """Main entry point"""
    # Configure paths - update these with your paths
    TRANSCRIPTIONS_CSV = "/Users/namanagarwal/voice call/call_transcriptions.csv"
    ANALYSIS_CSV = "/Users/namanagarwal/voice call/analysis_results.csv" 
    DB_PATH = "/Users/namanagarwal/voice call/call_analysis.db"
    CATEGORIES_FILE = "/Users/namanagarwal/voice call/categories.csv"  # Create this if needed
    
    # Parse command line arguments if any
    import argparse
    parser = argparse.ArgumentParser(description='Call Center Analysis with Database')
    parser.add_argument('--reanalyze', action='store_true', help='Reanalyze all calls')
    parser.add_argument('--model', default='gpt-4o', help='OpenAI model to use')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size')
    parser.add_argument('--db-report', action='store_true', help='Just show database report')
    args = parser.parse_args()
    
    # Initialize database manager
    db_manager = DatabaseManager(DB_PATH)
    
    # If just showing report, do that and exit
    if args.db_report:
        stats = db_manager.get_summary_statistics()
        
        print("\n" + "=" * 60)
        print(" CALL CENTER ANALYSIS REPORT ".center(60, "="))
        print("=" * 60)
        
        print(f"\nTotal transcriptions: {stats.get('total_transcriptions', 0)}")
        print(f"Analyzed calls: {stats.get('total_analyzed', 0)} ({stats.get('completed_analyses', 0)} completed, {stats.get('failed_analyses', 0)} failed)")
        
        if 'avg_confidence' in stats and stats['avg_confidence']:
            print(f"Average confidence score: {stats['avg_confidence']:.2f}%")
        
        # Show top issue categories if available
        if 'issue_categories' in stats and stats['issue_categories']:
            print("\nTop Issue Categories:")
            for i, cat in enumerate(stats['issue_categories'][:5]):
                print(f"{i+1}. {cat['primary_issue_category']}: {cat['count']} calls")
        
        print("\n" + "=" * 60)
        return
    
    # Create configuration
    config = Config(
        transcriptions_csv=TRANSCRIPTIONS_CSV,
        analysis_csv=ANALYSIS_CSV,
        categories_file=CATEGORIES_FILE,
        openai_model=args.model,
        batch_size=args.batch_size,
        max_concurrent=3,
        max_retries=3
    )
    
    # Create and run the analyzer
    analyzer = DbCallAnalysisService(config, db_manager)
    await analyzer.run(reanalyze=args.reanalyze)

if __name__ == "__main__":
    asyncio.run(main())