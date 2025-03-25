"""
Call Center Transcript Analysis System
A modular system for analyzing call center transcripts using AI.
"""

import os
import sys
import json
import pandas as pd
import time
import asyncio
import signal
import logging
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dotenv import load_dotenv
from dataclasses import dataclass, field, asdict
from functools import lru_cache

# ------------------------------
# Configuration
# ------------------------------
@dataclass
class Config:
    transcriptions_csv: str
    analysis_csv: str
    categories_file: str = "categories.csv"
    openai_model: str = "gpt-4o"
    batch_size: int = 10
    max_concurrent: int = 3
    max_retries: int = 3

# ------------------------------
# Logging Configuration
# ------------------------------
def configure_logging():
    """Configure application logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("call_analysis.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = configure_logging()

# ------------------------------
# API Client Configuration
# ------------------------------
class APIClient:
    @staticmethod
    def create_elevenlabs_client():
        """Create and return an ElevenLabs client"""
        load_dotenv()
        
        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            logger.error("ELEVENLABS_API_KEY not found in environment variables")
            raise ValueError("ELEVENLABS_API_KEY is required")
            
        try:
            from elevenlabs.client import ElevenLabs
            client = ElevenLabs(api_key=api_key)
            logger.info("ElevenLabs client initialized successfully")
            return client
        except ImportError as e:
            logger.error(f"ElevenLabs client initialization failed: {e}")
            raise ImportError("elevenlabs package is required for transcription") from e
        
    @staticmethod
    def create_openai_client():
        """Create and return an OpenAI client"""
        load_dotenv()
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY not found in environment variables")
            raise ValueError("OPENAI_API_KEY is required")
            
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key)
            logger.info("Using OpenAI client with standard initialization")
            return client
        except (ImportError, TypeError) as e:
            logger.warning(f"OpenAI client initialization failed: {e}. Falling back to legacy client.")
            import openai
            openai.api_key = api_key
            
            # Simple wrapper to maintain compatibility with both client versions
            class LegacyOpenAIWrapper:
                def __init__(self):
                    self.chat = type('obj', (object,), {
                        'completions': type('obj', (object,), {
                            'create': self._create_chat_completion
                        })
                    })
                
                def _create_chat_completion(self, model=None, messages=None, **kwargs):
                    return openai.ChatCompletion.create(model=model, messages=messages, **kwargs)
            
            return LegacyOpenAIWrapper()

# ------------------------------
# Signal Handling
# ------------------------------
class GracefulExit:
    def __init__(self):
        self.exit_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """Handle interrupt signal"""
        logger.info("Interrupt received, finishing current batch before exiting...")
        self.exit_requested = True
    
    def should_exit(self):
        """Check if exit has been requested"""
        return self.exit_requested

# ------------------------------
# Text Processing
# ------------------------------
class TextProcessor:
    @staticmethod
    def chunk_text(text: str, max_length: int = 8000) -> List[str]:
        """Split long text into chunks for API processing"""
        if not text or len(text) <= max_length:
            return [text] if text else []
        
        # Split by sentence endings
        sentence_endings = r'(?<=[.!?])\s+'
        sentences = re.split(sentence_endings, text)
        
        chunks = []
        current_chunk = []
        current_length = 0
        
        for sentence in sentences:
            sentence_length = len(sentence)
            
            if current_length + sentence_length + 1 <= max_length:
                current_chunk.append(sentence)
                current_length += sentence_length + 1  # +1 for space
            else:
                if current_chunk:
                    chunks.append(' '.join(current_chunk))
                current_chunk = [sentence]
                current_length = sentence_length
        
        # Add the last chunk
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks
    
    @staticmethod
    @lru_cache(maxsize=100)  # Cache results for efficiency
    def extract_date_from_filename(file_name: str) -> str:
        """Extract date from filename pattern with caching"""
        call_date = ""
        try:
            # Common date patterns in filenames
            date_patterns = [
                r'(\d{4}[-_/]\d{1,2}[-_/]\d{1,2})',  # YYYY-MM-DD, YYYY/MM/DD, YYYY_MM_DD
                r'(\d{1,2}[-_/]\d{1,2}[-_/]\d{4})',  # MM-DD-YYYY, MM/DD/YYYY, MM_DD_YYYY
                r'(\d{8})'  # YYYYMMDD
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, file_name)
                if match:
                    date_str = match.group(1)
                    # Convert YYYYMMDD to YYYY-MM-DD
                    if len(date_str) == 8 and date_str.isdigit():
                        call_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                    else:
                        call_date = date_str
                    break
        
        except Exception as e:
            logger.error(f"Error extracting date from filename {file_name}: {str(e)}")
        
        return call_date

# ------------------------------
# Category Management
# ------------------------------
@dataclass
class Categories:
    options: Dict[str, List[str]] = field(default_factory=lambda: {'L1': [], 'L2': [], 'L3': []})
    valid_combinations: List[Dict[str, str]] = field(default_factory=list)

class CategoryManager:
    def __init__(self, categories_file: str):
        self.categories_file = categories_file
    
    def load_categories(self) -> Categories:
        """Load category options from CSV file"""
        if not os.path.exists(self.categories_file):
            logger.warning(f"Categories file {self.categories_file} not found, using defaults")
            return self._get_default_categories()
        
        try:
            df = pd.read_csv(self.categories_file)
            return self._parse_category_dataframe(df)
        except Exception as e:
            logger.error(f"Error loading categories: {str(e)}")
            return Categories()
    
    def _get_default_categories(self) -> Categories:
        """Return basic default categories"""
        return Categories(
            options={
                'L1': ['Product Information', 'Technical Support', 'Pricing', 'Other'],
                'L2': ['General', 'Specific', 'Advanced', 'Basic'],
                'L3': ['Query', 'Issue', 'Feedback', 'Suggestion']
            },
            valid_combinations=[
                {'L1': 'Product Information', 'L2': 'General', 'L3': 'Query'},
                {'L1': 'Technical Support', 'L2': 'Specific', 'L3': 'Issue'},
                {'L1': 'Pricing', 'L2': 'Basic', 'L3': 'Query'},
                {'L1': 'Other', 'L2': 'General', 'L3': 'Feedback'}
            ]
        )
    
    def _parse_category_dataframe(self, df: pd.DataFrame) -> Categories:
        """Parse categories from DataFrame"""
        # Find column names that match expected patterns
        l1_candidates = ['L1_Category', 'Column A: L1 Category', 'Column A: L1 Category (High-Level)']
        l2_candidates = ['L2_Category', 'Column B: L2 Category', 'Column B: L2 Category (Mid-Level)']
        l3_candidates = ['L3_Category', 'Column C: L3 Category', 'Column C: L3 Category (Granular Detail)']
        
        # Get the first matching column name for each level
        l1_col = next((col for col in l1_candidates if col in df.columns), None)
        l2_col = next((col for col in l2_candidates if col in df.columns), None)
        l3_col = next((col for col in l3_candidates if col in df.columns), None)
        
        # If expected columns not found, try to use first three columns
        if not all([l1_col, l2_col, l3_col]):
            missing_cols = []
            if not l1_col: missing_cols.append("L1 category")
            if not l2_col: missing_cols.append("L2 category")
            if not l3_col: missing_cols.append("L3 category")
            
            logger.warning(f"Missing required columns in categories file: {', '.join(missing_cols)}")
            
            # Fall back to first three columns if they exist
            columns = df.columns.tolist()[:3]
            if len(columns) >= 3:
                logger.info(f"Using first three columns as L1, L2, L3: {columns}")
                l1_col, l2_col, l3_col = columns
            else:
                # Not enough columns available
                logger.error("Not enough columns in categories file")
                return Categories()
        
        # Extract unique values for each category level
        l1_options = df[l1_col].dropna().unique().tolist()
        l2_options = df[l2_col].dropna().unique().tolist()
        l3_options = df[l3_col].dropna().unique().tolist()
        
        # Build valid combinations from rows
        combinations = []
        for _, row in df.iterrows():
            # Skip rows with missing values
            l1 = row[l1_col] if pd.notna(row[l1_col]) else None
            l2 = row[l2_col] if pd.notna(row[l2_col]) else None
            l3 = row[l3_col] if pd.notna(row[l3_col]) else None
            
            if all([l1, l2, l3]):
                combinations.append({
                    'L1': l1,
                    'L2': l2,
                    'L3': l3
                })
        
        logger.info(f"Loaded {len(l1_options)} L1 categories, {len(combinations)} valid combinations")
        return Categories(
            options={
                'L1': l1_options,
                'L2': l2_options,
                'L3': l3_options
            },
            valid_combinations=combinations
        )

# ------------------------------
# Prompt Generation
# ------------------------------
class PromptGenerator:
    @staticmethod
    def generate_analysis_prompt(transcript: str, file_name: str, duration: int, text_processor: TextProcessor) -> str:
        """Generate the prompt for OpenAI to analyze the call transcript"""
        # Create a partial note if we're only using part of the transcript
        chunks = text_processor.chunk_text(transcript)
        text_to_use = chunks[0]
        partial_note = f"[PARTIAL TRANSCRIPT - First {len(text_to_use)} of {len(transcript)} chars]" if len(chunks) > 1 else ""
        
        return f"""
# Call Analysis Task
You are an expert call center analyst for Volt Money, a fintech company specializing in loans against securities (LAS), primarily mutual funds. Your goal is to perform a detailed forensic analysis of this call transcript to extract precise information about the issue, how to reproduce it, and its impact on users.

## Call Data:
- Call ID: {file_name}
- Duration: {duration} seconds
- Transcript: {partial_note} {text_to_use}

## Business Context:
Volt Money connects retail investors, financial advisors, and lending institutions, enabling customers to leverage their mutual fund investments as collateral without selling them. Key services include:
1. Loans Against Mutual Funds (with lenders like DSP, Tata Capital, Bajaj Finance)
2. Partner Affiliate Program (for financial advisors with commission structures)
3. Credit Limit Enhancement
4. Shortfall Management (for handling market fluctuations affecting collateral value)

## Critical Process Workflows:
1. Loan Application Process:
   a. Eligibility check using RTAs (KFIN, CAMS, MFC)
   b. KYC verification (Aadhaar, PAN, selfie)
   c. Portfolio valuation and credit limit determination
   d. Lender selection (DSP, Tata, Bajaj)
   e. Bank account verification and mandate setup
   f. Agreement acceptance
   g. Loan disbursement

2. Withdrawal Process:
   a. Request initiation via app/portal
   b. OTP validation for security
   c. Amount verification against available limit
   d. Processing and disbursement to registered bank account

3. Shortfall Management:
   a. Market fluctuation notification
   b. Grace period for resolution
   c. Additional pledging or partial repayment
   d. Potential automatic sell-off if unresolved

4. Partner Commission Process:
   a. Client application processing
   b. Loan disbursement triggering commission calculation
   c. Bank account verification
   d. Periodic payout based on bank details

## User Personas:
1. End Customers:
   - New Users: Often confused about OTP processes, documentation, KYC
   - Existing Users: Typically concerned with withdrawals, repayments, interest charges
   - Experience Levels: Range from "Inexperienced" to "Experienced"
   - Common Pain Points: Unclear fee structures, process complexity, technical difficulties

2. Affiliate Partners:
   - Primary Concerns: Commission payouts, bank detail updates, client registration
   - Key Activities: Client onboarding, eligibility checks, application processing
   - Business Needs: Timely payouts, clear offers to present to clients, system reliability

## Technical Systems:
1. Customer-Facing: Volt Money mobile app, Web portal, Digital verification systems (OTP, Aadhaar, selfie)
2. Partner Platforms: Partner Dashboard, Red Vision integration, Commission payout system
3. Back-End Systems: Mutual Fund Pledge Portal, Payment Processing System, Loan Management System, KYC verification

## Common Issue Categories:
1. Process Issues (42%): OTP handling, Bank detail updates, Mandate setup, Documentation
2. Technical Issues (31%): Application loading failures, Payment button visibility, Data synchronization, Login problems
3. Communication Gaps (18%): Interest rate clarity, Offer details, Process timeline expectations
4. Knowledge Gaps (9%): Product understanding, System navigation, Procedural knowledge

## Extraction Tasks:
Carefully analyze the transcript and extract the following with high confidence:

1. Issue Classification (Be specific and confident)
* **Primary Issue Category**: Categorize as one of: Process Issue, Technical Issue, Communication Gap, Knowledge Gap. If none apply, choose the closest fit.
* **Specific Issue**: Name the exact issue including WHERE in the process flow it occurs (e.g., "OTP delivery failure during loan disbursement withdrawal request" rather than just "OTP issue")
* **Process Stage**: Identify the exact workflow stage where the issue occurs (e.g., "KYC verification during loan application", "OTP validation during withdrawal process")
* **Issue Status**: Classify as: Resolved During Call, Workaround Provided, Escalated, or Unresolved

2. Caller Information (Gather context about who is experiencing the issue)
* **Caller Type**: Identify specifically as: End Customer (New/Existing), Affiliate Partner, Financial Advisor, Internal Staff, or Other (specify)
* **Experience Level**: Assess as: New User (< 1 month), Intermediate (1-6 months), Experienced (6+ months), or Expert (power user)
* **Intent**: Describe the specific business objective they were trying to accomplish (e.g., "Withdraw funds from available credit line")

3. Technical Context (Capture system details precisely)
* **System/Portal**: Identify the exact system mentioned: Mobile App, Web Portal, Partner Dashboard, Red Vision, Payment System, KYC System, or other specific system
* **Device Information**: Note specifics about browser, OS, device model if mentioned
* **Error Messages**: Capture exact error text in quotes - this is critical for troubleshooting
* **Feature Involved**: Name the specific feature (e.g., "Mutual fund portfolio valuation for credit limit" rather than just "valuation")

4. Issue Recreation Path (Be methodical and detailed)
* **Preconditions**: List the exact state, permissions, data, or conditions needed to encounter the issue
* **Action Sequence**: Number each step (1,2,3...) in the exact workflow sequence that led to the issue
* **Workflow Stage**: Explicitly state which of the critical process workflows (Loan Application, Withdrawal, Shortfall Management, Partner Commission) and which specific sub-step (a,b,c,d,...) is affected
* **Failure Point**: Identify the precise step where the process broke down, with any triggering conditions
* **Expected vs. Actual Outcome**: Clearly contrast what should have happened vs. what actually occurred

5. Impact Assessment
* **Issue Description Quote**: Extract the most descriptive quote where the caller explains the issue
* **Impact Statement Quote**: Find exact quote showing business impact (delays, customer dissatisfaction, financial impact)
* **Severity**: Assess as Critical (business-stopping), High (significant workflow disruption), Medium (causes delays but has workarounds), or Low (minor inconvenience)
* **Frequency**: Note if this appears to be First Occurrence, Intermittent, or Recurring based on caller's statements

6. Resolution Path
* **Attempted Solutions**: List solutions that were already tried before or during the call
* **Resolution Steps**: Document the steps that resolved the issue or the recommended next actions
* **Knowledge Gap Identified**: Note any training or documentation needs revealed in the conversation

7. Structured Issue Summary
* Write a detailed paragraph (at least 5 sentences) that includes:
  - The specific issue and its symptoms
  - The exact workflow stage and process step where it occurs
  - Step-by-step recreation instructions
  - Business impact on the user's workflow
  - Recommended solution or investigation path

## Analysis Guidelines:
- CRITICAL: For any issues (especially OTP failures), specify EXACTLY which process (Loan Application, Withdrawal, etc.) and which step (a,b,c,d) the issue occurred in
- When identifying a problem, always pinpoint the specific workflow step by referencing the Critical Process Workflows section
- If information isn't explicitly mentioned in the transcript, indicate "Not mentioned" rather than guessing
- For error messages, use exact quotes from the transcript
- For action sequences, be specific about each step taken
- Look for financial terms specific to Volt Money's business (pledging, lien, shortfall, credit limit, etc.)

## Output Format:
Return a valid JSON object with the following structure (including all new fields):
{{
  "issue_classification": {{
    "primary_category": "",
    "specific_issue": "",
    "process_stage": "",
    "issue_status": "",
    "severity": ""
  }},
  "caller_information": {{
    "caller_type": "",
    "experience_level": "",
    "intent": ""
  }},
  "technical_context": {{
    "system_portal": "",
    "device_information": "",
    "error_messages": "",
    "feature_involved": ""
  }},
  "issue_recreation": {{
    "preconditions": "",
    "action_sequence": "",
    "workflow_stage": "",
    "failure_point": "",
    "expected_vs_actual": "",
    "frequency": ""
  }},
  "resolution_path": {{
    "attempted_solutions": "",
    "resolution_steps": "",
    "knowledge_gap_identified": ""
  }},
  "key_quotes": {{
    "issue_description": "",
    "impact_statement": ""
  }},
  "issue_summary": ""
}}
"""

# ------------------------------
# OpenAI API Interface
# ------------------------------
class OpenAIClient:
    def __init__(self, model: str, max_retries: int = 3):
        self.client = APIClient.create_openai_client()
        self.model = model
        self.max_retries = max_retries
    
    async def analyze_transcript(self, messages: List[Dict[str, str]], call_id: str) -> Dict[str, Any]:
        """Process a transcript with the OpenAI API with retries"""
        for retry in range(self.max_retries + 1):
            try:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.client.chat.completions.create(
                        model=self.model,
                        messages=messages,
                        response_format={"type": "json_object"}
                    )
                )
                
                # NEW CODE BLOCK - Add token usage tracking
                if hasattr(response, 'usage'):
                    logger.info(
                        f"Token usage for {call_id}: "
                        f"{response.usage.prompt_tokens} prompt + "
                        f"{response.usage.completion_tokens} completion = "
                        f"{response.usage.total_tokens} total"
                    )
            # END NEW CODE BLOCK
                usd_to_inr = 83.5  # Update this conversion rate as needed
    
    # Pricing in USD per 1K tokens
                prompt_price_usd = 0.00005  # for gpt-4o input
                completion_price_usd = 0.00015  # for gpt-4o output
                
                # Convert to Rupees
                prompt_price_inr = prompt_price_usd * usd_to_inr
                completion_price_inr = completion_price_usd * usd_to_inr
                
                # Calculate cost in Rupees
                cost_inr = (response.usage.prompt_tokens / 1000 * prompt_price_inr) + \
                        (response.usage.completion_tokens / 1000 * completion_price_inr)
                
                logger.info(f"Estimated cost for {call_id}: ₹{cost_inr:.2f}")
                # END NEW CODE BLOCK
        
                result_text = response.choices[0].message.content
                
                # Safely parse JSON and validate 
                try:
                    result = json.loads(result_text)
                    
                    # Basic validation
                    if not isinstance(result, dict):
                        raise ValueError("API returned non-object JSON")
                        
                    # Ensure we have at least some of the expected structure
                    if not any(key in result for key in ["issue_classification", "technical_context", "issue_summary"]):
                        raise ValueError("API response missing critical fields")
                    
                    # Add call ID to result
                    result["call_id"] = call_id
                    return result
                    
                except json.JSONDecodeError as json_err:
                    if retry < self.max_retries:
                        logger.warning(f"Invalid JSON from API for {call_id}, retrying: {str(json_err)}")
                        await asyncio.sleep(5 * (retry + 1))
                        continue
                    else:
                        # Create a partial result with error info
                        return {
                            "call_id": call_id, 
                            "api_error": f"JSON parsing error: {str(json_err)}",
                            "issue_summary": "Analysis failed due to API formatting error. The transcript may require manual review.",
                            "raw_response": result_text[:500] + "..." if len(result_text) > 500 else result_text
                        }
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                
                # Extract API error codes if available
                api_error_code = "unknown"
                if hasattr(e, 'code'):
                    api_error_code = e.code
                elif hasattr(e, 'status_code'):
                    api_error_code = e.status_code
                
                if retry < self.max_retries:
                    backoff = 5 * (retry + 1)
                    logger.warning(f"Error ({error_type}/{api_error_code}), retrying in {backoff}s ({retry+1}/{self.max_retries}): {error_msg}")
                    await asyncio.sleep(backoff)
                else:
                    logger.error(f"Failed after {self.max_retries} attempts: {error_type}/{api_error_code}: {error_msg}")
                    
                    # Return a result dict with error info
                    return {
                        "call_id": call_id,
                        "api_error": f"{error_type} (code: {api_error_code}): {error_msg}",
                        "issue_classification": {
                            "primary_category": "API Error",
                            "specific_issue": f"API failure: {error_type}",
                            "issue_status": "Unresolved",
                            "severity": "High"
                        },
                        "issue_summary": f"The analysis failed due to an API error: {error_type}. The transcript may require manual review."
                    }

# ------------------------------
# Result Formatter
# ------------------------------
@dataclass
class AnalysisResult:
    call_id: str
    call_date: str = ""
    analysis_status: str = "completed"
    api_error: str = ""
    primary_issue_category: str = ""
    specific_issue: str = ""
    issue_status: str = ""
    issue_severity: str = ""
    caller_type: str = ""
    experience_level: str = ""
    caller_intent: str = ""
    system_portal: str = ""
    device_information: str = ""
    error_messages: str = ""
    feature_involved: str = ""
    issue_preconditions: str = ""
    action_sequence: str = ""
    failure_point: str = ""
    expected_vs_actual: str = ""
    issue_frequency: str = ""
    attempted_solutions: str = ""
    resolution_steps: str = ""
    knowledge_gap_identified: str = ""
    issue_description_quote: str = ""
    impact_statement_quote: str = ""
    issue_summary: str = ""
    confidence_score: float = 0.0
    analysis_timestamp: str = ""
    processing_time_ms: float = 0.0
    note: str = ""

class ResultFormatter:
    def __init__(self, text_processor: TextProcessor):
        self.text_processor = text_processor
    
    def format_analysis_result(self, result: Dict[str, Any], file_name: str) -> Dict[str, Any]:
        """Format and normalize analysis results for CSV output"""
        call_date = self.text_processor.extract_date_from_filename(file_name)
        
        # Create base result object
        formatted_result = AnalysisResult(call_id=file_name, call_date=call_date)
        
        # Handle API errors
        if "api_error" in result:
            has_valid_analysis = ("issue_classification" in result or "issue_summary" in result)
            formatted_result.analysis_status = "partial" if has_valid_analysis else "failed"
            formatted_result.api_error = result.get("api_error", "Unknown API error")
            
            if not has_valid_analysis:
                return asdict(formatted_result)
        elif "error" in result:
            formatted_result.analysis_status = "failed"
            formatted_result.api_error = result.get("error", "Unknown error")
            return asdict(formatted_result)
        
        # Extract data safely with defaults for missing fields
        try:
            issue_class = result.get("issue_classification", {})
            caller_info = result.get("caller_information", {})
            tech_context = result.get("technical_context", {})
            issue_recreation = result.get("issue_recreation", {})
            resolution_path = result.get("resolution_path", {})
            key_quotes = result.get("key_quotes", {})
            
            # Update fields if they exist in the result
            formatted_result.primary_issue_category = issue_class.get("primary_category", "")
            formatted_result.specific_issue = issue_class.get("specific_issue", "")
            formatted_result.issue_status = issue_class.get("issue_status", "")
            formatted_result.issue_severity = issue_class.get("severity", "")
            formatted_result.caller_type = caller_info.get("caller_type", "")
            formatted_result.experience_level = caller_info.get("experience_level", "")
            formatted_result.caller_intent = caller_info.get("intent", "")
            formatted_result.system_portal = tech_context.get("system_portal", "")
            formatted_result.device_information = tech_context.get("device_information", "")
            formatted_result.error_messages = tech_context.get("error_messages", "")
            formatted_result.feature_involved = tech_context.get("feature_involved", "")
            formatted_result.issue_preconditions = issue_recreation.get("preconditions", "")
            formatted_result.action_sequence = issue_recreation.get("action_sequence", "")
            formatted_result.failure_point = issue_recreation.get("failure_point", "")
            formatted_result.expected_vs_actual = issue_recreation.get("expected_vs_actual", "")
            formatted_result.issue_frequency = issue_recreation.get("frequency", "")
            formatted_result.attempted_solutions = resolution_path.get("attempted_solutions", "")
            formatted_result.resolution_steps = resolution_path.get("resolution_steps", "")
            formatted_result.knowledge_gap_identified = resolution_path.get("knowledge_gap_identified", "")
            formatted_result.issue_description_quote = key_quotes.get("issue_description", "")
            formatted_result.impact_statement_quote = key_quotes.get("impact_statement", "")
            formatted_result.issue_summary = result.get("issue_summary", "")
            
            # Calculate and add confidence score
            formatted_result.confidence_score = self._calculate_confidence_score(result)
            
            # Add timestamp for analysis completion
            formatted_result.analysis_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add note if present
            if "note" in result:
                formatted_result.note = result["note"]
            
            # Add processing metrics
            if "processing_time" in result:
                formatted_result.processing_time_ms = result["processing_time"]
            
            return {k: v for k, v in asdict(formatted_result).items() if v}
        
        except Exception as e:
            logger.error(f"Error formatting result for {file_name}: {str(e)}")
            return {
                "call_id": file_name,
                "call_date": call_date,
                "analysis_status": "failed",
                "error": f"Error formatting result: {str(e)}"
            }
            
    def _calculate_confidence_score(self, result: Dict[str, Any]) -> float:
        """Calculate a confidence score based on completeness of analysis"""
        try:
            # Define key fields that indicate confident analysis
            key_indicators = [
                result.get("issue_classification", {}).get("primary_category", ""),
                result.get("issue_classification", {}).get("specific_issue", ""),
                result.get("issue_classification", {}).get("severity", ""),
                result.get("technical_context", {}).get("system_portal", ""),
                result.get("technical_context", {}).get("feature_involved", ""),
                result.get("issue_recreation", {}).get("action_sequence", ""),
                result.get("issue_recreation", {}).get("failure_point", ""),
                result.get("issue_recreation", {}).get("expected_vs_actual", ""),
                result.get("key_quotes", {}).get("issue_description", ""),
                result.get("issue_summary", "")
            ]
            
            # Count non-empty fields
            filled_fields = sum(1 for field in key_indicators if field and field.lower() != "not mentioned")
            
            # Calculate percentage (0-100)
            confidence_score = (filled_fields / len(key_indicators)) * 100
            
            # Boost score for detailed fields
            if len(result.get("issue_summary", "").split()) > 50:
                confidence_score += 5
                
            if "step" in result.get("issue_recreation", {}).get("action_sequence", "").lower():
                confidence_score += 5
                
            if result.get("key_quotes", {}).get("issue_description", "") and \
               result.get("key_quotes", {}).get("impact_statement", ""):
                confidence_score += 5
                
            # Cap at 100
            return min(100.0, confidence_score)
            
        except Exception as e:
            logger.warning(f"Error calculating confidence score: {str(e)}")
            return 0.0

# ------------------------------
# Data Manager
# ------------------------------
class DataManager:
    def __init__(self, transcriptions_path: str, analysis_path: str):
        self.transcriptions_path = transcriptions_path
        self.analysis_path = analysis_path
        self.date_based_path = self._generate_date_based_path(analysis_path)
    
    def _generate_date_based_path(self, base_path: str) -> str:
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
    
    def load_transcriptions(self) -> Optional[pd.DataFrame]:
        """Load transcriptions from CSV file"""
        if not os.path.exists(self.transcriptions_path):
            logger.error(f"Transcriptions file {self.transcriptions_path} not found")
            return None
        
        try:
            df = pd.read_csv(self.transcriptions_path)
            logger.info(f"Loaded {len(df)} transcriptions from {self.transcriptions_path}")
            
            # Log additional info about the data
            valid_transcripts = df[df['transcription'].notna() & 
                                 (df['transcription'].astype(str).str.strip() != "") & 
                                 (~df['transcription'].astype(str).str.startswith("ERROR:"))].shape[0]
            
            logger.info(f"Found {valid_transcripts} valid transcriptions out of {len(df)} total")
            
            return df
        except Exception as e:
            logger.error(f"Error loading transcriptions: {str(e)}")
            return None
    
    def load_analysis_results(self) -> pd.DataFrame:
        """Load existing analysis results"""
        if not os.path.exists(self.analysis_path):
            logger.info(f"No existing analysis file found at {self.analysis_path}")
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(self.analysis_path)
            logger.info(f"Loaded {len(df)} existing analyses")
            
            # Log status breakdown
            if not df.empty and 'analysis_status' in df.columns:
                status_counts = df['analysis_status'].value_counts().to_dict()
                logger.info(f"Analysis status breakdown: {status_counts}")
            
            self._create_backup()
            return df
        except Exception as e:
            logger.error(f"Error loading existing analyses: {str(e)}")
            return pd.DataFrame()
    
    def _create_backup(self):
        """Create backup of existing analysis file"""
        backup_file = f"{self.analysis_path}.bak"
        try:
            import shutil
            shutil.copy2(self.analysis_path, backup_file)
            logger.info(f"Created backup at {backup_file}")
        except Exception as e:
            logger.warning(f"Failed to create backup: {str(e)}")
    
    def save_analysis_results(self, df: pd.DataFrame):
        """Save analysis results to CSV"""
        try:
            # Save to original path
            df.to_csv(self.analysis_path, index=False)
            logger.info(f"Saved analysis results ({len(df)} items) to {self.analysis_path}")
            
            # Save to date-based path
            df.to_csv(self.date_based_path, index=False)
            logger.info(f"Saved date-based copy to {self.date_based_path}")
            
            # Log quality metrics if confidence score exists
            if 'confidence_score' in df.columns:
                avg_confidence = df['confidence_score'].mean()
                high_confidence = df[df['confidence_score'] >= 80].shape[0]
                low_confidence = df[df['confidence_score'] < 50].shape[0]
                logger.info(f"Analysis quality metrics: Avg confidence: {avg_confidence:.2f}, " 
                           f"High confidence: {high_confidence} ({high_confidence/len(df)*100:.1f}%), "
                           f"Low confidence: {low_confidence} ({low_confidence/len(df)*100:.1f}%)")
                
        except Exception as e:
            logger.error(f"Error saving analysis results: {str(e)}")
            
    def get_date_based_path(self) -> str:
        """Return the date-based file path"""
        return self.date_based_path

# ------------------------------
# Call Analysis Service
# ------------------------------
class CallAnalysisService:
    def __init__(self, config: Config):
        self.config = config
        self.text_processor = TextProcessor()
        self.result_formatter = ResultFormatter(self.text_processor)
        self.category_manager = CategoryManager(config.categories_file)
        self.openai_client = OpenAIClient(
            model=config.openai_model,
            max_retries=config.max_retries
        )
        self.data_manager = DataManager(
            transcriptions_path=config.transcriptions_csv,
            analysis_path=config.analysis_csv
        )
        self.batch_size = config.batch_size
        self.graceful_exit = GracefulExit()
    
    def prepare_batch_prompts(self, transcriptions: List[Dict[str, Any]]) -> Tuple[List[List[Dict[str, str]]], List[Dict[str, Any]]]:
        """Prepare prompts for each valid transcript"""
        messages_list = []
        valid_items = []
        
        for item in transcriptions:
            # Skip invalid or empty transcriptions
            if not item.get('transcription') or not isinstance(item['transcription'], str) or item['transcription'].startswith("ERROR:"):
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
    
    async def analyze_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
            start_time = time.time()
            
            # Process the transcript
            result = await self.openai_client.analyze_transcript(messages, item['file_name'])
            
            # Add processing time information
            processing_time = (time.time() - start_time) * 1000  # convert to ms
            result["processing_time"] = processing_time
            
            # Add note if using partial transcript
            chunks = self.text_processor.chunk_text(item['transcription'])
            if len(chunks) > 1:
                result["note"] = f"Analysis based on partial transcription ({len(chunks[0])}/{len(item['transcription'])} chars)"
            
            # Output completion status with error handling
            if "api_error" in result:
                print(f"⚠️  Partial analysis for {item['file_name']}: API error but some data recovered")
            elif "error" in result:
                print(f"❌ Failed: {item['file_name']} - {result.get('error')[:50]}...")
            else:
                # Calculate confidence score
                confidence = self.result_formatter._calculate_confidence_score(result)
                
                # Use emoji based on confidence
                emoji = "✅" if confidence >= 80 else "⚠️" if confidence >= 50 else "❓"
                
                # Get the primary issue if available
                primary_issue = result.get("issue_classification", {}).get("primary_category", "Unknown")
                specific_issue = result.get("issue_classification", {}).get("specific_issue", "")
                
                # Print status with confidence score
                print(f"{emoji} {item['file_name']} analyzed (confidence: {confidence:.1f}%) - {primary_issue}: {specific_issue[:40]}")
            
            results.append(result)
        
        # Clear the progress line
        print(" " * 100, end="\r")
        
        return results
    
    def filter_transcriptions(self, transcriptions_df: pd.DataFrame, analysis_df: pd.DataFrame, reanalyze: bool) -> List[Dict[str, Any]]:
        """Filter transcriptions that need to be analyzed"""
        transcriptions_to_analyze = []
        
        for _, row in transcriptions_df.iterrows():
            # Skip invalid transcriptions
            if not isinstance(row['transcription'], str) or not row['transcription'].strip() or row['transcription'].startswith("ERROR:"):
                continue
            
            file_name = row['file_name']
            
            # Skip if already successfully analyzed (unless reanalyze=True)
            if not reanalyze and not analysis_df.empty and file_name in analysis_df['call_id'].values:
                status = analysis_df[analysis_df['call_id'] == file_name]['analysis_status'].iloc[0]
                if status == "completed":
                    continue
            
            transcriptions_to_analyze.append(row.to_dict())
        
        return transcriptions_to_analyze
    
    async def process_transcriptions(self, transcriptions_df: pd.DataFrame, reanalyze: bool = False) -> pd.DataFrame:
        """Process all transcriptions in batches"""
        
        # Load existing analysis if available
        analysis_df = self.data_manager.load_analysis_results()
        
        # Filter transcriptions to analyze
        transcriptions_to_analyze = self.filter_transcriptions(transcriptions_df, analysis_df, reanalyze)
        
        if not transcriptions_to_analyze:
            logger.info("No transcriptions to analyze")
            return analysis_df
        
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
            
            # Format and save results from this batch
            formatted_results = []
            for result in batch_results:
                file_name = result.get("call_id", "unknown")
                formatted_result = self.result_formatter.format_analysis_result(result, file_name)
                formatted_results.append(formatted_result)
                
                # Log results
                if formatted_result["analysis_status"] == "completed":
                    logger.info(f"✓ Analyzed: {file_name}")
                else:
                    logger.warning(f"✗ Failed: {file_name} - {formatted_result.get('error', 'Unknown error')}")
            
            # Combine with existing results and save
            if formatted_results:
                batch_df = pd.DataFrame(formatted_results)
                
                # Update existing DataFrame
                if not analysis_df.empty:
                    # Remove existing entries for these call_ids
                    call_ids = batch_df['call_id'].values
                    analysis_df = analysis_df[~analysis_df['call_id'].isin(call_ids)]
                
                # Combine and save
                analysis_df = pd.concat([analysis_df, batch_df], ignore_index=True)
                self.data_manager.save_analysis_results(analysis_df)
        
        return analysis_df
    
    async def run(self, reanalyze: bool = False, min_confidence: float = 0, retry_low_confidence: bool = False, dry_run: bool = False, verbose: bool = False):
        """Main entry point to run the analysis process"""
        logger.info("=" * 50)
        logger.info("CALL ANALYZER".center(50))
        logger.info("=" * 50)
        
        if reanalyze:
            logger.info("REANALYSIS MODE: Analyzing all calls")
        
        # Load transcriptions file
        transcriptions_df = self.data_manager.load_transcriptions()
        if transcriptions_df is None:
            return
        
        # Process all transcriptions
        await self.process_transcriptions(transcriptions_df, reanalyze)
        
        logger.info("=" * 50)
        logger.info("PROCESS COMPLETE".center(50))
        logger.info("=" * 50)

# ------------------------------
# Command Line Interface
# ------------------------------
@dataclass
class CommandLineArgs:
    transcriptions: str = 'call_transcriptions.csv'
    output: str = 'call_analysis_results.csv'
    categories: str = 'categories.csv'
    model: str = 'gpt-4o'
    max_retries: int = 3
    batch_size: int = 10
    max_concurrent: int = 3
    reanalyze: bool = False
    verbose: bool = False
    dry_run: bool = False
    min_confidence: float = 0.0
    retry_low_confidence: bool = False

class CommandLineInterface:
    @staticmethod
    def parse_arguments() -> CommandLineArgs:
        """Parse command line arguments"""
        import argparse
        
        parser = argparse.ArgumentParser(
            description='Call Center Transcript Analysis System',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        
        # Input/output files
        file_group = parser.add_argument_group('File Configuration')
        file_group.add_argument('--transcriptions', type=str, default='call_transcriptions.csv',
                               help='Path to transcriptions CSV file')
        file_group.add_argument('--output', type=str, default='call_analysis_results.csv',
                               help='Path for analysis results CSV file')
        file_group.add_argument('--categories', type=str, default='categories.csv',
                               help='Path to categories CSV file')
        
        # API and model settings
        api_group = parser.add_argument_group('API Configuration')
        api_group.add_argument('--model', type=str, default='gpt-4o',
                              help='OpenAI model to use (gpt-4o, gpt-4, etc.)')
        api_group.add_argument('--max-retries', type=int, default=3,
                              help='Maximum number of API retries on failure')
        
        # Processing settings
        process_group = parser.add_argument_group('Processing Options')
        process_group.add_argument('--batch-size', type=int, default=10,
                              help='Number of transcripts to process in each batch')
        process_group.add_argument('--max-concurrent', type=int, default=3,
                              help='Maximum number of concurrent API calls')
        process_group.add_argument('--reanalyze', action='store_true',
                                help='Reanalyze all calls, including those already processed')
        process_group.add_argument('--verbose', action='store_true',
                                help='Display detailed progress information during processing')
        process_group.add_argument('--dry-run', action='store_true',
                                help='List transcripts that would be analyzed but do not perform analysis')
        
        # Quality threshold option
        quality_group = parser.add_argument_group('Analysis Quality Options')
        quality_group.add_argument('--min-confidence', type=float, default=0,
                                 help='Minimum confidence score (0-100) required for analysis to be considered valid')
        quality_group.add_argument('--retry-low-confidence', action='store_true',
                                 help='Automatically retry analyses with confidence scores below threshold')
        
        args = parser.parse_args()
        return CommandLineArgs(
            transcriptions=args.transcriptions,
            output=args.output,
            categories=args.categories,
            model=args.model,
            max_retries=args.max_retries,
            batch_size=args.batch_size,
            max_concurrent=args.max_concurrent,
            reanalyze=args.reanalyze,
            verbose=args.verbose,
            dry_run=args.dry_run,
            min_confidence=args.min_confidence,
            retry_low_confidence=args.retry_low_confidence
        )

# ------------------------------
# Main Application
# ------------------------------
async def main():
    """Entry point for command line usage"""
    try:
        # Parse command line arguments
        args = CommandLineInterface.parse_arguments()
        
        # Create configuration
        config = Config(
            transcriptions_csv=args.transcriptions,
            analysis_csv=args.output,
            categories_file=args.categories,
            openai_model=args.model,
            batch_size=args.batch_size,
            max_concurrent=args.max_concurrent,
            max_retries=args.max_retries
        )
        
        # Display startup banner
        print("\n" + "=" * 70)
        print(" CALL CENTER TRANSCRIPT ANALYSIS SYSTEM ".center(70, "="))
        print("=" * 70)
        
        # Log system information
        logger.info("System started")
        logger.info(f"Python version: {sys.version.split()[0]}")
        logger.info(f"Operating system: {sys.platform}")
        
        # Log configuration details
        logger.info("\nCONFIGURATION:")
        logger.info(f"Transcriptions CSV: {config.transcriptions_csv}")
        logger.info(f"Analysis output:    {config.analysis_csv}")
        logger.info(f"Categories file:    {config.categories_file}")
        logger.info(f"OpenAI model:       {config.openai_model}")
        logger.info(f"Batch size:         {config.batch_size}")
        logger.info(f"Max concurrent:     {config.max_concurrent}")
        logger.info(f"Max retries:        {config.max_retries}")
        logger.info(f"Reanalyze all:      {args.reanalyze}")
        logger.info(f"Min confidence:     {args.min_confidence}")
        logger.info(f"Retry low conf:     {args.retry_low_confidence}")
        logger.info(f"Dry run:            {args.dry_run}")
        logger.info(f"Verbose:            {args.verbose}")
        
        # Create a progress display for the console
        print("\n📊 Call Center Analysis Task Details:")
        print(f"📁 Processing transcripts from: {config.transcriptions_csv}")
        print(f"🔍 Using AI model: {config.openai_model}")
        print(f"⚙️  Batch size: {config.batch_size}")
        
        # Show mode information
        mode_indicators = []
        if args.reanalyze:
            mode_indicators.append("⚠️  REANALYSIS MODE (processing all transcripts)")
        if args.dry_run:
            mode_indicators.append("🔍 DRY RUN MODE (no actual analysis)")
        if args.min_confidence > 0:
            mode_indicators.append(f"📏 Quality threshold: {args.min_confidence}% minimum confidence")
        if args.retry_low_confidence:
            mode_indicators.append("🔄 Will retry low-confidence analyses")
            
        if mode_indicators:
            print("\nSpecial processing modes:")
            for indicator in mode_indicators:
                print(f"- {indicator}")
        else:
            print("ℹ️  Running in standard mode (only new/failed transcripts)")
        
        print("\nStarting analysis...\n")
        
        # Create progress tracking variables
        start_time = time.time()
        
        # Run the analyzer
        analyzer = CallAnalysisService(config)
        await analyzer.run(
            reanalyze=args.reanalyze,
            min_confidence=args.min_confidence,
            retry_low_confidence=args.retry_low_confidence,
            dry_run=args.dry_run,
            verbose=args.verbose
        )
        
        # Calculate and display execution statistics
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        # Display completion banner
        print("\n" + "=" * 70)
        print(" ANALYSIS COMPLETE ".center(70, "="))
        print(f" Time elapsed: {minutes} minutes, {seconds} seconds ".center(70))
        
        # Get path information from the analyzer
        data_manager = analyzer.data_manager
        date_path = data_manager.get_date_based_path()
        
        print(f"\n✅ Analysis completed successfully!")
        print(f"📄 Results saved to: {config.analysis_csv}")
        print(f"📑 Date-based copy:  {date_path}")
        
        # Display processing summary
        logger.info(f"Total execution time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        logger.info(f"Date-based results saved to: {date_path}")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Display error in console too
        print("\n❌ ERROR: The analysis process encountered a problem:")
        print(f"   {str(e)}")
        print("\nCheck the log file for details: call_analysis.log")

# ------------------------------
# Token Counter
# ------------------------------
def count_tokens(text: str) -> int:
    """
    Estimate the number of tokens in a text string.
    This is a simple approximation - for precise counting, use tiktoken library.
    """
    # Average tokens per word for English text
    words = text.split()
    return int(len(words) * 1.3)  # 1.3 is an approximation factor

def analyze_token_usage() -> Dict[str, Any]:
    """Analyze the token usage of each component"""
    # Get the source code
    with open(__file__, 'r') as f:
        source = f.read()
    
    # Define sections to analyze
    sections = {
        "Logging": "# ------------------------------\n# Logging Configuration",
        "API Client": "# ------------------------------\n# API Client Configuration",
        "Text Processing": "# ------------------------------\n# Text Processing",
        "Category Management": "# ------------------------------\n# Category Management",
        "Prompt Generation": "# ------------------------------\n# Prompt Generation",
        "OpenAI API": "# ------------------------------\n# OpenAI API Interface",
        "Result Formatter": "# ------------------------------\n# Result Formatter",
        "Data Manager": "# ------------------------------\n# Data Manager",
        "Analysis Service": "# ------------------------------\n# Call Analysis Service",
        "CLI": "# ------------------------------\n# Command Line Interface",
        "Main": "# ------------------------------\n# Main Application"
    }
    
    # Extract and count tokens for each section
    token_counts = {}
    total_tokens = count_tokens(source)
    
    for name, section_start in sections.items():
        start_idx = source.find(section_start)
        if start_idx == -1:
            token_counts[name] = 0
            continue
            
        # Find the start of the next section
        next_sections = [source.find(s) for s in sections.values() if source.find(s) > start_idx]
        end_idx = min(next_sections) if next_sections else len(source)
        
        # Extract section text and count tokens
        section_text = source[start_idx:end_idx]
        token_counts[name] = count_tokens(section_text)
    
    # Compute percentages
    for name in token_counts:
        token_counts[name] = {
            "tokens": token_counts[name],
            "percentage": round(token_counts[name] / total_tokens * 100, 1)
        }
    
    # Add total
    token_counts["Total"] = {"tokens": total_tokens, "percentage": 100.0}
    
    return token_counts

if __name__ == "__main__":
    if "--token-analysis" in sys.argv:
        token_analysis = analyze_token_usage()
        print("\nToken Usage Analysis:")
        print("=" * 50)
        for section, data in token_analysis.items():
            print(f"{section.ljust(20)}: {data['tokens']} tokens ({data['percentage']}%)")
        print("=" * 50)
    else:
        asyncio.run(main())