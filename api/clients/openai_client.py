#!/usr/bin/env python3
"""
Client for interacting with OpenAI API to analyze call transcriptions.
"""

import os
import json
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional, Union
try:
    import openai
    from openai import AsyncOpenAI
except ImportError:
    print("Error: OpenAI package not installed. Please install it using: pip install openai>=1.0.0")

logger = logging.getLogger(__name__)

class OpenAIClient:
    """
    Client for OpenAI API interactions
    """
    
    def __init__(self, api_key: str = None, model: str = "gpt-4-turbo", 
                 max_retries: int = 3, timeout: int = 60, 
                 rate_limit_rpm: int = 3):
        """
        Initialize the OpenAI client
        
        Args:
            api_key: OpenAI API key (if None, will use from environment)
            model: Model to use for analysis
            max_retries: Maximum retry attempts
            timeout: API timeout in seconds
            rate_limit_rpm: Rate limit in requests per minute
        """
        self.model = model
        self.max_retries = max_retries
        self.timeout = timeout
        self.rate_limit_rpm = rate_limit_rpm
        self.min_seconds_between_calls = 60.0 / rate_limit_rpm
        self.last_call_time = 0
        
        # Initialize client
        try:
            self.client = AsyncOpenAI(api_key=api_key, timeout=timeout)
            logger.info(f"OpenAI client initialized with model: {model}")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {str(e)}")
            raise
        
        # Token cost estimates (update these based on actual model pricing)
        self.token_costs = {
            "gpt-4-turbo": {"input": 0.00001, "output": 0.00003},
            "gpt-4": {"input": 0.00003, "output": 0.00006},
            "gpt-3.5-turbo": {"input": 0.0000015, "output": 0.000002}
        }
    
    async def _rate_limit(self):
        """Apply rate limiting to API calls"""
        now = time.time()
        time_since_last_call = now - self.last_call_time
        
        if time_since_last_call < self.min_seconds_between_calls:
            delay = self.min_seconds_between_calls - time_since_last_call
            logger.debug(f"Rate limiting: waiting {delay:.2f} seconds")
            await asyncio.sleep(delay)
        
        self.last_call_time = time.time()
    
    def _calculate_cost(self, token_usage: Dict[str, int]) -> float:
        """
        Calculate the cost of API call based on token usage
        
        Args:
            token_usage: Token usage data from API response
            
        Returns:
            Estimated cost in USD
        """
        # Get token costs for the model, fallback to gpt-4-turbo if not found
        costs = self.token_costs.get(self.model, self.token_costs["gpt-4-turbo"])
        
        input_tokens = token_usage.get("prompt_tokens", 0)
        output_tokens = token_usage.get("completion_tokens", 0)
        
        input_cost = input_tokens * costs["input"]
        output_cost = output_tokens * costs["output"]
        
        return input_cost + output_cost
    
    def _parse_json_response(self, text: str) -> Dict[str, Any]:
        """
        Parse JSON from the response text
        
        Args:
            text: Response text to parse
            
        Returns:
            Parsed JSON as dictionary
        """
        # Strip any non-JSON prefixes/suffixes
        text = text.strip()
        
        # Find the first { and last } to extract JSON
        start = text.find('{')
        end = text.rfind('}')
        
        if start != -1 and end != -1 and end > start:
            json_str = text[start:end+1]
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing error: {str(e)}")
                return {"api_error": f"JSON parsing error: {str(e)}"}
        else:
            logger.error("No valid JSON found in response")
            return {"api_error": "No valid JSON found in response"}
    
    async def analyze_transcript(self, messages: List[Dict[str, str]], 
                                 call_id: str) -> Dict[str, Any]:
        """
        Analyze a call transcript
        
        Args:
            messages: List of message dictionaries
            call_id: ID of the call being analyzed
            
        Returns:
            Analysis results
        """
        result = {"call_id": call_id}
        
        for attempt in range(self.max_retries):
            try:
                # Apply rate limiting
                await self._rate_limit()
                
                # Make API call
                logger.info(f"Sending analysis request for call {call_id} (attempt {attempt+1}/{self.max_retries})")
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.1,  # Lower temperature for more consistent results
                    max_tokens=4000,  # Adjust as needed for your response size
                    request_timeout=self.timeout
                )
                
                # Extract content
                if response.choices and len(response.choices) > 0:
                    content = response.choices[0].message.content
                    
                    # Parse the response as JSON
                    analysis_result = self._parse_json_response(content)
                    
                    # Add token usage and cost information
                    if hasattr(response, 'usage'):
                        result["token_usage"] = {
                            "prompt_tokens": response.usage.prompt_tokens,
                            "completion_tokens": response.usage.completion_tokens,
                            "total_tokens": response.usage.total_tokens
                        }
                        result["cost"] = self._calculate_cost(result["token_usage"])
                    
                    # Merge the analysis result with our result object
                    result.update(analysis_result)
                    return result
                else:
                    logger.warning(f"Empty response for call {call_id}")
                    continue
                
            except (openai.RateLimitError, openai.APITimeoutError) as e:
                wait_time = (2 ** attempt) + 1  # Exponential backoff
                logger.warning(f"API error on attempt {attempt+1}, waiting {wait_time}s: {str(e)}")
                await asyncio.sleep(wait_time)
                continue
                
            except Exception as e:
                logger.error(f"Error analyzing call {call_id} on attempt {attempt+1}: {str(e)}")
                if attempt == self.max_retries - 1:
                    # Last attempt failed, return error
                    result["api_error"] = str(e)
                    return result
                continue
        
        # If we get here, all retries failed
        result["api_error"] = f"Failed after {self.max_retries} attempts"
        return result 