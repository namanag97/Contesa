#!/usr/bin/env python3
"""
Client for interacting with ElevenLabs Speech-to-Text API.
"""

import os
import logging
import time
import json
try:
    import requests
except ImportError:
    print("Error: Requests package not installed. Please install it using: pip install requests>=2.31.0")
    requests = None
from typing import Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

class ElevenLabsClient:
    """
    Client for ElevenLabs Speech-to-Text API
    """
    
    def __init__(self, api_key: str = None, base_url: str = "https://api.elevenlabs.io/v1"):
        """
        Initialize the ElevenLabs client
        
        Args:
            api_key: ElevenLabs API key (if None, will use from environment)
            base_url: Base URL for the API
        """
        if requests is None:
            raise ImportError("The requests package is required. Please install it using: pip install requests>=2.31.0")
            
        self.api_key = api_key or os.environ.get("ELEVENLABS_API_KEY")
        if not self.api_key:
            logger.warning("ElevenLabs API key not provided")
            
        self.base_url = base_url
        logger.info("ElevenLabs client initialized")
    
    def get_headers(self) -> Dict[str, str]:
        """
        Get API headers
        
        Returns:
            Headers dictionary
        """
        return {
            "xi-api-key": self.api_key,
            "Accept": "application/json"
        }
    
    def transcribe_audio(self, audio_data: bytes, 
                         language_code: str = "en", 
                         model_id: str = "scribe_v1") -> Dict[str, Any]:
        """
        Transcribe audio to text
        
        Args:
            audio_data: Binary audio data
            language_code: Language code for transcription
            model_id: Model ID to use for transcription
            
        Returns:
            Transcription results dictionary
        """
        url = f"{self.base_url}/speech-to-text"
        
        try:
            files = {
                "file": ("audio.aac", audio_data)
            }
            
            data = {
                "language_code": language_code,
                "model_id": model_id,
                "speaker_count": 2  # Assume 2 speakers for call center conversations
            }
            
            start_time = time.time()
            logger.info(f"Starting transcription with model {model_id}")
            
            response = requests.post(
                url,
                headers=self.get_headers(),
                files=files,
                data=data,
                timeout=300  # 5 minute timeout for longer files
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"Transcription request completed in {elapsed_time:.2f} seconds")
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Transcription successful: {len(result.get('text', ''))} characters")
                return result
            else:
                error_message = f"Transcription failed: HTTP {response.status_code}"
                try:
                    error_data = response.json()
                    if "detail" in error_data:
                        error_message += f" - {error_data['detail']}"
                except Exception:
                    error_message += f" - {response.text[:100]}..."
                
                logger.error(error_message)
                return {
                    "status": "error",
                    "error": error_message
                }
                
        except requests.exceptions.Timeout:
            logger.error("Transcription request timed out")
            return {
                "status": "error",
                "error": "Request timed out after 5 minutes"
            }
        except Exception as e:
            logger.error(f"Transcription error: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def get_transcription_models(self) -> Dict[str, Any]:
        """
        Get available transcription models
        
        Returns:
            Dictionary with available models
        """
        url = f"{self.base_url}/speech-to-text/models"
        
        try:
            response = requests.get(
                url,
                headers=self.get_headers(),
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get transcription models: HTTP {response.status_code}")
                return {"status": "error", "error": f"HTTP {response.status_code}"}
                
        except Exception as e:
            logger.error(f"Error getting transcription models: {str(e)}")
            return {"status": "error", "error": str(e)} 