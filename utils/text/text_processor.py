#!/usr/bin/env python3
"""
Text Processing Utilities
Provides functions for text manipulation and extraction.
"""

import re
import logging
from typing import List, Optional
from functools import lru_cache

# Configure logging
logger = logging.getLogger(__name__)

class TextProcessor:
    """Utilities for processing text data"""
    
    @staticmethod
    def chunk_text(text: str, max_length: int = 8000) -> List[str]:
        """
        Split long text into chunks for API processing
        
        Args:
            text: Text to chunk
            max_length: Maximum length for each chunk
            
        Returns:
            List of text chunks
        """
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
        """
        Extract date from filename pattern with caching
        
        Args:
            file_name: Filename to extract date from
            
        Returns:
            Extracted date as string in YYYY-MM-DD format or empty string if not found
        """
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
    
    @staticmethod
    def clean_text(text: str) -> str:
        """
        Clean and normalize text
        
        Args:
            text: Text to clean
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
            
        # Replace multiple spaces with a single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        return text
    
    @staticmethod
    def extract_phone_number(text: str) -> Optional[str]:
        """
        Extract phone number from text
        
        Args:
            text: Text to extract phone number from
            
        Returns:
            Phone number or None if not found
        """
        try:
            # Look for patterns like +91XXXXXXXXXX or 0XXXXXXXXXX
            patterns = [
                r'\+\d{12}',  # +91XXXXXXXXXX
                r'\+\d{2}\s?\d{10}',  # +91 XXXXXXXXXX
                r'\d{10}',  # XXXXXXXXXX (10 digits)
                r'\d{3}[-\s]?\d{3}[-\s]?\d{4}'  # XXX-XXX-XXXX or XXX XXX XXXX
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    return match.group(0)
                    
            return None
        except Exception as e:
            logger.error(f"Error extracting phone number: {str(e)}")
            return None 