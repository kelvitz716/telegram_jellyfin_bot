#file_processing/media_identifier.py:

import re
import os
from typing import Dict, Any, Optional, Tuple

class MediaFilenameParser:
    """
    Advanced media filename parsing utility.
    Extracts metadata from various media filename formats.
    """
    
    TV_SHOW_PATTERNS = [
        r'(?P<show>.*?)[. _-]S(?P<season>\d{1,2})E(?P<episode>\d{1,2})',  # S01E01
        r'(?P<show>.*?)[. _-](?P<season>\d{1,2})x(?P<episode>\d{1,2})',   # 1x01
        r'(?P<show>.*?)[. _-](?:season|s)?\s?(?P<season>\d{1,2})\s?(?:episode|e)?\s?(?P<episode>\d{1,2})',  # Season 1 Episode 1
    ]
    
    MOVIE_PATTERNS = [
        r'(?P<title>.*?)[. _-](?P<year>\d{4})',  # Movie Title 2023
        r'(?P<title>.*?)\((?P<year>\d{4})\)',    # Movie Title (2023)
    ]
    
    @classmethod
    def parse_filename(cls, filename: str) -> Dict[str, Any]:
        """
        Parse media filename and extract metadata.
        
        Args:
            filename: Media filename
            
        Returns:
            Extracted metadata dictionary
        """
        # Remove file extension
        filename_no_ext = os.path.splitext(filename)[0]
        
        # Try TV show patterns first
        for pattern in cls.TV_SHOW_PATTERNS:
            match = re.search(pattern, filename_no_ext, re.IGNORECASE)
            if match:
                return {
                    'type': 'tv_show',
                    'show_name': cls._clean_name(match.group('show')),
                    'season': int(match.group('season')),
                    'episode': int(match.group('episode'))
                }
        
        # Try movie patterns
        for pattern in cls.MOVIE_PATTERNS:
            match = re.search(pattern, filename_no_ext, re.IGNORECASE)
            if match:
                return {
                    'type': 'movie',
                    'title': cls._clean_name(match.group('title')),
                    'year': int(match.group('year'))
                }
        
        # Fallback: return cleaned filename
        return {
            'type': 'unknown',
            'name': cls._clean_name(filename_no_ext)
        }
    
    @staticmethod
    def _clean_name(name: str) -> str:
        """
        Clean and normalize media name.
        
        Args:
            name: Raw media name
            
        Returns:
            Cleaned media name
        """
        # Replace periods and underscores with spaces
        name = name.replace('.', ' ').replace('_', ' ')
        
        # Remove special characters and extra whitespaces
        name = re.sub(r'[^\w\s()]', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name