import os
import re
import json
import shutil
import requests
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("media_categorizer.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configuration
TMDB_API_KEY = "YOUR_TMDB_API_KEY"  # Get from https://www.themoviedb.org/settings/api
TELEGRAM_DOWNLOAD_DIR = "/path/to/telegram/downloads"
MOVIES_DIR = "/path/to/jellyfin/movies"
TV_SHOWS_DIR = "/path/to/jellyfin/tvshows"
UNMATCHED_DIR = "/path/to/unmatched"  # For files that couldn't be categorized

# Regular expressions for initial categorization
TV_SHOW_PATTERNS = [
    r'[Ss](\d{1,2})[Ee](\d{1,2})',  # S01E01 format
    r'[. _-](\d{1,2})x(\d{1,2})[. _-]',  # 1x01 format
    r'[. _]([Ee]pisode[. _])(\d{1,2})[. _]',  # Episode 01
    r'Season[. _](\d{1,2})[. _]Episode[. _](\d{1,2})',  # Season 1 Episode 01
]

MOVIE_YEAR_PATTERN = r'(.*?)[. _](\d{4})[. _]'  # Movie Name 2023 pattern

VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.m4v', '.flv']

def is_video_file(filename: str) -> bool:
    """Check if file has a video extension."""
    return os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS

def initial_categorization(filename: str) -> Tuple[str, Dict]:
    """
    Initial categorization using regex patterns.
    Returns: ("tv" or "movie" or "unknown", metadata_dict)
    """
    basename = os.path.basename(filename)
    
    # Check TV show patterns
    for pattern in TV_SHOW_PATTERNS:
        match = re.search(pattern, basename)
        if match:
            # Extract show name (everything before the pattern)
            show_part = re.split(pattern, basename)[0].strip()
            # Clean up show name
            show_name = re.sub(r'[._]', ' ', show_part).strip()
            
            return "tv", {
                "show_name": show_name,
                "season": int(match.group(1)),
                "episode": int(match.group(2))
            }
    
    # Check movie year pattern
    movie_match = re.search(MOVIE_YEAR_PATTERN, basename)
    if movie_match:
        movie_name = re.sub(r'[._]', ' ', movie_match.group(1)).strip()
        year = movie_match.group(2)
        return "movie", {"movie_name": movie_name, "year": year}
    
    # If no pattern matched, clean the name and return unknown
    clean_name = re.sub(r'[._]', ' ', os.path.splitext(basename)[0]).strip()
    return "unknown", {"name": clean_name}

def verify_with_tmdb(media_type: str, metadata: Dict) -> Tuple[str, Dict]:
    """
    Verify and enhance metadata using TMDb API.
    Returns: (confirmed_type, enhanced_metadata)
    """
    if media_type == "tv":
        return verify_tv_show(metadata)
    elif media_type == "movie":
        return verify_movie(metadata)
    else:
        return guess_media_type(metadata)

def verify_tv_show(metadata: Dict) -> Tuple[str, Dict]:
    """Verify a TV show with TMDb API."""
    show_name = metadata.get("show_name", "")
    
    url = f"https://api.themoviedb.org/3/search/tv"
    params = {
        "api_key": TMDB_API_KEY,
        "query": show_name,
        "page": 1
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get("results", [])
        
        if results:
            # Get the first (most relevant) result
            show = results[0]
            enhanced_metadata = {
                "show_name": show["name"],
                "tmdb_id": show["id"],
                "season": metadata.get("season"),
                "episode": metadata.get("episode"),
                "first_air_date": show.get("first_air_date"),
                "overview": show.get("overview")
            }
            return "tv", enhanced_metadata
        else:
            # No results from TMDb
            logger.warning(f"No TV show match found for: {show_name}")
            return "unknown", metadata
            
    except Exception as e:
        logger.error(f"Error verifying TV show: {str(e)}")
        return "tv", metadata  # Return original type and metadata on error

def verify_movie(metadata: Dict) -> Tuple[str, Dict]:
    """Verify a movie with TMDb API."""
    movie_name = metadata.get("movie_name", "")
    year = metadata.get("year")
    
    url = f"https://api.themoviedb.org/3/search/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "query": movie_name,
        "year": year,
        "page": 1
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get("results", [])
        
        if results:
            # Get the first (most relevant) result
            movie = results[0]
            enhanced_metadata = {
                "movie_name": movie["title"],
                "tmdb_id": movie["id"],
                "year": movie.get("release_date", "")[:4] if movie.get("release_date") else year,
                "overview": movie.get("overview")
            }
            return "movie", enhanced_metadata
        else:
            # No results from TMDb
            logger.warning(f"No movie match found for: {movie_name} ({year})")
            return "unknown", metadata
            
    except Exception as e:
        logger.error(f"Error verifying movie: {str(e)}")
        return "movie", metadata  # Return original type and metadata on error

def guess_media_type(metadata: Dict) -> Tuple[str, Dict]:
    """
    Try to guess media type for unknown files using TMDb API.
    First try movie search, then TV search.
    """
    name = metadata.get("name", "")
    
    # Try movie search first
    url = f"https://api.themoviedb.org/3/search/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "query": name,
        "page": 1
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get("results", [])
        
        if results:
            # Get the first result
            movie = results[0]
            enhanced_metadata = {
                "movie_name": movie["title"],
                "tmdb_id": movie["id"],
                "year": movie.get("release_date", "")[:4] if movie.get("release_date") else "",
                "overview": movie.get("overview")
            }
            return "movie", enhanced_metadata
    except Exception as e:
        logger.error(f"Error in movie search for unknown media: {str(e)}")
    
    # If movie search fails, try TV search
    url = f"https://api.themoviedb.org/3/search/tv"
    params = {
        "api_key": TMDB_API_KEY,
        "query": name,
        "page": 1
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get("results", [])
        
        if results:
            # Get the first result
            show = results[0]
            # We don't have season/episode info, so set defaults
            enhanced_metadata = {
                "show_name": show["name"],
                "tmdb_id": show["id"],
                "season": 1,  # Default season
                "episode": 1,  # Default episode
                "first_air_date": show.get("first_air_date"),
                "overview": show.get("overview"),
                "needs_manual_check": True  # Flag for manual verification
            }
            return "tv", enhanced_metadata
    except Exception as e:
        logger.error(f"Error in TV search for unknown media: {str(e)}")
    
    # If both searches fail, return unknown
    return "unknown", metadata

def format_filename(media_type: str, metadata: Dict) -> str:
    """Format filename according to Jellyfin naming conventions."""
    if media_type == "tv":
        show_name = metadata.get("show_name", "Unknown Show")
        season = metadata.get("season", 1)
        episode = metadata.get("episode", 1)
        
        # Format: ShowName/Season 01/ShowName - S01E01.ext
        return f"{show_name} - S{season:02d}E{episode:02d}"
    
    elif media_type == "movie":
        movie_name = metadata.get("movie_name", "Unknown Movie")
        year = metadata.get("year", "")
        
        # Format: MovieName (Year)/MovieName (Year).ext
        if year:
            return f"{movie_name} ({year})"
        return movie_name
    
    else:
        return metadata.get("name", "Unknown Media")

def move_to_jellyfin_library(filepath: str, media_type: str, metadata: Dict) -> bool:
    """Move the file to the appropriate Jellyfin library folder with proper naming."""
    try:
        filename = os.path.basename(filepath)
        extension = os.path.splitext(filename)[1]
        
        if media_type == "tv":
            show_name = metadata.get("show_name", "Unknown Show")
            season = metadata.get("season", 1)
            
            # Create directory structure
            show_dir = os.path.join(TV_SHOWS_DIR, show_name)
            season_dir = os.path.join(show_dir, f"Season {season:02d}")
            os.makedirs(season_dir, exist_ok=True)
            
            # Format new filename
            new_filename = f"{format_filename(media_type, metadata)}{extension}"
            destination = os.path.join(season_dir, new_filename)
            
        elif media_type == "movie":
            movie_name = metadata.get("movie_name", "Unknown Movie")
            year = metadata.get("year", "")
            
            if year:
                movie_dir = os.path.join(MOVIES_DIR, f"{movie_name} ({year})")
            else:
                movie_dir = os.path.join(MOVIES_DIR, movie_name)
                
            os.makedirs(movie_dir, exist_ok=True)
            
            # Format new filename
            new_filename = f"{format_filename(media_type, metadata)}{extension}"
            destination = os.path.join(movie_dir, new_filename)
            
        else:
            # For unknown media types
            os.makedirs(UNMATCHED_DIR, exist_ok=True)
            destination = os.path.join(UNMATCHED_DIR, filename)
        
        # Move the file
        shutil.move(filepath, destination)
        logger.info(f"Moved: {filepath} -> {destination}")
        
        # Create metadata file for manual verification if needed
        if media_type == "unknown" or metadata.get("needs_manual_check", False):
            metadata_file = f"{os.path.splitext(destination)[0]}.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=4)
            logger.info(f"Created metadata file: {metadata_file}")
            
        return True
        
    except Exception as e:
        logger.error(f"Failed to move file: {str(e)}")
        return False

def scan_and_process():
    """Scan the download directory and process all video files."""
    processed_count = 0
    failed_count = 0
    
    for root, _, files in os.walk(TELEGRAM_DOWNLOAD_DIR):
        for filename in files:
            filepath = os.path.join(root, filename)
            
            if not is_video_file(filepath):
                continue
                
            try:
                # Step 1: Initial categorization with regex
                media_type, metadata = initial_categorization(filename)
                logger.info(f"Initial categorization: {filename} -> {media_type}")
                
                # Step 2: Verify/enhance with TMDb API
                confirmed_type, enhanced_metadata = verify_with_tmdb(media_type, metadata)
                logger.info(f"Confirmed categorization: {filename} -> {confirmed_type}")
                
                # Step 3: Move to appropriate library
                if move_to_jellyfin_library(filepath, confirmed_type, enhanced_metadata):
                    processed_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                failed_count += 1
    
    logger.info(f"Processing complete. Processed: {processed_count}, Failed: {failed_count}")
    return processed_count, failed_count

def manual_correction(filepath: str, media_type: str, **metadata):
    """
    Manually correct the categorization of a file.
    Can be used to fix incorrectly categorized files.
    """
    try:
        # First, check if the file exists
        if not os.path.exists(filepath):
            logger.error(f"File not found: {filepath}")
            return False
            
        # Apply the manual correction
        if move_to_jellyfin_library(filepath, media_type, metadata):
            logger.info(f"Manual correction successful for: {filepath}")
            return True
        else:
            logger.error(f"Manual correction failed for: {filepath}")
            return False
            
    except Exception as e:
        logger.error(f"Error in manual correction: {str(e)}")
        return False

if __name__ == "__main__":
    # Create directories if they don't exist
    os.makedirs(MOVIES_DIR, exist_ok=True)
    os.makedirs(TV_SHOWS_DIR, exist_ok=True)
    os.makedirs(UNMATCHED_DIR, exist_ok=True)
    
    # Process files
    processed, failed = scan_and_process()
    print(f"Processing complete. Processed: {processed}, Failed: {failed}")
    
    # Example of manual correction:
    # manual_correction(