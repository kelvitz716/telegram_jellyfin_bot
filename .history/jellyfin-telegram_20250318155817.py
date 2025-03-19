import os
import re
import json
import shutil
import requests
import logging
import datetime
from pathlib import Path
from typing import Dict, Tuple, Optional, List
from difflib import SequenceMatcher

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
    r'[Ss](\d{1,2})[Ee](\d{1,2})',        # S01E01 format
    r'[. _-](\d{1,2})x(\d{1,2})[. _-]',   # 1x01 format
    r'[. _]([Ee]pisode[. _])(\d{1,2})',   # Episode 01
    r'Season[. _](\d{1,2})[. _]Episode[. _](\d{1,2})',  # Season 1 Episode 01
    r'S(\d{1,2})E(\d{1,2})(?![a-zA-Z])',  # S1E1 without separator
]

MOVIE_YEAR_PATTERNS = [
    r'(.*?)[. _](\d{4})[. _]',   # Movie Name 2023 pattern
    r'(.*?)\((\d{4})\)',         # Movie Name (2023) pattern
    r'(.*?)\.(\d{4})\.',         # Movie.Name.2023.rest
]

CLEAN_PATTERNS = [
    r'(?i)1080p|720p|2160p|4k|uhd|hdr',
    r'(?i)bluray|bdrip|brrip|webrip|web-dl|webdl|web',
    r'(?i)10bit|8bit|x264|x265|xvid|hevc|avc|h\.264|h\.265',
    r'(?i)aac|ac3|mp3|dd5\.1|dd2\.0|dts|dts-hd|truehd|atmos',
    r'(?i)-[a-z0-9]+$',  # Release groups at end
    r'(?i)\b(complete|extended|directors\.cut|unrated|internal|proper|repack)\b',
    r'@\w+',  # Telegram tags
    r'\[.*?\]|\(.*?\)',  # Any text in brackets/parentheses
]

VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.m4v', '.flv']

def is_video_file(filename: str) -> bool:
    """Check if file has a video extension."""
    return Path(filename).suffix.lower() in VIDEO_EXTENSIONS

def clean_title(title: str) -> str:
    """Clean title by removing quality tags, codecs, and special patterns."""
    cleaned = re.sub(r'[._]', ' ', title)  # Replace dots/underscores with spaces
    
    for pattern in CLEAN_PATTERNS:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Remove remaining special characters and trim
    cleaned = re.sub(r'[^\w\s-]', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def initial_categorization(filename: str) -> Tuple[str, Dict]:
    """
    Initial categorization using regex patterns.
    Returns: ("tv"|"movie"|"unknown", metadata_dict)
    """
    basename = Path(filename).stem  # Get filename without extension
    
    # Check TV show patterns first
    for pattern in TV_SHOW_PATTERNS:
        match = re.search(pattern, basename)
        if match:
            show_part = basename[:match.start()].strip(' ._-')
            return "tv", {
                "show_name": clean_title(show_part),
                "season": int(match.group(1)),
                "episode": int(match.group(2)),
                "original_filename": filename
            }
    
    # Check movie year patterns
    for pattern in MOVIE_YEAR_PATTERNS:
        match = re.search(pattern, basename)
        if match:
            current_year = datetime.datetime.now().year
            year = int(match.group(2))
            if 1900 < year <= current_year + 2:  # Sanity check year
                return "movie", {
                    "movie_name": clean_title(match.group(1)),
                    "year": str(year),
                    "original_filename": filename
                }
    
    # Fallback to unknown with cleaned name
    return "unknown", {
        "name": clean_title(basename),
        "original_filename": filename
    }

def verify_with_tmdb(media_type: str, metadata: Dict) -> Tuple[str, Dict]:
    """
    Verify and enhance metadata using TMDb API.
    Returns: (confirmed_type, enhanced_metadata)
    """
    # Check for manual correction patterns first
    corrected_type, corrected_meta = match_from_corrections(metadata["original_filename"])
    if corrected_type:
        return corrected_type, {**metadata, **corrected_meta}
    
    if media_type == "tv":
        return verify_tv_show(metadata)
    elif media_type == "movie":
        return verify_movie(metadata)
    return guess_media_type(metadata)

def verify_tv_show(metadata: Dict) -> Tuple[str, Dict]:
    """Verify a TV show with TMDb API."""
    show_name = metadata["show_name"]
    url = "https://api.themoviedb.org/3/search/tv"
    params = {"api_key": TMDB_API_KEY, "query": show_name}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get("results", [])
        
        if not results:
            logger.warning(f"No TV results for: {show_name}")
            return "unknown", metadata
        
        # Find best match using similarity scoring
        best_match = max(
            results,
            key=lambda x: SequenceMatcher(None, show_name.lower(), x["name"].lower()).ratio()
        )
        
        # Get series details
        series_url = f"https://api.themoviedb.org/3/tv/{best_match['id']}"
        series_response = requests.get(series_url, params={"api_key": TMDB_API_KEY})
        series_data = series_response.json()
        
        enhanced = {
            "show_name": best_match["name"],
            "tmdb_id": best_match["id"],
            "season": metadata["season"],
            "episode": metadata["episode"],
            "overview": series_data.get("overview"),
            "first_air_date": series_data.get("first_air_date"),
            "poster_path": series_data.get("poster_path")
        }
        return "tv", {**metadata, **enhanced}
    
    except Exception as e:
        logger.error(f"TV verification failed: {str(e)}")
        return "unknown", metadata

def verify_movie(metadata: Dict) -> Tuple[str, Dict]:
    """Verify a movie with TMDb API."""
    movie_name = metadata["movie_name"]
    year = metadata.get("year")
    url = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": movie_name, "year": year}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get("results", [])
        
        if not results:
            if year:
                logger.info(f"Retrying without year for: {movie_name}")
                return verify_movie({**metadata, "year": None})
            return "unknown", metadata
        
        # Find best match using combined score
        best_match = max(results, key=lambda x: (
            SequenceMatcher(None, movie_name.lower(), x["title"].lower()).ratio() +
            (0.2 if x.get("release_date", "").startswith(str(year)) else 0)
        ))
        
        enhanced = {
            "movie_name": best_match["title"],
            "tmdb_id": best_match["id"],
            "year": best_match.get("release_date", "")[:4],
            "overview": best_match.get("overview"),
            "poster_path": best_match.get("poster_path")
        }
        return "movie", {**metadata, **enhanced}
    
    except Exception as e:
        logger.error(f"Movie verification failed: {str(e)}")
        return "unknown", metadata

def guess_media_type(metadata: Dict) -> Tuple[str, Dict]:
    """Guess media type using fallback searches."""
    # Try movie search first
    movie_type, movie_meta = verify_movie({"movie_name": metadata["name"]})
    if movie_type == "movie":
        return movie_type, movie_meta
    
    # Then try TV search
    tv_type, tv_meta = verify_tv_show({"show_name": metadata["name"], "season": 1, "episode": 1})
    if tv_type == "tv":
        return tv_type, tv_meta
    
    return "unknown", metadata

def format_filename(media_type: str, metadata: Dict) -> str:
    """Format filename according to Jellyfin conventions."""
    if media_type == "tv":
        return (f"{metadata['show_name']} - " 
                f"S{metadata['season']:02}E{metadata['episode']:02}")
                
    if media_type == "movie":
        year = metadata.get("year", "")
        return f"{metadata['movie_name']} ({year})" if year else metadata["movie_name"]
    
    return metadata["name"]

def move_to_jellyfin_library(filepath: str, media_type: str, metadata: Dict) -> bool:
    """Move file to appropriate Jellyfin library location."""
    try:
        src = Path(filepath)
        dest_dir = Path(UNMATCHED_DIR)
        
        if media_type == "tv":
            show_dir = Path(TV_SHOWS_DIR) / metadata["show_name"].replace(":", " -")  # Handle colon in names
            season = metadata["season"]
            dest_dir = show_dir / f"Season {season:02}"
            dest_file = dest_dir / f"{format_filename(media_type, metadata)}{src.suffix}"
            
        elif media_type == "movie":
            movie_name = metadata["movie_name"].replace(":", " -")
            year = metadata.get("year", "")
            dest_dir = Path(MOVIES_DIR) / (f"{movie_name} ({year})" if year else movie_name)
            dest_file = dest_dir / f"{format_filename(media_type, metadata)}{src.suffix}"
            
        else:
            dest_file = Path(UNMATCHED_DIR) / src.name

        # Create destination directory
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Perform move operation
        shutil.move(str(src), str(dest_file))
        logger.info(f"Moved '{src.name}' to {dest_file}")
        
        # Cleanup empty directories
        try:
            src.parent.rmdir()  # Remove empty source directory
        except OSError:
            pass
            
        return True
        
    except Exception as e:
        logger.error(f"Failed to move file: {str(e)}")
        return False

def scan_and_process():
    """Main processing loop for directory scanning."""
    processed = failed = 0
    
    for root, _, files in os.walk(TELEGRAM_DOWNLOAD_DIR):
        for filename in files:
            filepath = os.path.join(root, filename)
            
            if not is_video_file(filename):
                continue
                
            try:
                # Initial processing
                media_type, meta = initial_categorization(filename)
                
                # TMDB verification
                confirmed_type, enhanced_meta = verify_with_tmdb(media_type, meta)
                
                # File movement
                if move_to_jellyfin_library(filepath, confirmed_type, enhanced_meta):
                    processed += 1
                else:
                    failed += 1
                    
            except Exception as e:
                logger.error(f"Failed processing {filename}: {str(e)}")
                failed += 1
                
    logger.info(f"Processed {processed} files, {failed} failures")
    return processed, failed

def manual_correction(filepath: str, media_type: str, **metadata) -> bool:
    """
    Manually correct file categorization and store pattern for future use.
    
    Args:
        filepath: Path to the file needing correction
        media_type: Correct media type ('tv'|'movie'|'unknown')
        metadata: Required fields based on media_type:
            - tv: show_name, season, episode, [tmdb_id]
            - movie: movie_name, [year], [tmdb_id]
    
    Returns:
        bool: True if correction succeeded, False otherwise
    """
    try:
        filepath = Path(filepath)
        if not filepath.exists():
            logger.error(f"File not found: {filepath}")
            return False

        # Handle TMDB ID lookup if provided
        if media_type in ["tv", "movie"] and "tmdb_id" in metadata:
            tmdb_id = metadata["tmdb_id"]
            endpoint = "tv" if media_type == "tv" else "movie"
            url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
            response = requests.get(url, params={"api_key": TMDB_API_KEY})
            response.raise_for_status()
            tmdb_data = response.json()
            
            if media_type == "tv":
                metadata["show_name"] = tmdb_data.get("name", metadata["show_name"])
                metadata["season"] = metadata.get("season", 1)
                metadata["episode"] = metadata.get("episode", 1)
            else:
                metadata["movie_name"] = tmdb_data.get("title", metadata["movie_name"])
                if "release_date" in tmdb_data:
                    metadata["year"] = tmdb_data["release_date"][:4]

        # Move file using corrected metadata
        if move_to_jellyfin_library(str(filepath), media_type, metadata):
            # Save correction pattern
            pattern = create_matching_pattern(filepath.name)
            correction = {
                "media_type": media_type,
                "metadata": metadata,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            corrections_file = Path("corrections.json")
            corrections = {}
            if corrections_file.exists():
                with corrections_file.open() as f:
                    corrections = json.load(f)
            
            corrections[pattern] = correction
            
            with corrections_file.open("w") as f:
                json.dump(corrections, f, indent=2)
            
            logger.info(f"Saved correction pattern: {pattern}")
            return True
        return False

    except Exception as e:
        logger.error(f"Manual correction failed: {str(e)}")
        return False

def create_matching_pattern(filename: str) -> str:
    """Generate regex pattern for filename matching."""
    base = Path(filename).stem
    pattern = re.sub(
        r'\d{4}|\b(?:s\d+e\d+|season\s*\d+|episode\s*\d+)\b|[._-]|@\w+',
        lambda m: f'[\\W_]*{re.escape(m.group())}[\\W_]*' if m.group().isalnum() else '.*',
        base,
        flags=re.IGNORECASE
    )
    return f'^{pattern}.*{Path(filename).suffix}$'

def match_from_corrections(filename: str) -> Tuple[Optional[str], Optional[Dict]]:
    """Check if filename matches any stored correction patterns."""
    try:
        with open("corrections.json") as f:
            corrections = json.load(f)
        
        for pattern, correction in corrections.items():
            if re.match(pattern, filename, re.IGNORECASE):
                logger.debug(f"Matched correction pattern: {pattern}")
                return correction["media_type"], correction["metadata"]
        return None, None
    except (FileNotFoundError, json.JSONDecodeError):
        return None, None
    except Exception as e:
        logger.warning(f"Correction matching failed: {str(e)}")
        return None, None

def run_telegram_bot():
    """Run Telegram bot for receiving and processing files."""
    from telegram import Update
    from telegram.ext import (
        Updater,
        CommandHandler,
        MessageHandler,
        Filters,
        CallbackContext
    )

    class TelegramFileHandler:
        def __init__(self):
            self.active_downloads = set()

        async def handle_file(self, update: Update, context: CallbackContext) -> None:
            file = update.message.document
            if file.file_name in self.active_downloads:
                return

            self.active_downloads