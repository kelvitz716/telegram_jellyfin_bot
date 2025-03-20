#!/usr/bin/env python3
# file_watcher.py - Monitors download directory and processes media files for Jellyfin

import os
import time
import logging
import re
import json
import shutil
import requests
from pathlib import Path
from logging.handlers import RotatingFileHandler
from difflib import SequenceMatcher
from typing import Dict, Tuple, Optional, List, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration
CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "paths": {
        "download_dir": "telegram_download_dir",
        "movies_dir": "movies_dir",
        "tv_shows_dir": "tv_shows_dir",
        "unmatched_dir": "unmatched_dir"
    },
    "tmdb": {
        "api_key": "YOUR_TMDB_API_KEY"
    },
    "logging": {
        "level": "INFO",
        "max_size_mb": 10,
        "backup_count": 5
    },
    "notification": {
        "enabled": True,
        "method": "print"  # Options: "print", "telegram", "pushbullet"
    },
    "process_existing_files": True
}

# Regular expressions for categorization
TV_SHOW_PATTERNS = [
    r'[Ss](\d{1,2})[Ee](\d{1,2})',  # S01E01 format
    r'[. _-](\d{1,2})x(\d{1,2})[. _-]',  # 1x01 format
    r'[. _]([Ee]pisode[. _])(\d{1,2})[. _]',  # Episode 01
    r'Season[. _](\d{1,2})[. _]Episode[. _](\d{1,2})',  # Season 1 Episode 01
]

MOVIE_YEAR_PATTERN = r'(.*?)[. _](\d{4})[. _]'  # Movie Name 2023 pattern

VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.m4v', '.flv']

# Set up processing locks to prevent duplicate processing
PROCESSING_LOCKS = {}


class MediaCategorizer:
    def __init__(self, config_path: str = CONFIG_FILE):
        """Initialize the Media Categorizer with configuration."""
        self.config = self._load_or_create_config(config_path)
        self._setup_logging()
        self._setup_directories()
        
        # Initialize TMDb API client
        self.tmdb_api_key = self.config["tmdb"]["YOUR_TMDB_API_KEY"]
        self.api_last_request = 0
        self.api_min_interval = 0.25  # 250ms between requests (4 requests per second)
        
        # Print banner
        print("\n" + "="*80)
        print(" "*30 + "MEDIA WATCHER STARTED")
        print("="*80 + "\n")
        
    def _load_or_create_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from file or create default if not exists."""
        if not os.path.exists(config_path):
            with open(config_path, 'w') as f:
                json.dump(DEFAULT_CONFIG, f, indent=4)
            print(f"Created default configuration file at {config_path}")
            print("Please edit this file with your TMDb API key and directory paths")
            return DEFAULT_CONFIG
        
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _setup_logging(self):
        """Set up logging with rotating file handler."""
        log_level = getattr(logging, self.config["logging"].get("level", "INFO"))
        max_size_mb = self.config["logging"].get("max_size_mb", 10)
        backup_count = self.config["logging"].get("backup_count", 5)
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                RotatingFileHandler(
                    "media_watcher.log", 
                    maxBytes=max_size_mb * 1024 * 1024, 
                    backupCount=backup_count
                ),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("MediaWatcher")
    
    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        self.download_dir = self.config["paths"]["telegram_download_dir"]
        self.movies_dir = self.config["paths"]["movies_dir"]
        self.tv_shows_dir = self.config["paths"]["tv_shows_dir"]
        self.unmatched_dir = self.config["paths"]["unmatched_dir"]
        
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.movies_dir, exist_ok=True)
        os.makedirs(self.tv_shows_dir, exist_ok=True)
        os.makedirs(self.unmatched_dir, exist_ok=True)
        
        print(f"Monitoring download directory: {os.path.abspath(self.download_dir)}")
        print(f"Movies will be saved to: {os.path.abspath(self.movies_dir)}")
        print(f"TV Shows will be saved to: {os.path.abspath(self.tv_shows_dir)}")
        print(f"Unmatched content will be saved to: {os.path.abspath(self.unmatched_dir)}")
    
    def notify(self, message: str):
        """Send notification based on configuration."""
        if not self.config["notification"].get("enabled", False):
            return
            
        method = self.config["notification"].get("method", "print")
        
        if method == "print":
            print(f"\n[NOTIFICATION] {message}")
        # Additional notification methods could be added here
    
    def _throttle_api_request(self):
        """Throttle API requests to avoid hitting rate limits."""
        current_time = time.time()
        elapsed = current_time - self.api_last_request
        
        if elapsed < self.api_min_interval:
            sleep_time = self.api_min_interval - elapsed
            time.sleep(sleep_time)
            
        self.api_last_request = time.time()
    
    def _make_tmdb_request(self, url: str, params: Dict[str, Any], retry_count: int = 1) -> Dict[str, Any]:
        """Make a request to TMDb API with rate limiting and retries."""
        self._throttle_api_request()
        
        try:
            print(f"   > Making TMDb API request to: {url.split('/')[-1]}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and retry_count > 0:  # Rate limit exceeded
                self.logger.warning("TMDb API rate limit reached. Waiting and retrying...")
                print("   > TMDb API rate limit reached. Waiting and retrying...")
                retry_after = int(e.response.headers.get('Retry-After', 2))
                time.sleep(retry_after)
                return self._make_tmdb_request(url, params, retry_count - 1)
            else:
                self.logger.error(f"HTTP Error: {str(e)}")
                print(f"   > HTTP Error: {str(e)}")
                raise
        except Exception as e:
            self.logger.error(f"Error making TMDb request: {str(e)}")
            print(f"   > Error making TMDb request: {str(e)}")
            raise
    
    def is_video_file(self, filename: str) -> bool:
        """Check if file has a video extension."""
        return os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS
    
    def clean_name(self, name: str) -> str:
        """Clean up a name by replacing dots, underscores with spaces."""
        return re.sub(r'[._]', ' ', name).strip()
    
    def calculate_similarity(self, a: str, b: str) -> float:
        """Calculate string similarity between two strings."""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    def initial_categorization(self, filename: str) -> Tuple[str, Dict[str, Any]]:
        """
        Initial categorization using regex patterns.
        Returns: ("tv" or "movie" or "unknown", metadata_dict)
        """
        basename = os.path.basename(filename)
        print(f"   > Analyzing filename: {basename}")
        print(f"   > Checking for TV show patterns...")
        
        # Check TV show patterns
        for pattern in TV_SHOW_PATTERNS:
            match = re.search(pattern, basename)
            if match:
                # Extract show name (everything before the pattern)
                show_part = re.split(pattern, basename)[0].strip()
                # Clean up show name
                show_name = self.clean_name(show_part)
                
                print(f"   > Found TV show pattern! Show: {show_name}, Season: {match.group(1)}, Episode: {match.group(2)}")
                
                return "tv", {
                    "show_name": show_name,
                    "season": int(match.group(1)),
                    "episode": int(match.group(2))
                }
        
        # Check movie year pattern
        print(f"   > Checking for movie patterns...")
        movie_match = re.search(MOVIE_YEAR_PATTERN, basename)
        if movie_match:
            movie_name = self.clean_name(movie_match.group(1))
            year = movie_match.group(2)
            print(f"   > Found movie pattern! Movie: {movie_name}, Year: {year}")
            return "movie", {"movie_name": movie_name, "year": year}
        
        # If no pattern matched, clean the name and return unknown
        clean_name = self.clean_name(os.path.splitext(basename)[0])
        print(f"   > No recognizable patterns found. Will try to identify: {clean_name}")
        return "unknown", {"name": clean_name}
    
    def verify_tv_show(self, metadata: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Verify a TV show with TMDb API."""
        show_name = metadata.get("show_name", "")
        print(f"   > Verifying TV show with TMDb: {show_name}")
        
        url = "https://api.themoviedb.org/3/search/tv"
        params = {
            "api_key": self.tmdb_api_key,
            "query": show_name,
            "page": 1
        }
        
        try:
            response = self._make_tmdb_request(url, params)
            results = response.get("results", [])
            
            if not results:
                self.logger.warning(f"No TV show match found for: {show_name}")
                print(f"   > No TV show match found for: {show_name}")
                return "unknown", metadata
            
            # Handle multiple results
            if len(results) > 1:
                # Calculate confidence score based on name similarity
                candidates = []
                print(f"   > Multiple matches found. Calculating confidence scores...")
                
                for result in results:
                    sim_score = self.calculate_similarity(show_name, result["name"])
                    # Add bonus for exact matches
                    if show_name.lower() == result["name"].lower():
                        sim_score += 0.2
                    # Add recent show bonus
                    if result.get("first_air_date") and result["first_air_date"] > "2015-01-01":
                        sim_score += 0.1
                    candidates.append((sim_score, result))
                    print(f"     - {result['name']} ({result.get('first_air_date', 'Unknown date')}): Score {sim_score:.2f}")
                
                # Sort by confidence score
                candidates.sort(reverse=True, key=lambda x: x[0])
                
                # If the top match is significantly better, use it
                if candidates[0][0] > 0.8 or (len(candidates) > 1 and candidates[0][0] > candidates[1][0] + 0.2):
                    show = candidates[0][1]
                    print(f"   > Selected best match: {show['name']} (Score: {candidates[0][0]:.2f})")
                else:
                    # Store all candidates in metadata for manual review
                    top_candidates = [{"name": r[1]["name"], "id": r[1]["id"], 
                                      "first_air_date": r[1].get("first_air_date", ""), 
                                      "confidence": r[0]} for r in candidates[:5]]
                    
                    metadata["candidates"] = top_candidates
                    metadata["needs_manual_review"] = True
                    self.logger.warning(f"Multiple TV show matches found for: {show_name}. Requires manual review.")
                    print(f"   > Multiple TV show matches with similar scores found for: {show_name}")
                    print(f"   > Marking for manual review and storing top 5 candidates")
                    return "unknown", metadata
            else:
                show = results[0]
                print(f"   > Found TV show match: {show['name']} ({show.get('first_air_date', 'Unknown date')})")
                
            enhanced_metadata = {
                "show_name": show["name"],
                "tmdb_id": show["id"],
                "season": metadata.get("season"),
                "episode": metadata.get("episode"),
                "first_air_date": show.get("first_air_date"),
                "overview": show.get("overview")
            }
            return "tv", enhanced_metadata
                
        except Exception as e:
            self.logger.error(f"Error verifying TV show: {str(e)}")
            print(f"   > Error verifying TV show: {str(e)}")
            return "tv", metadata  # Return original type and metadata on error
    
    def verify_movie(self, metadata: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Verify a movie with TMDb API."""
        movie_name = metadata.get("movie_name", "")
        year = metadata.get("year")
        print(f"   > Verifying movie with TMDb: {movie_name} ({year if year else 'Unknown year'})")
        
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": self.tmdb_api_key,
            "query": movie_name,
            "page": 1
        }
        
        if year:
            params["year"] = year
        
        try:
            response = self._make_tmdb_request(url, params)
            results = response.get("results", [])
            
            if not results:
                # Try a more relaxed search without the year constraint
                if year:
                    self.logger.info(f"No results for '{movie_name} ({year})'. Trying without year...")
                    print(f"   > No results for '{movie_name} ({year})'. Trying without year...")
                    params.pop("year")
                    try:
                        response = self._make_tmdb_request(url, params)
                        results = response.get("results", [])
                        if not results:
                            self.logger.warning(f"No movie match found for: {movie_name}")
                            print(f"   > No movie match found for: {movie_name}")
                            return "unknown", metadata
                    except Exception as inner_e:
                        self.logger.error(f"Error in second movie search attempt: {str(inner_e)}")
                        print(f"   > Error in second movie search attempt: {str(inner_e)}")
                        return "unknown", metadata
                else:
                    self.logger.warning(f"No movie match found for: {movie_name}")
                    print(f"   > No movie match found for: {movie_name}")
                    return "unknown", metadata
            
            # Handle multiple results
            if len(results) > 1:
                # Calculate confidence for each result
                candidates = []
                print(f"   > Multiple matches found. Calculating confidence scores...")
                
                for result in results:
                    sim_score = self.calculate_similarity(movie_name, result["title"])
                    
                    # Add bonus for matching year
                    result_year = result.get("release_date", "")[:4] if result.get("release_date") else ""
                    if year and result_year == year:
                        sim_score += 0.2
                    
                    # Add bonus for popularity
                    popularity_bonus = min(result.get("popularity", 0) / 100, 0.1)
                    sim_score += popularity_bonus
                    
                    # Add bonus for vote count (more votes = more likely to be correct)
                    vote_bonus = min(result.get("vote_count", 0) / 1000, 0.1)
                    sim_score += vote_bonus
                    
                    candidates.append((sim_score, result))
                    print(f"     - {result['title']} ({result_year}): Score {sim_score:.2f}")
                
                # Sort by confidence score
                candidates.sort(reverse=True, key=lambda x: x[0])
                
                # If top match is significantly better, use it
                if candidates[0][0] > 0.8 or (len(candidates) > 1 and candidates[0][0] > candidates[1][0] + 0.2):
                    movie = candidates[0][1]
                    print(f"   > Selected best match: {movie['title']} (Score: {candidates[0][0]:.2f})")
                else:
                    # Store candidates for manual review
                    top_candidates = [{"title": r[1]["title"], "id": r[1]["id"], 
                                      "release_date": r[1].get("release_date", ""),
                                      "confidence": r[0]} for r in candidates[:5]]
                    
                    metadata["candidates"] = top_candidates
                    metadata["needs_manual_review"] = True
                    self.logger.warning(f"Multiple movie matches found for: {movie_name}. Requires manual review.")
                    print(f"   > Multiple movie matches with similar scores found for: {movie_name}")
                    print(f"   > Marking for manual review and storing top 5 candidates")
                    return "unknown", metadata
            else:
                movie = results[0]
                result_year = movie.get("release_date", "")[:4] if movie.get("release_date") else ""
                print(f"   > Found movie match: {movie['title']} ({result_year})")
            
            enhanced_metadata = {
                "movie_name": movie["title"],
                "tmdb_id": movie["id"],
                "year": movie.get("release_date", "")[:4] if movie.get("release_date") else year,
                "overview": movie.get("overview"),
                "poster_path": movie.get("poster_path")
            }
            return "movie", enhanced_metadata
                
        except Exception as e:
            self.logger.error(f"Error verifying movie: {str(e)}")
            print(f"   > Error verifying movie: {str(e)}")
            return "movie", metadata  # Return original type and metadata on error
    
    def guess_media_type(self, metadata: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Try to guess media type for unknown files using TMDb API.
        First try movie search, then TV search.
        """
        name = metadata.get("name", "")
        print(f"   > Attempting to identify unknown media: {name}")
        
        # Try movie search first
        try:
            print(f"   > Checking if it might be a movie...")
            url = "https://api.themoviedb.org/3/search/movie"
            params = {
                "api_key": self.tmdb_api_key,
                "query": name,
                "page": 1
            }
            
            response = self._make_tmdb_request(url, params)
            results = response.get("results", [])
            
            if results:
                # Get the first result
                movie = results[0]
                result_year = movie.get("release_date", "")[:4] if movie.get("release_date") else ""
                print(f"   > Found potential movie match: {movie['title']} ({result_year})")
                
                enhanced_metadata = {
                    "movie_name": movie["title"],
                    "tmdb_id": movie["id"],
                    "year": movie.get("release_date", "")[:4] if movie.get("release_date") else "",
                    "overview": movie.get("overview")
                }
                return "movie", enhanced_metadata
        except Exception as e:
            self.logger.error(f"Error in movie search for unknown media: {str(e)}")
            print(f"   > Error in movie search for unknown media: {str(e)}")
        
        # If movie search fails, try TV search
        try:
            print(f"   > Checking if it might be a TV show...")
            url = "https://api.themoviedb.org/3/search/tv"
            params = {
                "api_key": self.tmdb_api_key,
                "query": name,
                "page": 1
            }
            
            response = self._make_tmdb_request(url, params)
            results = response.get("results", [])
            
            if results:
                # Get the first result
                show = results[0]
                print(f"   > Found potential TV show match: {show['name']} ({show.get('first_air_date', 'Unknown date')})")
                
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
            self.logger.error(f"Error in TV search for unknown media: {str(e)}")
            print(f"   > Error in TV search for unknown media: {str(e)}")
        
        # If both searches fail, return unknown
        print(f"   > Could not identify media type. Marking as unknown.")
        return "unknown", metadata
    
    def verify_with_tmdb(self, media_type: str, metadata: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Verify and enhance metadata using TMDb API.
        Returns: (confirmed_type, enhanced_metadata)
        """
        if media_type == "tv":
            return self.verify_tv_show(metadata)
        elif media_type == "movie":
            return self.verify_movie(metadata)
        else:
            return self.guess_media_type(metadata)
    
    def format_filename(self, media_type: str, metadata: Dict[str, Any]) -> str:
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
    
    def move_to_jellyfin_library(self, filepath: str, media_type: str, metadata: Dict[str, Any]) -> bool:
        """Move the file to the appropriate Jellyfin library folder with proper naming."""
        try:
            filename = os.path.basename(filepath)
            extension = os.path.splitext(filename)[1]
            
            if media_type == "tv":
                show_name = metadata.get("show_name", "Unknown Show")
                season = metadata.get("season", 1)
                
                print(f"   > Moving file to TV Shows library:")
                print(f"     - Show: {show_name}")
                print(f"     - Season: {season}")
                print(f"     - Episode: {metadata.get('episode', 1)}")
                
                # Create directory structure
                show_dir = os.path.join(self.tv_shows_dir, show_name)
                season_dir = os.path.join(show_dir, f"Season {season:02d}")
                os.makedirs(season_dir, exist_ok=True)
                
                # Format new filename
                new_filename = f"{self.format_filename(media_type, metadata)}{extension}"
                destination = os.path.join(season_dir, new_filename)
                
                print(f"     - Final path: {destination}")
                
            elif media_type == "movie":
                movie_name = metadata.get("movie_name", "Unknown Movie")
                year = metadata.get("year", "")
                
                print(f"   > Moving file to Movies library:")
                print(f"     - Title: {movie_name}")
                if year:
                    print(f"     - Year: {year}")
                
                if year:
                    movie_dir = os.path.join(self.movies_dir, f"{movie_name} ({year})")
                else:
                    movie_dir = os.path.join(self.movies_dir, movie_name)
                    
                os.makedirs(movie_dir, exist_ok=True)
                
                # Format new filename
                new_filename = f"{self.format_filename(media_type, metadata)}{extension}"
                destination = os.path.join(movie_dir, new_filename)
                
                print(f"     - Final path: {destination}")
                
            else:
                # For unknown media types
                os.makedirs(self.unmatched_dir, exist_ok=True)
                destination = os.path.join(self.unmatched_dir, filename)
                
                print(f"   > Media type unknown. Moving to unmatched directory:")
                print(f"     - Final path: {destination}")
            
            # Check if destination exists
            if os.path.exists(destination):
                # Create a unique filename by appending a number
                base, ext = os.path.splitext(destination)
                counter = 1
                while os.path.exists(f"{base} ({counter}){ext}"):
                    counter += 1
                destination = f"{base} ({counter}){ext}"
                print(f"     - File already exists! Using alternate path: {destination}")
            
            # Move the file
            shutil.move(filepath, destination)
            self.logger.info(f"Moved: {filepath} -> {destination}")
            print(f"   > Successfully moved file")
            
            # Create metadata file for manual verification if needed
            if media_type == "unknown" or metadata.get("needs_manual_check", False):
                metadata_file = f"{os.path.splitext(destination)[0]}.json"
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=4)
                self.logger.info(f"Created metadata file: {metadata_file}")
                print(f"   > Created metadata file for manual verification")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to move file: {str(e)}")
            print(f"   > ERROR: Failed to move file: {str(e)}")
            return False
    
    def process_file(self, filepath: str) -> bool:
        """Process a single file."""
        try:
            filename = os.path.basename(filepath)
            
            # Skip non-video files
            if not self.is_video_file(filepath):
                self.logger.debug(f"Skipping non-video file: {filename}")
                return False
                
            # Skip files that are still being modified
            try:
                print(f"\n=== Checking if file is still being modified: {filename} ===")
                file_size_before = os.path.getsize(filepath)
                time.sleep(1)  # Wait a moment
                file_size_after = os.path.getsize(filepath)
                
                if file_size_before != file_size_after:
                    self.logger.info(f"File {filename} is still being modified. Will process later.")
                    print(f"File {filename} is still being modified. Will process later.")
                    return False
            except FileNotFoundError:
                self.logger.warning(f"File disappeared during size check: {filename}")
                print(f"File disappeared during size check: {filename}")
                return False
                
            print(f"\n{'='*80}")
            print(f"PROCESSING NEW FILE: {filename}")
            print(f"{'='*80}")
            
            self.notify(f"Processing new file: {filename}")
            self.logger.info(f"Processing: {filepath}")
            
            # Step 1: Initial categorization with regex
            print("\n=== STEP 1: Initial Categorization ===")
            media_type, metadata = self.initial_categorization(filename)
            self.logger.info(f"Initial categorization: {filename} -> {media_type}")
            self.notify(f"Initial categorization: {filename} as {media_type}")
            
            # Step 2: Verify/enhance with TMDb API
            print("\n=== STEP 2: TMDb Verification ===")
            confirmed_type, enhanced_metadata = self.verify_with_tmdb(media_type, metadata)
            self.logger.info(f"Confirmed categorization: {filename} -> {confirmed_type}")
            self.notify(f"Confirmed categorization: {filename} as {confirmed_type}")
            
            # Step 3: Move to appropriate library
            print("\n=== STEP 3: Moving to Library ===")
            if self.move_to_jellyfin_library(filepath, confirmed_type, enhanced_metadata):
                self.notify(f"Successfully processed: {filename}")
                print(f"\n✅ Successfully processed: {filename}")
                return True
            else:
                self.notify(f"Failed to process: {filename}")
                print(f"\n❌ Failed to process: {filename}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing {filepath}: {str(e)}")
            self.notify(f"Error processing {os.path.basename(filepath)}: {str(e)}")
            print(f"\n❌ Error processing {filepath}: {str(e)}")
            return False


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, categorizer):
        self.categorizer = categorizer
        self.logger = categorizer.logger
    
    def on_closed(self, event):
        """Process a file when it's completely written and closed."""
        if event.is_directory:
            return
            
        filepath = event.src_path
        
        # Skip hidden files and non-video files quickly
        filename = os.path.basename(filepath)
        if filename.startswith('.') or not self.categorizer.is_video_file(filepath):
            return
            
        # Skip if file is already being processed
        if filepath in PROCESSING_LOCKS:
            self.logger.debug(f"File already being processed: {filepath}")
            return
            
        # Add processing lock
        PROCESSING_LOCKS[filepath] = time.time()
        
        try:
            print(f"\nDETECTED FILE CLOSED: {filename}")
            # Allow file to stabilize
            time.sleep(2)
            if os.path.exists(filepath):
                self.categorizer.process_file(filepath)
        finally:
            # Remove processing lock
            if filepath in PROCESSING_LOCKS:
                del PROCESSING_LOCKS[filepath]
    
    def on_moved(self, event):
        """Process a file when it's moved into the watched directory."""
        if event.is_directory:
            return
            
        filepath = event.dest_path
        
        # Skip hidden files and non-video files quickly
        filename = os.path.basename(filepath)
        if filename.startswith('.') or not self.categorizer.is_video_file(filepath):
            return
            
        # Skip if file is already being processed
        if filepath in PROCESSING_LOCKS:
            self.logger.debug(f"File already being processed: {filepath}")
            return
            
        # Add processing lock
        PROCESSING_LOCKS[filepath] = time.time()
        
        try:
            print(f"\nDETECTED FILE MOVED: {filename}")
            # Allow file to stabilize
            time.sleep(2)
            if os.path.exists(filepath):
                self.categorizer.process_file(filepath)
        finally:
            # Remove processing lock
            if filepath in PROCESSING_LOCKS:
                del PROCESSING_LOCKS[filepath]


def main():
    # Initialize the categorizer
    categorizer = MediaCategorizer()
    
    # Process existing files in download directory
    if categorizer.config.get("process_existing_files", True):
        categorizer.logger.info("Processing existing files...")
        print("\n=== PROCESSING EXISTING FILES IN DOWNLOAD DIRECTORY ===")
        
        existing_files = []
        for root, _, files in os.walk(categorizer.download_dir):
            for filename in files:
                if categorizer.is_video_file(filename):
                    existing_files.append(os.path.join(root, filename))
        
        if existing_files:
            print(f"Found {len(existing_files)} existing video files to process:")
            for idx, filepath in enumerate(existing_files, 1):
                print(f"{idx}. {os.path.basename(filepath)}")
            
            for filepath in existing_files:
                # Skip if file is already being processed
                if filepath in PROCESSING_LOCKS:
                    continue
                    
                # Add processing lock
                PROCESSING_LOCKS[filepath] = time.time()
                
                try:
                    categorizer.process_file(filepath)
                finally:
                    # Remove processing lock
                    if filepath in PROCESSING_LOCKS:
                        del PROCESSING_LOCKS[filepath]
        else:
            print("No existing video files found in download directory.")
    
    # Set up watchdog observer
    event_handler = FileEventHandler(categorizer)
    observer = Observer()
    observer.schedule(event_handler, categorizer.download_dir, recursive=True)
    
    categorizer.logger.info(f"Watching directory: {categorizer.download_dir}")
    
    print("\n" + "="*80)
    print(" "*30 + "WATCHING FOR NEW FILES")
    print("="*80)
    
    categorizer.notify("Media file watcher started and ready for new files")
    
    # Start watching
    observer.start()
    try:
        print("\nWaiting for new files... (Press Ctrl+C to exit)")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        observer.stop()
    observer.join()
    print("Media Watcher has been stopped.")


if __name__ == "__main__":
    main()