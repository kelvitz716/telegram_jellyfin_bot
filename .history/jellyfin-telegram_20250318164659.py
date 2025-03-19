import os
import re
import json
import shutil
import time
import requests
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
from difflib import SequenceMatcher
from typing import Dict, Tuple, Optional, List, Any

# Configuration
CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "paths": {
        "telegram_download_dir": "/path/to/telegram/downloads",
        "movies_dir": "/path/to/jellyfin/movies",
        "tv_shows_dir": "/path/to/jellyfin/tvshows",
        "unmatched_dir": "/path/to/unmatched"
    },
    "tmdb": {
        "api_key": "YOUR_TMDB_API_KEY"
    },
    "telegram": {
        "bot_token": "YOUR_TELEGRAM_BOT_TOKEN",
        "enabled": False
    },
    "logging": {
        "level": "INFO",
        "max_size_mb": 10,
        "backup_count": 5
    }
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

class MediaCategorizer:
    def __init__(self, config_path: str = CONFIG_FILE):
        """Initialize the Media Categorizer with configuration."""
        self.config = self._load_or_create_config(config_path)
        self._setup_logging()
        self._setup_directories()
        
        # Initialize TMDb API client
        self.tmdb_api_key = self.config["tmdb"]["api_key"]
        self.api_last_request = 0
        self.api_min_interval = 0.25  # 250ms between requests (4 requests per second)
        
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
                    "media_categorizer.log", 
                    maxBytes=max_size_mb * 1024 * 1024, 
                    backupCount=backup_count
                ),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
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
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and retry_count > 0:  # Rate limit exceeded
                self.logger.warning("TMDb API rate limit reached. Waiting and retrying...")
                retry_after = int(e.response.headers.get('Retry-After', 2))
                time.sleep(retry_after)
                return self._make_tmdb_request(url, params, retry_count - 1)
            else:
                self.logger.error(f"HTTP Error: {str(e)}")
                raise
        except Exception as e:
            self.logger.error(f"Error making TMDb request: {str(e)}")
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
        
        # Check TV show patterns
        for pattern in TV_SHOW_PATTERNS:
            match = re.search(pattern, basename)
            if match:
                # Extract show name (everything before the pattern)
                show_part = re.split(pattern, basename)[0].strip()
                # Clean up show name
                show_name = self.clean_name(show_part)
                
                return "tv", {
                    "show_name": show_name,
                    "season": int(match.group(1)),
                    "episode": int(match.group(2))
                }
        
        # Check movie year pattern
        movie_match = re.search(MOVIE_YEAR_PATTERN, basename)
        if movie_match:
            movie_name = self.clean_name(movie_match.group(1))
            year = movie_match.group(2)
            return "movie", {"movie_name": movie_name, "year": year}
        
        # If no pattern matched, clean the name and return unknown
        clean_name = self.clean_name(os.path.splitext(basename)[0])
        return "unknown", {"name": clean_name}
    
    def verify_tv_show(self, metadata: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Verify a TV show with TMDb API."""
        show_name = metadata.get("show_name", "")
        
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
                return "unknown", metadata
            
            # Handle multiple results
            if len(results) > 1:
                # Calculate confidence score based on name similarity
                candidates = []
                for result in results:
                    sim_score = self.calculate_similarity(show_name, result["name"])
                    # Add bonus for exact matches
                    if show_name.lower() == result["name"].lower():
                        sim_score += 0.2
                    # Add recent show bonus
                    if result.get("first_air_date") and result["first_air_date"] > "2015-01-01":
                        sim_score += 0.1
                    candidates.append((sim_score, result))
                
                # Sort by confidence score
                candidates.sort(reverse=True, key=lambda x: x[0])
                
                # If the top match is significantly better, use it
                if candidates[0][0] > 0.8 or (len(candidates) > 1 and candidates[0][0] > candidates[1][0] + 0.2):
                    show = candidates[0][1]
                else:
                    # Store all candidates in metadata for manual review
                    top_candidates = [{"name": r[1]["name"], "id": r[1]["id"], 
                                      "first_air_date": r[1].get("first_air_date", ""), 
                                      "confidence": r[0]} for r in candidates[:5]]
                    
                    metadata["candidates"] = top_candidates
                    metadata["needs_manual_review"] = True
                    self.logger.warning(f"Multiple TV show matches found for: {show_name}. Requires manual review.")
                    return "unknown", metadata
            else:
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
                
        except Exception as e:
            self.logger.error(f"Error verifying TV show: {str(e)}")
            return "tv", metadata  # Return original type and metadata on error
    
    def verify_movie(self, metadata: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Verify a movie with TMDb API."""
        movie_name = metadata.get("movie_name", "")
        year = metadata.get("year")
        
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": self.tmdb_api_key,
            "query": movie_name,
            "year": year,
            "page": 1
        }
        
        try:
            response = self._make_tmdb_request(url, params)
            results = response.get("results", [])
            
            if not results:
                # Try a more relaxed search without the year constraint
                if year:
                    self.logger.info(f"No results for '{movie_name} ({year})'. Trying without year...")
                    params.pop("year")
                    try:
                        response = self._make_tmdb_request(url, params)
                        results = response.get("results", [])
                        if not results:
                            self.logger.warning(f"No movie match found for: {movie_name}")
                            return "unknown", metadata
                    except Exception as inner_e:
                        self.logger.error(f"Error in second movie search attempt: {str(inner_e)}")
                        return "unknown", metadata
                else:
                    self.logger.warning(f"No movie match found for: {movie_name}")
                    return "unknown", metadata
            
            # Handle multiple results
            if len(results) > 1:
                # Calculate confidence for each result
                candidates = []
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
                
                # Sort by confidence score
                candidates.sort(reverse=True, key=lambda x: x[0])
                
                # If top match is significantly better, use it
                if candidates[0][0] > 0.8 or (len(candidates) > 1 and candidates[0][0] > candidates[1][0] + 0.2):
                    movie = candidates[0][1]
                else:
                    # Store candidates for manual review
                    top_candidates = [{"title": r[1]["title"], "id": r[1]["id"], 
                                      "release_date": r[1].get("release_date", ""),
                                      "confidence": r[0]} for r in candidates[:5]]
                    
                    metadata["candidates"] = top_candidates
                    metadata["needs_manual_review"] = True
                    self.logger.warning(f"Multiple movie matches found for: {movie_name}. Requires manual review.")
                    return "unknown", metadata
            else:
                movie = results[0]
            
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
            return "movie", metadata  # Return original type and metadata on error
    
    def guess_media_type(self, metadata: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Try to guess media type for unknown files using TMDb API.
        First try movie search, then TV search.
        """
        name = metadata.get("name", "")
        
        # Try movie search first
        try:
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
                enhanced_metadata = {
                    "movie_name": movie["title"],
                    "tmdb_id": movie["id"],
                    "year": movie.get("release_date", "")[:4] if movie.get("release_date") else "",
                    "overview": movie.get("overview")
                }
                return "movie", enhanced_metadata
        except Exception as e:
            self.logger.error(f"Error in movie search for unknown media: {str(e)}")
        
        # If movie search fails, try TV search
        try:
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
        
        # If both searches fail, return unknown
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
                
                # Create directory structure
                show_dir = os.path.join(self.tv_shows_dir, show_name)
                season_dir = os.path.join(show_dir, f"Season {season:02d}")
                os.makedirs(season_dir, exist_ok=True)
                
                # Format new filename
                new_filename = f"{self.format_filename(media_type, metadata)}{extension}"
                destination = os.path.join(season_dir, new_filename)
                
            elif media_type == "movie":
                movie_name = metadata.get("movie_name", "Unknown Movie")
                year = metadata.get("year", "")
                
                if year:
                    movie_dir = os.path.join(self.movies_dir, f"{movie_name} ({year})")
                else:
                    movie_dir = os.path.join(self.movies_dir, movie_name)
                    
                os.makedirs(movie_dir, exist_ok=True)
                
                # Format new filename
                new_filename = f"{self.format_filename(media_type, metadata)}{extension}"
                destination = os.path.join(movie_dir, new_filename)
                
            else:
                # For unknown media types
                os.makedirs(self.unmatched_dir, exist_ok=True)
                destination = os.path.join(self.unmatched_dir, filename)
            
            # Check if destination exists
            if os.path.exists(destination):
                # Create a unique filename by appending a number
                base, ext = os.path.splitext(destination)
                counter = 1
                while os.path.exists(f"{base} ({counter}){ext}"):
                    counter += 1
                destination = f"{base} ({counter}){ext}"
            
            # Move the file
            shutil.move(filepath, destination)
            self.logger.info(f"Moved: {filepath} -> {destination}")
            
            # Create metadata file for manual verification if needed
            if media_type == "unknown" or metadata.get("needs_manual_check", False):
                metadata_file = f"{os.path.splitext(destination)[0]}.json"
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=4)
                self.logger.info(f"Created metadata file: {metadata_file}")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to move file: {str(e)}")
            return False
    
    def scan_and_process(self):
        """Scan the download directory and process all video files."""
        processed_count = 0
        failed_count = 0
        
        for root, _, files in os.walk(self.download_dir):
            for filename in files:
                filepath = os.path.join(root, filename)
                
                if not self.is_video_file(filepath):
                    continue
                    
                try:
                    # Step 1: Initial categorization with regex
                    media_type, metadata = self.initial_categorization(filename)
                    self.logger.info(f"Initial categorization: {filename} -> {media_type}")
                    
                    # Step 2: Verify/enhance with TMDb API
                    confirmed_type, enhanced_metadata = self.verify_with_tmdb(media_type, metadata)
                    self.logger.info(f"Confirmed categorization: {filename} -> {confirmed_type}")
                    
                    # Step 3: Move to appropriate library
                    if self.move_to_jellyfin_library(filepath, confirmed_type, enhanced_metadata):
                        processed_count += 1
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Error processing {filename}: {str(e)}")
                    failed_count += 1
        
        self.logger.info(f"Processing complete. Processed: {processed_count}, Failed: {failed_count}")
        return processed_count, failed_count
    
    def manual_correction(self, filepath: str, media_type: str, **metadata):
        """
        Manually correct the categorization of a file.
        Can be used to fix incorrectly categorized files.
        
        Examples:
        - manual_correction("/path/to/file.mp4", "tv", show_name="Correct Show Name", season=2, episode=5)
        - manual_correction("/path/to/file.mp4", "movie", movie_name="Correct Movie Name", year="2023")
        - manual_correction("/path/to/file.mp4", "unknown")  # Move to unmatched folder
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # First, check if the file exists
            if not os.path.exists(filepath):
                self.logger.error(f"File not found: {filepath}")
                return False
            
            # If metadata contains a TMDb ID, fetch complete info
            if media_type in ["tv", "movie"] and metadata.get("tmdb_id"):
                try:
                    tmdb_id = metadata["tmdb_id"]
                    
                    if media_type == "tv":
                        url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
                    else:  # movie
                        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
                    
                    params = {"api_key": self.tmdb_api_key}
                    response = self._make_tmdb_request(url, params)
                    metadata.update(response)
                except Exception as e:
                    self.logger.error(f"Error fetching TMDb info: {str(e)}")
            
            # Move to appropriate library
            return self.move_to_jellyfin_library(filepath, media_type, metadata)
            
        except Exception as e:
            self.logger.error(f"Error in manual correction: {str(e)}")
            return False

def run_telegram_bot(categorizer):
    """
    Run a Telegram bot integration.
    This would be expanded with actual Telegram bot implementation.
    """
    try:
        from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
        
        def start(update, context):
            update.message.reply_text('Hello! Send me media files to categorize them for Jellyfin.')
        
        def handle_document(update, context):
            file = update.message.document
            if not categorizer.is_video_file(file.file_name):
                update.message.reply_text("This doesn't seem to be a video file. Please send video files only.")
                return
                
            update.message.reply_text(f"Downloading {file.file_name}...")
            
            # Download file
            file_path = os.path.join(categorizer.download_dir, file.file_name)
            file_info = context.bot.get_file(file.file_id)
            file_info.download(file_path)
            
            update.message.reply_text(f"Processing {file.file_name}...")
            
            # Process the file
            try:
                media_type, metadata = categorizer.initial_categorization(file.file_name)
                confirmed_type, enhanced_metadata = categorizer.verify_with_tmdb(media_type, metadata)
                
                if categorizer.move_to_jellyfin_library(file_path, confirmed_type, enhanced_metadata):
                    destination = categorizer.format_filename(confirmed_type, enhanced_metadata)
                    update.message.reply_text(f"✅ File categorized as {confirmed_type}: {destination}")
                else:
                    update.message.reply_text("❌ Failed to categorize file.")
                    
            except Exception as e:
                update.message.reply_text(f"❌ Error: {str(e)}")
        
        bot_token = categorizer.config["telegram"]["bot_token"]
        if bot_token == "YOUR_TELEGRAM_BOT_TOKEN":
            categorizer.logger.error("Telegram bot token not configured. Please update the config file.")
            return False
            
        # Set up the Telegram bot
        updater = Updater(bot_token)
        dispatcher = updater.dispatcher
        
        # Add handlers
        dispatcher.add_handler(CommandHandler("start", start))
        dispatcher.add_handler(MessageHandler(Filters.document, handle_document))
        
        # Start the bot
        categorizer.logger.info("Starting Telegram bot...")
        updater.start_polling()
        updater.idle()
        return True
    
    except ImportError:
        categorizer.logger.error("python-telegram-bot package not installed. Please install it with: pip install python-telegram-bot")
        return False
    except Exception as e:
        categorizer.logger.error(f"Error starting Telegram bot: {str(e)}")
        return False

def main():
    """Main function to run the media categorizer."""
    # Initialize the categorizer
    categorizer = MediaCategorizer()
    
    # Start bot if enabled
    if categorizer.config["telegram"].get("enabled", False):
        if run_telegram_bot(categorizer):
            # Bot is running, don't exit
            return
        else:
            print("Failed to start Telegram bot. Running in standalone mode.")
    
    # Process files
    processed, failed = categorizer.scan_and_process()
    print(f"Processing complete. Processed: {processed}, Failed: {failed}")

if __name__ == "__main__":
    main()