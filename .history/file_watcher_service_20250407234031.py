#!/usr/bin/env python3
# file_watcher.py - Monitors download directory and processes media files for Jellyfin

import os
import time
import logging
import re
import json
import shutil
import requests
import threading
import cmd
import sys
import traceback
from pathlib import Path
from logging.handlers import RotatingFileHandler
from difflib import SequenceMatcher
from typing import Dict, Tuple, Optional, List, Any, Callable
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Utility modules (imported from separate files in a real implementation)
class TelegramRateLimiter:
    """Rate limiter for Telegram API calls."""
    def __init__(self):
        self.locks = {}
        self.master_lock = threading.Lock()
        self.last_calls = {}
        self.min_interval = 0.5  # seconds between calls
    
    def wait_if_needed(self, key):
        with self.master_lock:
            current_time = time.time()
            if key in self.last_calls:
                elapsed = current_time - self.last_calls[key]
                if elapsed < self.min_interval:
                    time.sleep(self.min_interval - elapsed)
            self.last_calls[key] = time.time()

class ApiRateLimiter:
    """Rate limiter for TMDb API calls."""
    def __init__(self, requests_per_second=4):
        self.interval = 1.0 / requests_per_second
        self.last_request = 0
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_request
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.last_request = time.time()

# Create instances for rate limiters
telegram_message_limiter = TelegramRateLimiter()
api_rate_limiter = ApiRateLimiter()

# Configuration
CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "paths": {
        "telegram_download_dir": "telegram_download_dir",
        "movies_dir": "movies_dir",
        "tv_shows_dir": "tv_shows_dir",
        "unmatched_dir": "unmatched_dir"
    },
    "tmdb": {
        "api_key": "api_key"
    },
    "logging": {
        "level": "INFO",
        "max_size_mb": 10,
        "backup_count": 5
    },
    "notification": {
        "enabled": True,
        "method": "telegram",  # Options: "print", "telegram", "pushbullet"
        "telegram": {
            "bot_token": "bot_token",
            "chat_id": "chat_id"
        }
    },
    "process_existing_files": True,
    "max_worker_threads": 3,
    "lock_timeout_minutes": 60,
    "file_stabilization_seconds": 2,
    "manual_categorization": {
        "session_timeout_seconds": 300,  # 5 minutes
        "media_types": ["movie", "tv_show", "anime", "documentary", "other"]
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


class ProcessingManager:
    """Manages locks for file processing to prevent concurrent processing of the same file."""
    def __init__(self, timeout_minutes: int = 60):
        self._locks = {}
        self._master_lock = threading.Lock()
        self._timeout_seconds = timeout_minutes * 60
    
    def acquire_lock(self, filepath: str) -> bool:
        """Try to acquire a lock for processing a file.
        
        Args:
            filepath: The path to the file to lock
            
        Returns:
            bool: True if lock was acquired, False if file is already locked
        """
        with self._master_lock:
            if filepath in self._locks:
                # Check if lock is stale
                if time.time() - self._locks[filepath] > self._timeout_seconds:
                    self._locks[filepath] = time.time()
                    return True
                return False
            self._locks[filepath] = time.time()
            return True
    
    def release_lock(self, filepath: str) -> None:
        """Release a processing lock.
        
        Args:
            filepath: The path to the file to unlock
        """
        with self._master_lock:
            if filepath in self._locks:
                del self._locks[filepath]
    
    def cleanup_stale_locks(self) -> int:
        """Remove stale locks.
        
        Returns:
            int: Number of locks removed
        """
        with self._master_lock:
            current_time = time.time()
            stale_files = [f for f, t in self._locks.items() 
                          if current_time - t > self._timeout_seconds]
            for filepath in stale_files:
                del self._locks[filepath]
            return len(stale_files)


class ProcessingProgress:
    """Tracks and reports progress of file processing."""
    def __init__(self, filename: str, notify_callback: Callable):
        """Initialize a progress tracker.
        
        Args:
            filename: Name of the file being processed
            notify_callback: Function to call for progress notifications
        """
        self.filename = filename
        self.notify = notify_callback
        self.start_time = time.time()
    
    def start(self) -> None:
        """Mark the start of processing."""
        self.notify(f"📝 Started processing: <b>{self.filename}</b>", "info")
    
    def update(self, stage: str, details: str = "") -> None:
        """Update processing status.
        
        Args:
            stage: Current stage of processing
            details: Additional details about the current stage
        """
        msg = f"🔄 Processing <b>{self.filename}</b>\nStage: {stage}"
        if details:
            msg += f"\n{details}"
        self.notify(msg, "progress")
    
    def complete(self, success: bool = True, destination: str = "") -> None:
        """Mark processing as complete.
        
        Args:
            success: Whether processing was successful
            destination: Where the file was moved to
        """
        elapsed = time.time() - self.start_time
        if success:
            msg = f"✅ Processed <b>{self.filename}</b> in {elapsed:.1f}s"
            if destination:
                msg += f"\nMoved to: {destination}"
            self.notify(msg, "success")
        else:
            self.notify(f"❌ Failed to process <b>{self.filename}</b> after {elapsed:.1f}s", "error")


class TmdbClient:
    """Client for interacting with The Movie Database API."""
    def __init__(self, api_key: str):
        """Initialize the TMDb client.
        
        Args:
            api_key: TMDb API key
        """
        self.api_key = api_key
        self.logger = logging.getLogger("TmdbClient")
    
    def search_movie(self, query: str, year: Optional[str] = None) -> Dict[str, Any]:
        """Search for a movie.
        
        Args:
            query: Movie title to search for
            year: Optional year to filter results
            
        Returns:
            TMDb API response as a dictionary
        """
        url = "https://api.themoviedb.org/3/search/movie"
        params = {"api_key": self.api_key, "query": query, "page": 1}
        if year:
            params["year"] = year
        
        return self._make_request(url, params)
    
    def search_tv(self, query: str) -> Dict[str, Any]:
        """Search for a TV show.
        
        Args:
            query: TV show title to search for
            
        Returns:
            TMDb API response as a dictionary
        """
        url = "https://api.themoviedb.org/3/search/tv"
        params = {"api_key": self.api_key, "query": query, "page": 1}
        
        return self._make_request(url, params)
    
    def _make_request(self, url: str, params: Dict[str, Any], retry_count: int = 3) -> Dict[str, Any]:
        """Make a request to TMDb API with rate limiting and retries.
        
        Args:
            url: API endpoint URL
            params: Query parameters
            retry_count: Number of retries on failure
            
        Returns:
            API response as a dictionary
            
        Raises:
            requests.RequestException: If request fails after all retries
        """
        for attempt in range(retry_count):
            try:
                # Wait if being rate limited
                api_rate_limiter.wait_if_needed()
                
                print(f"   > Making TMDb API request to: {url.split('/')[-1]}")
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as e:
                if e.response.status_code == 429 and attempt < retry_count - 1:  # Rate limit exceeded
                    retry_after = int(e.response.headers.get('Retry-After', 2))
                    self.logger.warning(f"TMDb API rate limit reached. Waiting {retry_after}s")
                    print(f"   > TMDb API rate limit reached. Waiting {retry_after}s")
                    time.sleep(retry_after)
                else:
                    self.logger.error(f"HTTP Error: {str(e)}")
                    if attempt == retry_count - 1:
                        raise
            except (requests.ConnectionError, requests.Timeout) as e:
                self.logger.error(f"Network error: {str(e)}")
                if attempt == retry_count - 1:
                    raise
                backoff = 2 ** attempt
                print(f"   > Network error. Retrying in {backoff}s")
                time.sleep(backoff)
            except Exception as e:
                self.logger.error(f"Unexpected error: {str(e)}")
                if attempt == retry_count - 1:
                    raise


class NotificationService:
    """Service for sending notifications."""
    def __init__(self, config: Dict[str, Any]):
        """Initialize the notification service.
        
        Args:
            config: Notification configuration
        """
        self.config = config
        self.logger = logging.getLogger("Notifications")
        self.enabled = config.get("enabled", False)
        self.method = config.get("method", "print")
        self.waiting_for_response = False
        self.response = None
        self.response_lock = threading.Lock()
        self.response_event = threading.Event()
    
    def notify(self, message: str, level: str = "info") -> None:
        """Send a notification.
        
        Args:
            message: Notification message
            level: Message importance level (info, success, warning, error, progress)
        """
        if not self.enabled:
            return

        if self.method == "print":
            print(f"\n[NOTIFICATION] {message}")

        elif self.method == "telegram":
            try:
                # Get Telegram config           
                telegram_config = self.config.get("telegram", {})
                bot_token = telegram_config.get("bot_token")
                chat_id = telegram_config.get("chat_id")

                if not bot_token or not chat_id:
                    self.logger.warning("Telegram configuration incomplete")
                    return
                
                # Wait if being rate limited
                telegram_message_limiter.wait_if_needed(f"notify_{bot_token}")

                # Format message based on level
                emoji = {
                    "info": "ℹ️",
                    "success": "✅",
                    "warning": "⚠️",
                    "error": "❌",
                    "progress": "🔄"
                }.get(level, "ℹ️")

                formatted_message = f"{emoji} {message}"

                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                params = {
                    "chat_id": chat_id,
                    "text": formatted_message,
                    "parse_mode": "HTML"
                }

                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 429:  # Rate limit hit
                    retry_after = int(response.headers.get('Retry-After', 2))
                    self.logger.warning(f"Telegram rate limit hit, waiting {retry_after}s")
                    time.sleep(retry_after)
                    # Retry the request
                    response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()

            except requests.RequestException as e:
                self.logger.error(f"Network error sending Telegram notification: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error sending notification: {str(e)}")

    def wait_for_response(self, prompt: str, timeout: int = 60) -> Optional[str]:
        """Send a prompt and wait for a user response.
        
        Args:
            prompt: The message to send
            timeout: Time to wait for response in seconds
            
        Returns:
            User response text or None if timed out
        """
        if self.method == "print":
            # For local testing, just use input()
            print(f"\n[PROMPT] {prompt}")
            return input("Enter response: ")
        
        elif self.method == "telegram":
            try:
                with self.response_lock:
                    self.waiting_for_response = True
                    self.response = None
                    self.response_event.clear()
                
                # Send the prompt
                telegram_config = self.config.get("telegram", {})
                bot_token = telegram_config.get("bot_token")
                chat_id = telegram_config.get("chat_id")
                
                if not bot_token or not chat_id:
                    self.logger.warning("Telegram configuration incomplete")
                    return None
                
                telegram_message_limiter.wait_if_needed(f"prompt_{bot_token}")
                
                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                params = {
                    "chat_id": chat_id,
                    "text": f"🔹 {prompt}",
                    "parse_mode": "HTML"
                }
                
                requests.get(url, params=params, timeout=10)
                
                # Wait for response with timeout
                response_received = self.response_event.wait(timeout)
                
                with self.response_lock:
                    self.waiting_for_response = False
                    if response_received and self.response:
                        return self.response
                    else:
                        # Send timeout message
                        self.notify("⏱️ Response timeout. Operation cancelled.", "warning")
                        return None
                
            except Exception as e:
                self.logger.error(f"Error waiting for response: {str(e)}")
                with self.response_lock:
                    self.waiting_for_response = False
                return None
    
    def set_response(self, text: str) -> None:
        """Set a response received from user (used by Telegram update handler).
        
        Args:
            text: Response text from user
        """
        with self.response_lock:
            if self.waiting_for_response:
                self.response = text
                self.response_event.set()


class FileMover:
    """Handles moving files to their correct locations."""
    def __init__(self, movies_dir: str, tv_shows_dir: str, unmatched_dir: str):
        """Initialize the file mover.
        
        Args:
            movies_dir: Directory for movies
            tv_shows_dir: Directory for TV shows
            unmatched_dir: Directory for unmatched files
        """
        self.movies_dir = movies_dir
        self.tv_shows_dir = tv_shows_dir
        self.unmatched_dir = unmatched_dir
        self.logger = logging.getLogger("FileMover")
    
    def move_file(self, filepath: str, media_type: str, metadata: Dict[str, Any]) -> Tuple[bool, str]:
        """Move a file to the appropriate directory with proper naming.
        
        Args:
            filepath: Path to the file to move
            media_type: Type of media (tv, movie, unknown)
            metadata: Metadata about the file
            
        Returns:
            (success, destination_path) tuple
        """
        try:
            filename = os.path.basename(filepath)
            extension = os.path.splitext(filename)[1]
            
            if media_type == "tv":
                show_name = self.sanitize_filename(metadata.get("show_name", "Unknown Show"))
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
                
            elif media_type == "movie":
                movie_name = self.sanitize_filename(metadata.get("movie_name", "Unknown Movie"))
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
                
            else:
                # For unknown media types
                os.makedirs(self.unmatched_dir, exist_ok=True)
                destination = os.path.join(self.unmatched_dir, filename)
            
            print(f"     - Final path: {destination}")
            
            # Check if destination exists
            destination = self.get_unique_filename(destination)
            
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
                
            return True, destination
            
        except OSError as e:
            self.logger.error(f"File system error moving file: {str(e)}")
            print(f"   > ERROR: File system error: {str(e)}")
            return False, ""
        except Exception as e:
            self.logger.error(f"Failed to move file: {str(e)}")
            print(f"   > ERROR: Failed to move file: {str(e)}")
            return False, ""
    
    def format_filename(self, media_type: str, metadata: Dict[str, Any]) -> str:
        """Format filename according to Jellyfin naming conventions.
        
        Args:
            media_type: Type of media (tv, movie)
            metadata: Metadata about the file
            
        Returns:
            Formatted filename (without extension)
        """
        if media_type == "tv":
            show_name = metadata.get("show_name", "Unknown Show")
            season = metadata.get("season", 1)
            episode = metadata.get("episode", 1)
            
            # Format: ShowName - S01E01
            return f"{show_name} - S{season:02d}E{episode:02d}"
        
        elif media_type == "movie":
            movie_name = metadata.get("movie_name", "Unknown Movie")
            year = metadata.get("year", "")
            
            # Format: MovieName (Year)
            if year:
                return f"{movie_name} ({year})"
            return movie_name
        
        else:
            return metadata.get("name", "Unknown Media")
    
    def get_unique_filename(self, filepath: str) -> str:
        """Get a unique filename by appending a counter if necessary.
        
        Args:
            filepath: Original path
            
        Returns:
            Path that doesn't exist yet
        """
        if not os.path.exists(filepath):
            return filepath
            
        base, ext = os.path.splitext(filepath)
        counter = 1
        while os.path.exists(f"{base} ({counter}){ext}"):
            counter += 1
        new_path = f"{base} ({counter}){ext}"
        print(f"     - File already exists! Using alternate path: {new_path}")
        return new_path
    
    def sanitize_filename(self, name: str) -> str:
        """Remove any characters that might be invalid for filenames.
        
        Args:
            name: Filename to sanitize
            
        Returns:
            Sanitized filename
        """
        # Replace potentially problematic characters
        invalid_chars = r'<>:"/\|?*'
        for char in invalid_chars:
            name = name.replace(char, '')
        return name.strip()


class ManualCategorizer:
    """Handles manual categorization of unmatched media files."""
    def __init__(self, notification_service: NotificationService, file_mover: FileMover, 
                 unmatched_dir: str, config: Dict[str, Any]):
        """Initialize the manual categorizer.
        
        Args:
            notification_service: Service for sending notifications and receiving responses
            file_mover: Service for moving files
            unmatched_dir: Directory containing unmatched files
            config: Configuration dictionary
        """
        self.notification = notification_service
        self.file_mover = file_mover
        self.unmatched_dir = unmatched_dir
        self.logger = logging.getLogger("ManualCategorizer")
        self.session_timeout = config.get("manual_categorization", {}).get("session_timeout_seconds", 300)
        self.media_types = config.get("manual_categorization", {}).get("media_types", 
                                     ["movie", "tv_show", "anime", "documentary", "other"])
        self.session_lock = threading.Lock()
        self.session_active = False
        
    def get_unmatched_files(self) -> List[str]:
        """Get list of unmatched video files.
        
        Returns:
            List of unmatched video file paths
        """
        if not os.path.exists(self.unmatched_dir):
            return []
            
        unmatched_files = []
        for filename in os.listdir(self.unmatched_dir):
            filepath = os.path.join(self.unmatched_dir, filename)
            # Only include video files, not metadata files
            if os.path.isfile(filepath) and os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS:
                unmatched_files.append(filepath)
        
        return unmatched_files
    
    def validate_year(self, year_str: str) -> Tuple[bool, Optional[str], str]:
        """Validate year input.
        
        Args:
            year_str: Year as string
            
        Returns:
            (is_valid, validated_year, error_message) tuple
        """
        try:
            year = int(year_str.strip())
            current_year = datetime.now().year
            
            if 1900 <= year <= current_year + 5:  # Allow up to 5 years in the future
                return True, str(year), ""
            else:
                return False, None, f"Year must be between 1900 and {current_year + 5}"
        except ValueError:
            return False, None, "Year must be a 4-digit number"
    
    def validate_season_episode(self, num_str: str) -> Tuple[bool, Optional[int], str]:
        """Validate season or episode number.
        
        Args:
            num_str: Season or episode number as string
            
        Returns:
            (is_valid, validated_number, error_message) tuple
        """
        try:
            num = int(num_str.strip())
            if num > 0:
                return True, num, ""
            else:
                return False, None, "Number must be greater than 0"
        except ValueError:
            return False, None, "Must be a valid number"
    
    def handle_unmatched_command(self) -> None:
        """Handle the /unmatched command.
        
        List all unmatched files and allow user to select one for categorization.
        """
        # Check if a session is already active
        with self.session_lock:
            if self.session_active:
                self.notification.notify("⚠️ A manual categorization session is already in progress.", "warning")
                return
            self.session_active = True
        
        try:
            # Get list of unmatched files
            unmatched_files = self.get_unmatched_files()
            
            if not unmatched_files:
                self.notification.notify("📂 No unmatched files found.", "info")
                return
            
            # Create numbered list of files
            files_message = "📋 <b>Unmatched Files:</b>\n\n"
            for i, filepath in enumerate(unmatched_files, 1):
                filename = os.path.basename(filepath)
                files_message += f"{i}. {filename}\n"
            
            files_message += "\nEnter a number to select a file, or 'cancel' to abort."
            self.notification.notify(files_message, "info")
            
            # Wait for user selection
            selection = self.notification.wait_for_response("Select a file by number:", 
                                                           timeout=self.session_timeout)
            
            if not selection or selection.lower() == 'cancel':
                self.notification.notify("❌ Selection cancelled.", "info")
                return
            
            # Validate selection
            try:
                file_index = int(selection) - 1
                if file_index < 0 or file_index >= len(unmatched_files):
                    self.notification.notify(f"❌ Invalid selection. Please enter a number between 1 and {len(unmatched_files)}.", "error")
                    return
            except ValueError:
                self.notification.notify("❌ Invalid input. Please enter a number.", "error")
                return
            
            # Get selected file
            selected_file = unmatched_files[file_index]
            filename = os.path.basename(selected_file)
            
            # Confirm selection with user
            confirm = self.notification.wait_for_response(
                f"Selected: <b>{filename}</b>\nIs this correct? (yes/no)",
                timeout=self.session_timeout
            )
            
            if not confirm or confirm.lower() != 'yes':
                self.notification.notify("❌ Selection cancelled.", "info")
                return
            
            # Process the selected file
            success = self.categorize_file(selected_file)
            if not success:
                self.notification.notify("❌ Categorization failed.", "error")
            
        except Exception as e:
            self.logger.error(f"Error handling unmatched command: {str(e)}")
            self.notification.notify(f"❌ An error occurred: {str(e)}", "error")
        finally:
            # Release the session lock
            with self.session_lock:
                self.session_active = False
    
    def categorize_file(self, filepath: str) -> bool:
        """Manually categorize a file.
        
        Args:
            filepath: Path to the file to categorize
            
        Returns:
            True if categorization was successful, False otherwise
        """
        try:
            filename = os.path.basename(filepath)
            
            # Check if file still exists
            if not os.path.exists(filepath):
                self.notification.notify(f"❌ File no longer exists: {filename}", "error")
                return False
            
            # Step 1: Select media type
            type_message = "📁 <b>Select Media Type:</b>\n\n"
            for i, media_type in enumerate(self.media_types, 1):
                type_message += f"{i}. {media_type.replace('_', ' ').title()}\n"
            
            type_message += "\nEnter a number to select a media type, or 'cancel' to abort."
            self.notification.notify(type_message, "info")
            
            # Wait for user selection
            type_selection = self.notification.wait_for_response("Select media type:", 
                                                               timeout=self.session_timeout)
            
            if not type_selection or type_selection.lower() == 'cancel':
                self.notification.notify("❌ Categorization cancelled.", "info")
                return False
            
            # Validate selection
            try:
                type_index = int(type_selection) - 1
                if type_index < 0 or type_index >= len(self.media_types):
                    self.notification.notify(f"❌ Invalid selection. Please enter a number between 1 and {len(self.media_types)}.", "error")
                    return False
            except ValueError:
                self.notification.notify("❌ Invalid input. Please enter a number.", "error")
                return False
            
            selected_type = self.media_types[type_index]
            
            # Step 2: Collect required metadata based on type
            metadata = {}
            
            if selected_type == "movie":
                # Get movie title
                while True:
                    title = self.notification.wait_for_response(
                        f"Enter the movie title for <b>{filename}</b>:",
                        timeout=self.session_timeout
                    )
                    
                    if not title:
                        self.notification.notify("❌ Categorization cancelled due to timeout.", "warning")
                        return False
                    
                    if title.lower() == 'cancel':
                        self.notification.notify("❌ Categorization cancelled.", "info")
                        return False
                    
                    if title.strip():
                        metadata["movie_name"] = title.strip()
                        break
                    else:
                        self.notification.notify("⚠️ Movie title cannot be empty. Please try again.", "warning")
                
                # Get year
                while True:
                    year = self.notification.wait_for_response(
                        f"Enter the release year for <b>{metadata['movie_name']}</b>:",
                        timeout=self.session_timeout
                    )
                    
                    if not year:
                        self.notification.notify("❌ Categorization cancelled due to timeout.", "warning")
                        return False
                    
                    if year.lower() == 'cancel':
                        self.notification.notify("❌ Categorization cancelled.", "info")
                        return False
                    
                    valid, validated_year, error = self.validate_year(year)
                    if valid:
                        metadata["year"] = validated_year
                        break
                    else:
                        self.notification.notify(f"⚠️ {error}. Please try again.", "warning")
                
            elif selected_type == "tv_show" or selected_type == "anime":
                # Get show name
                while True:
                    show_name = self.notification.wait_for_response(
                        f"Enter the show name for <b>{filename}</b>:",
                        timeout=self.session_timeout
                    )
                    
                    if not show_name:
                        self.notification.notify("❌ Categorization cancelled due to timeout.", "warning")
                        return False
                    
                    if show_name.lower() == 'cancel':
                        self.notification.notify("❌ Categorization cancelled.", "info")
                        return False
                    
                    if show_name.strip():
                        metadata["show_name"] = show_name.strip()
                        break
                    else:
                        self.notification.notify("⚠️ Show name cannot be empty. Please try again.", "warning")
                
                # Get season number
                while True:
                    season = self.notification.wait_for_response(
                        f"Enter the season number for <b>{metadata['show_name']}</b>:",
                        timeout=self.session_timeout
                    )
                    
                    if not season:
                        self.notification.notify("❌ Categorization cancelled due to timeout.", "warning")
                        return False
                    
                    if season.lower() == 'cancel':
                        self.notification.notify("❌ Categorization cancelled.", "info")
                        return False
                    
                    valid, validated_season, error = self.validate_season_episode(season)
                    if valid:
                        metadata["season"] = validated_season
                        break
                    else:
                        self.notification.notify(f"⚠️ {error}. Please try again.", "warning")
                
                # Get episode number
                while True:
                    episode = self.notification.wait_for_response(
                        f"Enter the episode number for Season {metadata['season']} of <b>{metadata['show_name']}</b>:",
                        timeout=self.session_timeout
                    )
                    
                    if not episode:
                        self.notification.notify("❌ Categorization cancelled due to timeout.", "warning")
                        return False
                    
                    if episode.lower() == 'cancel':
                        self.notification.notify("❌ Categorization cancelled.", "info")
                        return False
                    
                    valid, validated_episode, error = self.validate_season_episode(episode)
                    if valid:
                        metadata["episode"] = validated_episode
                        break
                    else:
                        self.notification.notify(f"⚠️ {error}. Please try again.", "warning")
            
            else:
                # For other types, just get a title
                while True:
                    title = self.notification.wait_for_response(
                        f"Enter a title for this {selected_type.replace('_', ' ')}:",
                        timeout=self.session_timeout
                    )
                    
                    if not title:
                        self.notification.notify("❌ Categorization cancelled due to timeout.", "warning")
                        return False
                    
                    if title.lower() == 'cancel':
                        self.notification.notify("❌ Categorization cancelled.", "info")
                        return False
                    
                    if title.strip():
                        metadata["name"] = title.strip()
                        break
                    else:
                        self.notification.notify("⚠️ Title cannot be empty. Please try again.", "warning")
            
            # Step 3: Display summary and confirm
            if selected_type == "movie":
                summary = (f"📝 <b>Summary:</b>\n\n"
                          f"File: {filename}\n"
                          f"Type: Movie\n"
                          f"Title: {metadata['movie_name']}\n"
                          f"Year: {metadata['year']}")
            elif selected_type == "tv_show" or selected_type == "anime":
                summary = (f"📝 <b>Summary:</b>\n\n"
                          f"File: {filename}\n"
                          f"Type: {selected_type.replace('_', ' ').title()}\n"
                          f"Show: {metadata['show_name']}\n"
                          f"Season: {metadata['season']}\n"
                          f"Episode: {metadata['episode']}")
            else:
                summary = (f"📝 <b>Summary:</b>\n\n"
                          f"File: {filename}\n"
                          f"Type: {selected_type.replace('_', ' ').title()}\n"
                          f"Title: {metadata['name']}")
            
            summary += "\n\nConfirm these details? (yes/no)"
            
            # Get confirmation
            confirm = self.notification.wait_for_response(summary, timeout=self.session_timeout)
            
            if not confirm or confirm.lower() != 'yes':
                self.notification.notify("❌ Categorization cancelled.", "info")
                return False
            
            # Step 4: Move the file
            media_type = "movie" if selected_type == "movie" else "tv" if selected_type in ["tv_show", "anime"] else "unknown"
            
            # Check disk space before moving
            try:
                file_size = os.path.getsize(filepath)
                target_dir = ""
                
                if media_type == "movie":
                    target_dir = self.file_mover.movies_dir
                elif media_type == "tv":
                    target_dir = self.file_mover.tv_shows_dir
                
                if target_dir:
                    # Get disk stats
                    stats = os.statvfs(target_dir)
                    free_space = stats.f_bavail * stats.f_frsize
                    
                    if file_size > free_space:
                        self.notification.notify(f"❌ Not enough disk space to move file. Need {file_size/(1024*1024):.1f} MB, but only {free_space/(1024*1024):.1f} MB available.", "error")
                        return False
            except Exception as e:
                self.logger.warning(f"Error checking disk space: {str(e)}")
                # Continue anyway, the move operation will fail if there's no space
            
            # Move the file
            self.notification.notify(f"🔄 Moving file to {selected_type.replace('_', ' ')} library...", "progress")
            success, destination = self.file_mover.move_file(filepath, media_type, metadata)
            
            if success:
                self.notification.notify(f"✅ Successfully categorized and moved:\n<b>{filename}</b>\nTo: {destination}", "success")
                return True
            else:
                self.notification.notify(f"❌ Failed to move file: {filename}", "error")
                return False
            
        except Exception as e:
            self.logger.error(f"Error categorizing file: {traceback.format_exc()}")
            self.notification.notify(f"❌ Error categorizing file: {str(e)}", "error")
            return False


# TelegramCommandHandler class for handling Telegram bot commands
class TelegramCommandHandler:
    """Handles Telegram bot commands."""
    def __init__(self, config: Dict[str, Any], manual_categorizer: ManualCategorizer):
        """Initialize the command handler.
        
        Args:
            config: Notification configuration
            manual_categorizer: Manual categorizer instance
        """
        self.config = config
        self.manual_categorizer = manual_categorizer
        self.logger = logging.getLogger("TelegramCommands")
        self.last_update_id = 1000000  # Added offset of 1000000
        self.notification = None  # Will be set later
        self.should_stop = False
        self.polling_thread = None
    
    def set_notification_service(self, notification_service: NotificationService):
        """Set the notification service.
        
        Args:
            notification_service: Notification service to use
        """
        self.notification = notification_service
    
    def start_polling(self):
        """Start polling for Telegram commands in a separate thread."""
        if self.config.get("method") != "telegram":
            return
            
        telegram_config = self.config.get("telegram", {})
        bot_token = telegram_config.get("bot_token")
        
        if not bot_token:
            self.logger.warning("Telegram bot token not configured")
            return
        
        self.polling_thread = threading.Thread(target=self._polling_loop)
        self.polling_thread.daemon = True
        self.polling_thread.start()
        self.logger.info("Started Telegram command polling")
    
    def stop_polling(self):
        """Stop the polling thread."""
        self.should_stop = True
        if self.polling_thread:
            self.polling_thread.join(timeout=5)
    
    def _polling_loop(self):
        """Main polling loop for getting Telegram updates."""
        telegram_config = self.config.get("telegram", {})
        bot_token = telegram_config.get("bot_token")
        chat_id = telegram_config.get("chat_id")
        
        if not bot_token or not chat_id:
            return
        
        url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        
        while not self.should_stop:
            try:
                params = {
                    "timeout": 30,
                    "offset": self.last_update_id + 1,
                    "allowed_updates": ["message"]  # Only listen for messages
                }
                
                response = requests.get(url, params=params, timeout=35)
                if response.status_code == 200:
                    updates = response.json()
                    if updates["ok"] and updates["result"]:
                        for update in updates["result"]:
                            self.last_update_id = max(self.last_update_id, update["update_id"])
                            self._process_update(update)
                time.sleep(1)  # Add small delay to reduce API calls
            except requests.RequestException as e:
                self.logger.error(f"Error polling Telegram: {str(e)}")
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Unexpected error in Telegram polling: {str(e)}")
                time.sleep(5)
    
    def _process_update(self, update: Dict[str, Any]):
        """Process a Telegram update.
        
        Args:
            update: Telegram update object
        """
        try:
            if "message" not in update:
                return
                
            message = update["message"]
            chat_id = str(message["chat"]["id"])
            config_chat_id = self.config.get("telegram", {}).get("chat_id", "")
            
            # Only process messages from the configured chat ID
            if chat_id != config_chat_id:
                return
            
            # Check if it's a command
            if "text" in message:
                text = message["text"]
                
                # Check if notification service is waiting for a response
                if self.notification and self.notification.waiting_for_response:
                    self.notification.set_response(text)
                    return
                
                # Process commands
                if text.startswith("/"):
                    self._handle_command(text)
        except Exception as e:
            self.logger.error(f"Error processing Telegram update: {str(e)}")
    
    def _handle_command(self, command: str):
        """Handle a Telegram command.
        
        Args:
            command: Command text
        """
        command = command.lower().split()[0]  # Get the command part
        
        if command == "/fw_unmatched":
            # Handle unmatched command
            self.logger.info("Received /fw_unmatched command")
            threading.Thread(target=self.manual_categorizer.handle_unmatched_command).start()
        elif command == "/fw_help":
            help_text = (
                "📋 <b>Available Commands:</b>\n\n"
                "/fw_unmatched - List and categorize unmatched media files\n"
                "/fw_help - Show this help message"
            )
            if self.notification:
                self.notification.notify(help_text, "info")


class MediaCategorizer:
    """Main class for categorizing and managing media files."""
    def __init__(self, config_path: str = CONFIG_FILE):
        """Initialize the Media Categorizer with configuration.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_or_create_config(config_path)
        self.config = self._validate_config(self.config)
        self._setup_logging()
        self._setup_directories()
        
        # Initialize notification service
        self.notification = NotificationService(self.config["notification"])
        
        # Initialize services
        self.tmdb_client = TmdbClient(self.config["tmdb"]["api_key"])
        self.file_mover = FileMover(
            self.movies_dir, 
            self.tv_shows_dir, 
            self.unmatched_dir
        )
        
        # Initialize manual categorizer
        self.manual_categorizer = ManualCategorizer(
            self.notification,
            self.file_mover,
            self.unmatched_dir,
            self.config
        )
        
        # Initialize Telegram command handler
        self.telegram_handler = TelegramCommandHandler(
            self.config["notification"],
            self.manual_categorizer
        )
        self.telegram_handler.set_notification_service(self.notification)
        
        # Initialize processing manager
        self.processing_manager = ProcessingManager(
            timeout_minutes=self.config.get("lock_timeout_minutes", 60)
        )
        
        # Initialize thread pool for parallel processing
        self.max_workers = self.config.get("max_worker_threads", 3)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Print banner
        print("\n" + "="*80)
        print(" "*30 + "MEDIA WATCHER STARTED")
        print("="*80 + "\n")
        
    def _load_or_create_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from file or create default if not exists.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        try:
            if not os.path.exists(config_path):
                os.makedirs(os.path.dirname(config_path) or '.', exist_ok=True)
                with open(config_path, 'w') as f:
                    json.dump(DEFAULT_CONFIG, f, indent=4)
                print(f"Created default configuration file at {config_path}")
                print("Please edit this file with your TMDb API key and directory paths")
                return DEFAULT_CONFIG
            
            with open(config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Error: Configuration file {config_path} contains invalid JSON")
            print("Using default configuration")
            return DEFAULT_CONFIG
        except Exception as e:
            print(f"Error loading configuration: {str(e)}")
            print("Using default configuration")
            return DEFAULT_CONFIG
    
    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate configuration and set defaults for missing values.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Validated configuration dictionary
        """
        # Start with a deep copy of the default config
        validated = json.loads(json.dumps(DEFAULT_CONFIG))
        
        # Update with provided config, preserving structure
        def update_recursive(d, u):
            for k, v in u.items():
                if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                    update_recursive(d[k], v)
                else:
                    d[k] = v
        
        update_recursive(validated, config)
        
        # Check TMDb API key
        if not validated["tmdb"]["api_key"] or validated["tmdb"]["api_key"] == "api_key":
            print("WARNING: TMDb API key not configured. Media identification will fail.")
        
        # Set default directories if not specified
        for path_key in validated["paths"]:
            if not validated["paths"][path_key]:
                validated["paths"][path_key] = DEFAULT_CONFIG["paths"][path_key]
                print(f"WARNING: No '{path_key}' specified, using default: {validated['paths'][path_key]}")
        
        return validated
    
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
        
        for dir_path in [self.download_dir, self.movies_dir, self.tv_shows_dir, self.unmatched_dir]:
            try:
                os.makedirs(dir_path, exist_ok=True)
            except OSError as e:
                self.logger.error(f"Failed to create directory {dir_path}: {str(e)}")
                print(f"ERROR: Failed to create directory {dir_path}: {str(e)}")
        
        print(f"Monitoring download directory: {os.path.abspath(self.download_dir)}")
        print(f"Movies will be saved to: {os.path.abspath(self.movies_dir)}")
        print(f"TV Shows will be saved to: {os.path.abspath(self.tv_shows_dir)}")
        print(f"Unmatched content will be saved to: {os.path.abspath(self.unmatched_dir)}")
    
    def notify(self, message: str, level: str = "info"):
        """Send notification through notification service.
        
        Args:
            message: Message to send
            level: Message importance level
        """
        self.notification.notify(message, level)
    
    def is_video_file(self, filename: str) -> bool:
        """Check if file has a video extension.
        
        Args:
            filename: Filename to check
            
        Returns:
            True if file is a video, False otherwise
        """
        return os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS
    
    def clean_name(self, name: str) -> str:
        """Clean up a name by replacing dots, underscores with spaces.
        
        Args:
            name: Name to clean
            
        Returns:
            Cleaned name
        """
        return re.sub(r'[._]', ' ', name).strip()
    
    def calculate_similarity(self, a: str, b: str) -> float:
        """Calculate string similarity between two strings.
        
        Args:
            a: First string
            b: Second string
            
        Returns:
            Similarity score (0.0 to 1.0)
        """
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    def initial_categorization(self, filename: str) -> Tuple[str, Dict[str, Any]]:
        """
        Initial categorization using regex patterns.
        
        Args:
            filename: Filename to categorize
            
        Returns:
            (media_type, metadata) tuple
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
        """Verify a TV show with TMDb API.
        
        Args:
            metadata: Initial TV show metadata
            
        Returns:
            (media_type, enhanced_metadata) tuple
        """
        show_name = metadata.get("show_name", "")
        print(f"   > Verifying TV show with TMDb: {show_name}")
        
        try:
            response = self.tmdb_client.search_tv(show_name)
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
        """Verify a movie with TMDb API.
        
        Args:
            metadata: Initial movie metadata
            
        Returns:
            (media_type, enhanced_metadata) tuple
        """
        movie_name = metadata.get("movie_name", "")
        year = metadata.get("year")
        print(f"   > Verifying movie with TMDb: {movie_name} ({year if year else 'Unknown year'})")
        
        try:
            # First attempt with year if available
            results = []
            if year:
                response = self.tmdb_client.search_movie(movie_name, year)
                results = response.get("results", [])
            
            # If no results with year or no year provided, try without year
            if not results:
                if year:
                    print(f"   > No results for '{movie_name} ({year})'. Trying without year...")
                response = self.tmdb_client.search_movie(movie_name)
                results = response.get("results", [])
            
            if not results:
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
        
        Args:
            metadata: Initial metadata with at least a name field
            
        Returns:
            (media_type, enhanced_metadata) tuple
        """
        name = metadata.get("name", "")
        print(f"   > Attempting to identify unknown media: {name}")
        
        # Try movie search first
        try:
            print(f"   > Checking if it might be a movie...")
            response = self.tmdb_client.search_movie(name)
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
            response = self.tmdb_client.search_tv(name)
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
        
        Args:
            media_type: Initial media type (tv, movie, unknown)
            metadata: Initial metadata
            
        Returns:
            (confirmed_type, enhanced_metadata) tuple
        """
        if media_type == "tv":
            return self.verify_tv_show(metadata)
        elif media_type == "movie":
            return self.verify_movie(metadata)
        else:
            return self.guess_media_type(metadata)
    
    def process_file(self, filepath: str) -> bool:
        """Process a single file.
        
        Args:
            filepath: Path to the file to process
            
        Returns:
            True if processing was successful, False otherwise
        """
        # Skip if file doesn't exist
        if not os.path.exists(filepath):
            return False
            
        # Skip if file is already being processed
        if not self.processing_manager.acquire_lock(filepath):
            self.logger.debug(f"File already being processed: {filepath}")
            return False
        
        try:
            filename = os.path.basename(filepath)
            
            # Skip non-video files
            if not self.is_video_file(filepath):
                return False
            
            # Create progress tracker
            progress = ProcessingProgress(filename, self.notify)
            progress.start()
            
            # Check if file is still being modified
            try:
                file_size_before = os.path.getsize(filepath)
                time.sleep(1)
                file_size_after = os.path.getsize(filepath)
                
                if file_size_before != file_size_after:
                    print(f"   > File is still being modified: {filename}")
                    self.notify(f"⏳ File is still being modified:\n<b>{filename}</b>\nWill process later.", "warning")
                    return False
            except FileNotFoundError:
                print(f"   > File disappeared: {filename}")
                self.notify(f"❌ File disappeared while checking:\n<b>{filename}</b>",  "error")
                return False
            
            # Step 1: Initial categorization
            media_type, metadata = self.initial_categorization(filename)
            print(f"   > Initial categorization: {media_type}")
            progress.update("Analyzing", f"Initial type: {media_type}")
            
            # Step 2: TMDb verification
            progress.update("Verifying with TMDb", "Searching for matches...")
            confirmed_type, enhanced_metadata = self.verify_with_tmdb(media_type, metadata)
            
            # Format TMDb results message
            if confirmed_type == "movie":
                details = (f"🎬 Identified as Movie:\n"
                          f"<b>{enhanced_metadata.get('movie_name')}</b>\n"
                          f"Year: {enhanced_metadata.get('year', 'Unknown')}")
                print(f"   > Confirmed as movie: {enhanced_metadata.get('movie_name')} ({enhanced_metadata.get('year', 'Unknown')})")
            elif confirmed_type == "tv":
                details = (f"📺 Identified as TV Show:\n"
                          f"<b>{enhanced_metadata.get('show_name')}</b>\n"
                          f"Season: {enhanced_metadata.get('season', '?')}\n"
                          f"Episode: {enhanced_metadata.get('episode', '?')}")
                print(f"   > Confirmed as TV show: {enhanced_metadata.get('show_name')} S{enhanced_metadata.get('season', '?')}E{enhanced_metadata.get('episode', '?')}")
            else:
                details = f"❓ Unable to identify media type"
                print(f"   > Media type unknown")
            
            progress.update("Identification complete", details)
            
            # Step 3: Move to library
            progress.update("Moving to library", f"Type: {confirmed_type}")
            success, destination = self.file_mover.move_file(filepath, confirmed_type, enhanced_metadata)
            
            if success:
                progress.complete(True, destination)
                return True
            else:
                progress.complete(False)
                return False
                
        except Exception as e:
            self.logger.exception(f"Unexpected error processing {filepath}: {str(e)}")
            print(f"   > UNEXPECTED ERROR: {str(e)}")
            self.notify(f"❌ Error processing:\n<b>{os.path.basename(filepath)}</b>\n{str(e)}", "error")
            return False
        finally:
            # Release the processing lock
            self.processing_manager.release_lock(filepath)
    
    def process_existing_files(self):
        """Process all existing video files in the download directory."""
        self.logger.info("Processing existing files...")
        print("\n=== PROCESSING EXISTING FILES IN DOWNLOAD DIRECTORY ===")
        
        existing_files = []
        for root, _, files in os.walk(self.download_dir):
            for filename in files:
                if self.is_video_file(filename):
                    existing_files.append(os.path.join(root, filename))
        
        if existing_files:
            print(f"Found {len(existing_files)} existing video files to process:")
            for idx, filepath in enumerate(existing_files, 1):
                print(f"{idx}. {os.path.basename(filepath)}")
            
            # Process files in parallel using thread pool
            futures = []
            for filepath in existing_files:
                # Submit task to thread pool
                future = self.executor.submit(self.process_file, filepath)
                futures.append(future)
            
            # Wait for all tasks to complete
            for future in futures:
                future.result()
        else:
            print("No existing video files found in download directory.")


class FileEventHandler(FileSystemEventHandler):
    """Handler for file system events from watchdog."""
    def __init__(self, categorizer: MediaCategorizer):
        """Initialize the event handler.
        
        Args:
            categorizer: MediaCategorizer instance
        """
        self.categorizer = categorizer
        self.logger = categorizer.logger
        self.stabilization_time = categorizer.config.get("file_stabilization_seconds", 2)
    
    def on_created(self, event):
        """Handle file creation events."""
        if event.is_directory:
            return
            
        filepath = event.src_path
        
        # Skip hidden files and non-video files quickly
        filename = os.path.basename(filepath)
        if filename.startswith('.') or not self.categorizer.is_video_file(filepath):
            return
            
        # Wait for file to stabilize
        self.logger.debug(f"New file detected: {filepath}, waiting for it to stabilize...")
        
        # Submit task to process file after stabilization period
        time.sleep(self.stabilization_time)
        
        # Process the file if it still exists
        if os.path.exists(filepath):
            self.categorizer.executor.submit(self.categorizer.process_file, filepath)
    
    def on_closed(self, event):
        """Process a file when it's completely written and closed."""
        if event.is_directory:
            return
            
        filepath = event.src_path
        
        # Skip hidden files and non-video files quickly
        filename = os.path.basename(filepath)
        if filename.startswith('.') or not self.categorizer.is_video_file(filepath):
            return
            
        # Process the file if it still exists
        if os.path.exists(filepath):
            self.logger.debug(f"File closed event: {filepath}")
            self.categorizer.executor.submit(self.categorizer.process_file, filepath)
    
    def on_moved(self, event):
        """Process a file when it's moved into the watched directory."""
        if event.is_directory:
            return
            
        filepath = event.dest_path
        
        # Skip hidden files and non-video files quickly
        filename = os.path.basename(filepath)
        if filename.startswith('.') or not self.categorizer.is_video_file(filepath):
            return
            
        # Wait for file to stabilize
        self.logger.debug(f"File moved event: {filepath}")
        
        # Submit task to process file
        if os.path.exists(filepath):
            self.categorizer.executor.submit(self.categorizer.process_file, filepath)


def main():
    # Initialize the categorizer
    categorizer = MediaCategorizer()
    
    # Start Telegram command handler
    if categorizer.config["notification"]["enabled"] and categorizer.config["notification"]["method"] == "telegram":
        categorizer.telegram_handler.start_polling()
    
    # Process existing files in download directory
    if categorizer.config.get("process_existing_files", True):
        categorizer.process_existing_files()
    
    # Set up watchdog observer
    event_handler = FileEventHandler(categorizer)
    observer = Observer()
    observer.schedule(event_handler, categorizer.download_dir, recursive=True)
    
    categorizer.logger.info(f"Watching directory: {categorizer.download_dir}")
    
    print("\n" + "="*80)
    print(" "*30 + "WATCHING FOR NEW FILES")
    print("="*80)
    
    categorizer.notify("📡 Media Watcher started and ready for new files", "info")
    
    # Start watching
    observer.start()
    try:
        print("\nWaiting for new files... (Press Ctrl+C to exit)")
        while True:
            # Periodically clean up stale locks
            stale_locks = categorizer.processing_manager.cleanup_stale_locks()
            if stale_locks > 0:
                categorizer.logger.warning(f"Removed {stale_locks} stale processing locks")
            
            time.sleep(60)  # Check for stale locks every minute
    except KeyboardInterrupt:
        print("\nShutting down...")
        observer.stop()
        categorizer.telegram_handler.stop_polling()
        categorizer.executor.shutdown(wait=True)
    observer.join()
    print("Media Watcher has been stopped.")


if __name__ == "__main__":
    main()