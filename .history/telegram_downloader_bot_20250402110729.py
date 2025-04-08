#!/usr/bin/env python3
"""
telegram_downloader.py - Downloads media files from Telegram

A modular, asynchronous Telegram downloader for media files with support for
batch downloads, progress tracking, and queuing.
"""
import os
import time
import json
import logging
import asyncio
import random
import hashlib
import aiohttp
import telebot
import requests
from typing import Dict, List, Optional, Union, Any, Set, Deque
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from collections import deque
from telebot.async_telebot import AsyncTeleBot
from telethon import TelegramClient
from telethon.errors import FloodWaitError, AuthKeyError, SessionPasswordNeededError

# Configuration constants
CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "telegram": {
        "api_id": "api_id",
        "api_hash": "api_hash",
        "bot_token": "bot_token",
        "enabled": True
    },
    "paths": {
        "telegram_download_dir": "telegram_download_dir",
        "movies_dir": "/path/to/movies",
        "tv_shows_dir": "/path/to/tv_shows",
        "unmatched_dir": "/path/to/unmatched"
    },
    "logging": {
        "level": "INFO",
        "max_size_mb": 10,
        "backup_count": 5
    },
    "download": {
        "chunk_size": 1024 * 1024,  # 1MB
        "progress_update_interval": 5,  # seconds
        "max_retries": 3,
        "retry_delay": 5,  # seconds
        "max_concurrent_downloads": 3,  # concurrent downloads
        "verify_downloads": True
    }
}

# Global logger
logger = None

@dataclass
class DownloadTask:
    """Represents a single download task."""
    file_id: str
    file_path: str
    file_size: int
    chat_id: int
    message_id: int
    original_message: object = None
    file_name: str = ""
    batch_id: str = None  # For grouping files in the same batch
    position: int = 0  # Position in batch
    total_files: int = 1  # Total files in batch
    
    # Status tracking
    status: str = "queued"  # queued, downloading, completed, failed
    
    # Add timestamp for sorting/prioritization
    added_time: float = field(default_factory=time.time)
    
    # Progress tracking
    downloaded_bytes: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    retries: int = 0


class RateLimiter:
    """Manages rate limiting for API calls and notifications."""
    
    def __init__(self, min_interval: float = 2.0):
        """
        Initialize rate limiter.
        
        Args:
            min_interval: Minimum seconds between updates for each chat
        """
        self.last_update: Dict[Union[int, str], float] = {}
        self.min_interval = min_interval
        self.lock = asyncio.Lock()
        
    async def can_update(self, chat_id: Union[int, str]) -> bool:
        """
        Check if an update is allowed for the given chat ID.
        
        Args:
            chat_id: The chat ID or unique identifier
            
        Returns:
            True if an update is allowed, False otherwise
        """
        async with self.lock:
            now = time.time()
            if chat_id not in self.last_update:
                self.last_update[chat_id] = now
                return True
            
            # Check if enough time has passed
            if now - self.last_update[chat_id] >= self.min_interval:
                self.last_update[chat_id] = now
                return True
            return False
            
    async def wait_if_needed(self, chat_id: Union[int, str]) -> None:
        """
        Wait until an update is allowed for the given chat ID.
        
        Args:
            chat_id: The chat ID or unique identifier
        """
        async with self.lock:
            now = time.time()
            if chat_id in self.last_update:
                time_since_last = now - self.last_update[chat_id]
                if time_since_last < self.min_interval:
                    wait_time = self.min_interval - time_since_last
                    await asyncio.sleep(wait_time)
            
            self.last_update[chat_id] = time.time()


class ProgressTracker:
    """Tracks and displays download progress for a single file."""
    
    def __init__(self, 
                 total_size: int, 
                 bot: AsyncTeleBot, 
                 chat_id: int, 
                 message_id: int, 
                 update_interval: float = 5.0, 
                 file_name: str = "", 
                 batch_id: Optional[str] = None, 
                 position: int = 0, 
                 total_files: int = 1,
                 rate_limiter: Optional[RateLimiter] = None):
        """
        Initialize progress tracker.
        
        Args:
            total_size: Total file size in bytes
            bot: AsyncTeleBot instance
            chat_id: Chat ID for status messages
            message_id: Message ID to update
            update_interval: Seconds between progress updates
            file_name: Name of the file being downloaded
            batch_id: Batch ID if part of a batch
            position: Position in batch
            total_files: Total files in batch
            rate_limiter: Rate limiter for message updates
        """
        self.total_size = total_size
        self.downloaded = 0
        self.bot = bot
        self.chat_id = chat_id
        self.message_id = message_id
        self.last_update = 0
        self.update_interval = update_interval
        self.start_time = time.time()
        self.active = True
        self.file_name = file_name
        self.batch_id = batch_id
        self.position = position
        self.total_files = total_files
        self.rate_limiter = rate_limiter or RateLimiter()
        self._last_progress_message = ""
        

    async def update(self, chunk_size: int) -> None:
        """
        Update progress with new downloaded chunk.
        
        Args:
            chunk_size: Size of the new chunk in bytes
        """
        self.downloaded += chunk_size
        
        current_time = time.time()
        if (current_time - self.last_update > self.update_interval and 
            self.active and 
            await self.rate_limiter.can_update(self.chat_id)):
            self.last_update = current_time
            await self.send_progress()
            
    async def send_progress(self) -> None:
        """Send progress update message."""
        try:
            if not self.active:
                return
                
            elapsed = time.time() - self.start_time
            percent = min(self.downloaded / self.total_size * 100, 100) if self.total_size > 0 else 0
            
            # Calculate speed
            speed = self.downloaded / elapsed if elapsed > 0 else 0
            speed_text = self._format_size(speed) + "/s"
            
            # Estimated time remaining
            if speed > 0 and self.total_size > 0:
                bytes_left = self.total_size - self.downloaded
                seconds_left = bytes_left / speed
                eta_text = self._format_time(seconds_left)
                eta_section = f"\nETA: {eta_text}"
            else:
                eta_section = ""
            
            # Bar length (20 chars)
            bar_length = 20
            filled_length = int(bar_length * percent / 100)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            # Format progress message with batch info if applicable
            batch_info = ""
            if self.total_files > 1:
                batch_info = f"File {self.position+1}/{self.total_files}: "
                
            message = (
                f"Downloading {batch_info}{self.file_name}...\n"
                f"{self._format_size(self.downloaded)} / {self._format_size(self.total_size)}\n"
                f"[{bar}] {percent:.1f}%\n"
                f"Speed: {speed_text}{eta_section}"
            )
            
            # Update message if changed and rate limited
            if (message != self._last_progress_message and 
                await self.rate_limiter.can_update(self.chat_id)):
                await self.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                    text=message
                )
                self._last_progress_message = message
        except Exception as e:
            logger.error(f"Failed to update progress: {str(e)}")

    def _format_size(self, size_bytes: float) -> str:
        """
        Format bytes into human-readable size.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Human-readable size string
        """
        if size_bytes < 1024:
            return f"{size_bytes:.0f} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.1f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f} GB"
    
    def _format_time(self, seconds: float) -> str:
        """
        Format seconds into human-readable time.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Human-readable time string
        """
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds // 60
            seconds %= 60
            return f"{minutes:.0f}m {seconds:.0f}s"
        else:
            hours = seconds // 3600
            seconds %= 3600
            minutes = seconds // 60
            return f"{hours:.0f}h {minutes:.0f}m"
            
    async def complete(self, success: bool = True) -> None:
        """
        Mark download as complete and send final status.
        
        Args:
            success: Whether the download was successful
        """
        self.active = False
        try:
            elapsed = time.time() - self.start_time
            
            batch_info = ""
            if self.total_files > 1:
                batch_info = f"File {self.position+1}/{self.total_files}: "
                
            if success:
                message = (
                    f"✅ Download complete! {batch_info}{self.file_name}\n"
                    f"File size: {self._format_size(self.downloaded)}\n"
                    f"Time: {self._format_time(elapsed)}"
                )
            else:
                message = f"❌ Download failed. {batch_info}{self.file_name}"
                
            # Wait if being rate limited
            await self.rate_limiter.wait_if_needed(f"notify_{self.chat_id}")
            
            # Final completion message
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=message
            )
        except Exception as e:
            logger.error(f"Failed to update completion status: {str(e)}")


class BatchProgressTracker:
    """Track progress for a batch of files."""
    
    def __init__(self, 
                 bot: AsyncTeleBot, 
                 chat_id: int, 
                 message_id: int, 
                 total_files: int, 
                 total_size: int = 0, 
                 batch_id: Optional[str] = None,
                 rate_limiter: Optional[RateLimiter] = None):
        """
        Initialize batch progress tracker.
        
        Args:
            bot: AsyncTeleBot instance
            chat_id: Chat ID for updates
            message_id: Message ID to update
            total_files: Total number of files in batch
            total_size: Total size of all files in bytes
            batch_id: Unique identifier for the batch
            rate_limiter: Rate limiter for message updates
        """
        self.bot = bot
        self.chat_id = chat_id
        self.message_id = message_id
        self.total_files = total_files
        self.total_size = total_size
        self.batch_id = batch_id
        self.completed_files = 0
        self.failed_files = 0
        self.downloaded_bytes = 0
        self.start_time = time.time()
        self.active = True
        self.last_update = 0
        self.update_interval = 5
        self.rate_limiter = rate_limiter or RateLimiter()
        self._last_progress_message = ""
        self.files = []  # Will store file data for batch processing

    async def update(self, downloaded: int = 0, completed: int = 0, failed: int = 0) -> None:
        """
        Update batch progress.
        
        Args:
            downloaded: Additional bytes downloaded
            completed: Additional files completed
            failed: Additional files failed
        """
        self.downloaded_bytes += downloaded
        self.completed_files += completed
        self.failed_files += failed
        
        current_time = time.time()
        if (current_time - self.last_update > self.update_interval and 
            self.active and 
            await self.rate_limiter.can_update(self.chat_id)):
            self.last_update = current_time
            await self.send_progress()

    async def send_progress(self) -> None:
        """Send batch progress update."""
        try:
            if not self.active:
                return
                
            elapsed = time.time() - self.start_time
            remaining = self.total_files - (self.completed_files + self.failed_files)
            percent = (self.completed_files + self.failed_files) / self.total_files * 100
            
            # Bar length (20 chars)
            bar_length = 20
            filled_length = int(bar_length * percent / 100)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            # Calculate speed
            speed = self.downloaded_bytes / elapsed if elapsed > 0 else 0
            speed_text = self._format_size(speed) + "/s"
            
            message = (
                f"Batch Download Progress\n"
                f"Completed: {self.completed_files} | Failed: {self.failed_files} | Remaining: {remaining}\n"
                f"[{bar}] {percent:.1f}%\n"
                f"Downloaded: {self._format_size(self.downloaded_bytes)}\n"
                f"Speed: {speed_text}"
            )
            
            # Update message if changed and rate limited
            if (message != self._last_progress_message and 
                await self.rate_limiter.can_update(self.chat_id)):
                await self.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                    text=message
                )
                self._last_progress_message = message
        except Exception as e:
            logger.error(f"Failed to update batch progress: {str(e)}")

    async def complete(self) -> None:
        """Mark batch as complete and send final status."""
        self.active = False
        try:
            elapsed = time.time() - self.start_time
            
            message = (
                f"✅ Batch Download Complete\n"
                f"Total Files: {self.total_files}\n"
                f"Successful: {self.completed_files}\n"
                f"Failed: {self.failed_files}\n"
                f"Total Downloaded: {self._format_size(self.downloaded_bytes)}\n"
                f"Time: {self._format_time(elapsed)}"
            )
            
            # Final completion message ignores rate limiting
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=message
            )
        except Exception as e:
            logger.error(f"Failed to update batch completion status: {str(e)}")

    def _format_size(self, size_bytes: float) -> str:
        """
        Format bytes into human-readable size.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Human-readable size string
        """
        if size_bytes < 1024:
            return f"{size_bytes:.0f} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.1f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f} GB"
    
    def _format_time(self, seconds: float) -> str:
        """
        Format seconds into human-readable time.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Human-readable time string
        """
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds // 60
            seconds %= 60
            return f"{minutes:.0f}m {seconds:.0f}s"
        else:
            hours = seconds // 3600
            seconds %= 3600
            minutes = seconds // 60
            return f"{hours:.0f}h {minutes:.0f}m"


class ConfigManager:
    """Manages configuration loading, validation and access."""
    
    def __init__(self, config_path: str = CONFIG_FILE):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_or_create_config()
        self._validate_config()
        
    def _load_or_create_config(self) -> Dict[str, Any]:
        """
        Load configuration from file or create default if it doesn't exist.
        
        Returns:
            Configuration dictionary
        """
        if not os.path.exists(self.config_path):
            os.makedirs(os.path.dirname(os.path.abspath(self.config_path)), exist_ok=True)
            with open(self.config_path, 'w') as f:
                json.dump(DEFAULT_CONFIG, f, indent=4)
            print(f"Created default configuration file at {self.config_path}")
            print("Please edit this file with your Telegram API credentials")
            return DEFAULT_CONFIG
        
        with open(self.config_path, 'r') as f:
            return json.load(f)
            
    def _validate_config(self) -> None:
        """Validate configuration and set defaults for missing values."""
        # Validate required paths
        if "paths" not in self.config:
            self.config["paths"] = DEFAULT_CONFIG["paths"]
        else:
            required_paths = [
                "telegram_download_dir",
                "movies_dir",
                "tv_shows_dir",
                "unmatched_dir"
            ]
            
            for path_key in required_paths:
                if path_key not in self.config["paths"]:
                    self.config["paths"][path_key] = DEFAULT_CONFIG["paths"][path_key]
                    
        # Validate required telegram settings
        if "telegram" not in self.config:
            self.config["telegram"] = DEFAULT_CONFIG["telegram"]
        else:
            for key in ["api_id", "api_hash", "bot_token", "enabled"]:
                if key not in self.config["telegram"]:
                    self.config["telegram"][key] = DEFAULT_CONFIG["telegram"][key]
        
        # Validate logging settings
        if "logging" not in self.config:
            self.config["logging"] = DEFAULT_CONFIG["logging"]
        else:
            for key in ["level", "max_size_mb", "backup_count"]:
                if key not in self.config["logging"]:
                    self.config["logging"][key] = DEFAULT_CONFIG["logging"][key]
        
        # Validate download settings
        if "download" not in self.config:
            self.config["download"] = DEFAULT_CONFIG["download"]
        else:
            for key in ["chunk_size", "progress_update_interval", "max_retries", 
                      "retry_delay", "max_concurrent_downloads", "verify_downloads"]:
                if key not in self.config["download"]:
                    self.config["download"][key] = DEFAULT_CONFIG["download"][key]
                    
        # Save config with any added defaults
        self.save()
                
    def save(self) -> None:
        """Save current configuration to file."""
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=4)
            
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """
        Safely get configuration value with default fallback.
        
        Args:
            section: Configuration section
            key: Configuration key
            default: Default value if not found
            
        Returns:
            Configuration value or default
        """
        if section in self.config and key in self.config[section]:
            return self.config[section][key]
        return default
    
    def __getitem__(self, key: str) -> Any:
        """
        Get configuration section by key.
        
        Args:
            key: Section name
            
        Returns:
            Configuration section dictionary
        """
        return self.config.get(key, {})


class TelegramClientManager:
    """Manages Telethon client for large file downloads."""
    
    def __init__(self, api_id: str, api_hash: str, bot_token: str):
        """
        Initialize Telethon client manager.
        
        Args:
            api_id: Telegram API ID
            api_hash: Telegram API hash
            bot_token: Telegram bot token
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.client = None
        self.connected = False
        self.connecting = False
        self._lock = asyncio.Lock()
        
    async def initialize(self) -> bool:
        """
        Initialize Telethon client.
        
        Returns:
            True if successful, False otherwise
        """
        async with self._lock:
            if self.connected:
                return True
                
            if self.connecting:
                while self.connecting:
                    await asyncio.sleep(0.1)
                return self.connected
                
            self.connecting = True
            
            try:
                # Create a session file path
                session_file = "telegram_bot_session"
                
                # Start the client
                self.client = TelegramClient(
                    session_file, 
                    self.api_id, 
                    self.api_hash
                )
                
                # Connect the client
                await self.client.start(bot_token=self.bot_token)
                
                # Check if authorized
                if not await self.client.is_user_authorized():
                    logger.warning("Telethon client not authorized")
                    self.connected = False
                else:
                    logger.info("Telethon client authorized successfully")
                    self.connected = True
                    
            except Exception as e:
                logger.error(f"Failed to initialize Telethon client: {str(e)}")
                self.connected = False
                
            finally:
                self.connecting = False
                
            return self.connected
            
    async def check_connection(self, max_retries: int = 3) -> bool:
        """
        Check Telethon connection and reconnect if needed.
        
        Args:
            max_retries: Maximum reconnection attempts
            
        Returns:
            True if connected, False otherwise
        """
        if not self.client:
            return await self.initialize()
            
        retry_count = 0
        while retry_count < max_retries:
            try:
                if not self.client.is_connected():
                    logger.info("Telethon client disconnected, attempting to reconnect...")
                    await self.client.connect()
                
                if not await self.client.is_user_authorized():
                    logger.error("Telethon client is not authorized")
                    return False
                    
                # Test connection with a simple request
                me = await self.client.get_me()
                if me:
                    self.connected = True
                    return True
                    
            except Exception as e:
                retry_count += 1
                logger.warning(f"Telethon connection check failed (attempt {retry_count}/{max_retries}): {str(e)}")
                if retry_count < max_retries:
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                    
        self.connected = False
        return False
        
    async def download_media(self, message, file_path: str, progress_callback=None) -> Optional[str]:
        """
        Download media using Telethon.
        
        Args:
            message: Telethon message object
            file_path: Path to save the file
            progress_callback: Progress callback function
            
        Returns:
            Path to downloaded file if successful, None otherwise
        """
        if not await self.check_connection():
            logger.error("Cannot download: Telethon client not connected")
            return None
            
        try:
            # Download the file
            downloaded_file = await self.client.download_media(
                message,
                file_path,
                progress_callback=progress_callback
            )
            
            return downloaded_file
        except FloodWaitError as e:
            logger.warning(f"Flood wait error: must wait {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return None
        except Exception as e:
            logger.error(f"Telethon download error: {str(e)}")
            return None
            
    async def close(self) -> None:
        """Close Telethon client connection."""
        if self.client:
            try:
                await self.client.disconnect()
                self.connected = False
                logger.info("Telethon client disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Telethon client: {str(e)}")


class DownloadManager:
    """Manages download queue and processes downloads."""
    
    def __init__(self, config_manager: ConfigManager, client_manager: TelegramClientManager, bot: AsyncTeleBot):
        """
        Initialize download manager.
        
        Args:
            config_manager: Configuration manager
            client_manager: Telethon client manager
            bot: AsyncTeleBot instance
        """
        self.config = config_manager
        self.client_manager = client_manager
        self.bot = bot
        self.download_dir = self.config["paths"]["telegram_download_dir"]
        
        # Download settings
        self.chunk_size = self.config["download"]["chunk_size"]
        self.progress_update_interval = self.config["download"]["progress_update_interval"]
        self.max_retries = self.config["download"]["max_retries"]
        self.retry_delay = self.config["download"]["retry_delay"]
        self.max_concurrent_downloads = self.config["download"]["max_concurrent_downloads"]
        self.verify_downloads = self.config["download"].get("verify_downloads", True)
        
        # Initialize queue and semaphore
        self.download_queue: Deque[DownloadTask] = deque()
        self.queue_lock = asyncio.Lock()
        self.download_semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        
        # Rate limiter for progress updates
        self.rate_limiter = RateLimiter()
        
        # Stats tracking
        self.download_stats = {
            "total_downloads": 0,
            "successful_downloads": 0,
            "failed_downloads": 0,
            "total_bytes": 0,
            "start_time": time.time()
        }
        
        # Active downloads tracking
        self.active_downloads: Dict[str, Dict[str, Any]] = {}
        
        # Batch trackers
        self.batch_trackers: Dict[str, BatchProgressTracker] = {}
        
        # Set of pending media groups (to avoid duplicates)
        self.pending_media_groups: Set[str] = set()
        
    async def start(self) -> None:
        """Start the download queue processor."""
        logger.info("Starting download queue processor")
        asyncio.create_task(self._process_download_queue())
        
    async def add_task(self, task: DownloadTask) -> None:
        """
        Add a download task to the queue.
        
        Args:
            task: Download task to add
        """
        async with self.queue_lock:
            self.download_queue.append(task)
            
        # Update the progress message to show queue position
        queue_position = len(self.download_queue)
        try:
            if queue_position > 1:
                await self.bot.edit_message_text(
                    chat_id=task.chat_id,
                    message_id=task.message_id,
                    text=f"In download queue... (position: {queue_position})"
                )
        except Exception as e:
            logger.error(f"Failed to update queue position message: {str(e)}")
            
        logger.info(f"Added {task.file_name} to download queue (position {queue_position})")
    
    async def _process_download_queue(self) -> None:
        """Process the download queue in the background."""
        logger.info("Download queue processor started")
        
        while True:
            task = None
            
            # Get next task from queue
            async with self.queue_lock:
                if self.download_queue:
                    task = self.download_queue.popleft()
            
            if task:
                # Wait for a download slot to become available
                async with self.download_semaphore:
                    # Update task status
                    task.status = "downloading"
                    task.start_time = time.time()
                    
                    # Update message to show download is starting
                    try:
                        await self.bot.edit_message_text(
                            chat_id=task.chat_id,
                            message_id=task.message_id,
                            text=f"Starting download: {task.file_name}..."
                        )
                    except Exception as e:
                        logger.error(f"Failed to update task status message: {str(e)}")
                    
                    # Process the download
                    await self._download_file(task)
            
            # Sleep to avoid CPU spinning
            await asyncio.sleep(0.5)
    
    async def _download_file(self, task: DownloadTask) -> None:
        """
        Download a file with progress tracking.
        
        Args:
            task: Download task
        """
        try:
            # Create progress tracker
            tracker = ProgressTracker(
                task.file_size, 
                self.bot, 
                task.chat_id, 
                task.message_id,
                update_interval=self.progress_update_interval,
                file_name=task.file_name,
                batch_id=task.batch_id,
                position=task.position,
                total_files=task.total_files,
                rate_limiter=self.rate_limiter
            )
            
            # Track download
            download_key = f"{task.chat_id}_{task.file_id}"
            self.active_downloads[download_key] = {
                "path": task.file_path,
                "tracker": tracker,
                "start_time": time.time(),
                "batch_id": task.batch_id,
                "task": task
            }
            
            # Update batch tracker if this is part of a batch
            if task.batch_id and task.batch_id in self.batch_trackers:
                await self.batch_trackers[task.batch_id].send_progress()
            
            # First try Telethon for larger files if available
            telethon_available = await self.client_manager.check_connection()
            use_telethon = (telethon_available and 
                           task.file_size > 20 * 1024 * 1024 and 
                           task.original_message)
            
            success = False
            
            if use_telethon:
                logger.info(f"Using Telethon for large file ({task.file_size / (1024*1024):.2f} MB): {task.file_path}")
                success = await self._download_with_telethon(task, tracker)
            else:
                logger.info(f"Using Bot API for download ({task.file_size / (1024*1024):.2f} MB): {task.file_path}")
                success = await self._download_with_botapi(task, tracker)
                
            # Update task end time
            task.end_time = time.time()
                
            # Verify download if enabled and successful
            if success and self.verify_downloads:
                success = await self._verify_download(task.file_path, task.file_size)
                if not success:
                    logger.error(f"File verification failed: {task.file_path}")
            
            # Update stats
            self.download_stats["total_downloads"] += 1
            if success:
                self.download_stats["successful_downloads"] += 1
                try:
                    file_size = os.path.getsize(task.file_path)
                    self.download_stats["total_bytes"] += file_size
                    tracker.downloaded = file_size  # Ensure tracker has correct final size
                except Exception as e:
                    logger.error(f"Failed to get file size: {str(e)}")
                    
                await tracker.complete(success=True)
                logger.info(f"Download complete: {task.file_path}")
                
                # Only notify on success if this is not part of a batch
                if not task.batch_id:
                    await self.bot.send_message(
                        chat_id=task.chat_id,
                        text=f"✅ Download complete! Media sorted by the file watcher will appear in your Jellyfin library."
                    )
            else:
                self.download_stats["failed_downloads"] += 1
                await tracker.complete(success=False)
                logger.error(f"Download failed after {self.max_retries} retries: {task.file_path}")
                
                # Try to delete partially downloaded file
                try:
                    if os.path.exists(task.file_path):
                        os.remove(task.file_path)
                except Exception as e:
                    logger.error(f"Failed to remove partial file: {str(e)}")
            
            # Update batch tracker if this is part of a batch
            if task.batch_id and task.batch_id in self.batch_trackers:
                batch_tracker = self.batch_trackers[task.batch_id]
                if success:
                    downloaded_size = os.path.getsize(task.file_path) if os.path.exists(task.file_path) else 0
                    await batch_tracker.update(downloaded=downloaded_size, completed=1)
                else:
                    await batch_tracker.update(failed=1)
                    
                # Check if this was the last file in the batch
                completed_and_failed = batch_tracker.completed_files + batch_tracker.failed_files
                if completed_and_failed >= batch_tracker.total_files:
                    # Final batch notification
                    await batch_tracker.complete()
                    
                    # Send a final notification about the batch
                    if batch_tracker.completed_files > 0:
                        await self.bot.send_message(
                            chat_id=task.chat_id,
                            text=(
                                f"✅ Batch download complete! {batch_tracker.completed_files}/{batch_tracker.total_files} files downloaded.\n"
                                f"Media sorted by the file watcher will appear in your Jellyfin library."
                            )
                        )
                    
                    # Clean up batch tracker
                    del self.batch_trackers[task.batch_id]
            
            # Remove from active downloads
            if download_key in self.active_downloads:
                del self.active_downloads[download_key]
                
        except Exception as e:
            logger.error(f"Error downloading file: {str(e)}")
            
            try:
                await self.bot.edit_message_text(
                    chat_id=task.chat_id,
                    message_id=task.message_id,
                    text=f"❌ Download failed: {str(e)}"
                )
            except:
                pass
            
            # Update stats
            self.download_stats["total_downloads"] += 1
            self.download_stats["failed_downloads"] += 1
            
            # Update batch tracker if this is part of a batch
            if task.batch_id and task.batch_id in self.batch_trackers:
                await self.batch_trackers[task.batch_id].update(failed=1)
            
            # Remove from active downloads
            download_key = f"{task.chat_id}_{task.file_id}"
            if download_key in self.active_downloads:
                del self.active_downloads[download_key]
    
    async def _download_with_telethon(self, task: DownloadTask, tracker: ProgressTracker) -> bool:
        """
        Download a file using Telethon.
        
        Args:
            task: Download task
            tracker: Progress tracker
            
        Returns:
            True if successful, False otherwise
        """
        for attempt in range(self.max_retries + 1):
            try:
                # Create a callback for progress updates
                async def progress_callback(current: int, total: int) -> None:
                    if tracker.downloaded != current:
                        chunk_size = current - tracker.downloaded
                        if chunk_size > 0:
                            await tracker.update(chunk_size)
                        else:
                            tracker.downloaded = current
                            tracker.total_size = total
                            await tracker.send_progress()
                
                # Get the original message through Telethon if needed
                if hasattr(task.original_message, 'chat') and hasattr(task.original_message, 'message_id'):
                    telethon_message = await self.client_manager.client.get_messages(
                        task.original_message.chat.id, 
                        ids=task.original_message.message_id
                    )
                    if telethon_message:
                        task.original_message = telethon_message
                
                # Download using Telethon
                downloaded_file = await self.client_manager.download_media(
                    task.original_message,
                    task.file_path,
                    progress_callback=progress_callback
                )
                
                if downloaded_file and os.path.exists(downloaded_file):
                    return True
                    
            except Exception as e:
                # Update retry information
                task.retries += 1
                
                logger.warning(f"Telethon download attempt {attempt+1}/{self.max_retries+1} failed: {str(e)}")
                
                if attempt < self.max_retries:
                    # Exponential backoff with jitter
                    wait_time = self.retry_delay * (1.5 ** attempt) * (0.8 + 0.4 * random.random())
                    
                    try:
                        await self.bot.edit_message_text(
                            chat_id=task.chat_id,
                            message_id=task.message_id,
                            text=f"⚠️ Download interrupted. Retrying in {wait_time:.1f}s... ({attempt+1}/{self.max_retries})"
                        )
                    except Exception as e:
                        logger.error(f"Failed to update retry message: {str(e)}")
                        
                    await asyncio.sleep(wait_time)
                    
                    # Verify connection before retry
                    await self.client_manager.check_connection()
                else:
                    return False
                    
        return False
    
    async def _download_with_botapi(self, task: DownloadTask, tracker: ProgressTracker) -> bool:
        """
        Download a file using Bot API.
        
        Args:
            task: Download task
            tracker: Progress tracker
            
        Returns:
            True if successful, False otherwise
        """
        for attempt in range(self.max_retries + 1):
            try:
                # Get file info
                file_info = await self.bot.get_file(task.file_id)
                file_url = f"https://api.telegram.org/file/bot{self.bot.token}/{file_info.file_path}"
                
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(task.file_path), exist_ok=True)
                
                # Set up async HTTP session for streaming download
                async with aiohttp.ClientSession() as session:
                    async with session.get(file_url) as response:
                        if response.status != 200:
                            raise Exception(f"HTTP error {response.status}: {response.reason}")
                            
                        # Get actual file size if not known
                        if task.file_size == 0 and 'content-length' in response.headers:
                            task.file_size = int(response.headers['content-length'])
                            tracker.total_size = task.file_size
                        
                        # Save file with async I/O
                        with open(task.file_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(self.chunk_size)
                                if not chunk:
                                    break
                                f.write(chunk)
                                await tracker.update(len(chunk))
                
                # Check if file exists and has content
                if os.path.exists(task.file_path) and os.path.getsize(task.file_path) > 0:
                    return True
                else:
                    raise Exception("Downloaded file is empty or doesn't exist")
                    
            except aiohttp.ClientError as e:
                task.retries += 1
                logger.warning(f"HTTP download attempt {attempt+1}/{self.max_retries+1} failed: {str(e)}")
            except Exception as e:
                task.retries += 1
                logger.warning(f"Download attempt {attempt+1}/{self.max_retries+1} failed: {str(e)}")
                
            # Handle retry logic
            if attempt < self.max_retries:
                # Exponential backoff with jitter
                wait_time = self.retry_delay * (1.5 ** attempt) * (0.8 + 0.4 * random.random())
                
                try:
                    await self.bot.edit_message_text(
                        chat_id=task.chat_id,
                        message_id=task.message_id,
                        text=f"⚠️ Download interrupted. Retrying in {wait_time:.1f}s... ({attempt+1}/{self.max_retries})"
                    )
                except Exception as e:
                    logger.error(f"Failed to update retry message: {str(e)}")
                    
                await asyncio.sleep(wait_time)
            else:
                return False
        
        return False
    
    async def _verify_download(self, file_path: str, expected_size: Optional[int] = None) -> bool:
        """
        Verify downloaded file integrity.
        
        Args:
            file_path: Path to the downloaded file
            expected_size: Expected file size in bytes
            
        Returns:
            True if file is valid, False otherwise
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"Verification failed: File {file_path} does not exist")
                return False
                
            # Check file size if expected size is provided
            if expected_size:
                actual_size = os.path.getsize(file_path)
                if actual_size < expected_size * 0.9:  # Allow for some difference (e.g., for text files)
                    logger.error(f"Verification failed: Size mismatch for {file_path}. "
                                f"Expected {expected_size}, got {actual_size}")
                    return False
            
            # Check if file is readable
            with open(file_path, 'rb') as f:
                # Read first chunk to verify file is readable
                f.seek(0)
                first_chunk = f.read(1024)
                
                # For files larger than 2KB, check end too
                size = os.path.getsize(file_path)
                if size > 2048:
                    f.seek(max(0, size - 1024), os.SEEK_SET)
                    last_chunk = f.read(1024)
                    
                    # Simple heuristic for binary files - check if either chunk has nulls
                    # This helps detect incomplete downloads of binary files
                    if b'\x00' in first_chunk or b'\x00' in last_chunk:
                        # For binary files, we expect non-zero content in both chunks
                        if len(first_chunk.strip(b'\x00')) == 0 or len(last_chunk.strip(b'\x00')) == 0:
                            logger.error(f"Verification failed: File {file_path} appears incomplete")
                            return False
            
            logger.debug(f"File verification passed: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Verification failed: Cannot access {file_path}: {e}")
            return False

    def get_queue_status(self) -> str:
        """
        Get formatted status of the download queue.
        
        Returns:
            Queue status string
        """
        queue_length = len(self.download_queue)
        
        active_count = self.max_concurrent_downloads - self.download_semaphore._value
        
        if queue_length == 0 and active_count == 0:
            return "No active downloads or queued files."
            
        status = f"📥 Download Queue Status\n\n"
        status += f"⏳ Active downloads: {active_count}/{self.max_concurrent_downloads}\n"
        status += f"🔄 Queued files: {queue_length}\n\n"
        
        if active_count > 0:
            status += "Currently downloading:\n"
            for key, download in self.active_downloads.items():
                # Extract filename from path
                filename = os.path.basename(download["path"])
                status += f"- {filename}\n"
        
        if queue_length > 0 and queue_length <= 5:
            # Show up to 5 queued files
            status += "\nNext in queue:\n"
            for i, task in enumerate(list(self.download_queue)[:5]):
                status += f"{i+1}. {task.file_name}\n"
        elif queue_length > 5:
            status += f"\nShowing first 5 of {queue_length} queued files:\n"
            for i, task in enumerate(list(self.download_queue)[:5]):
                status += f"{i+1}. {task.file_name}\n"
            
        return status
    
    def get_stats(self) -> str:
        """
        Get formatted download statistics.
        
        Returns:
            Statistics string
        """
        total = self.download_stats["total_downloads"]
        successful = self.download_stats["successful_downloads"]
        failed = self.download_stats["failed_downloads"]
        total_bytes = self.download_stats["total_bytes"]
        uptime = time.time() - self.download_stats["start_time"]
        
        # Format total bytes
        if total_bytes < 1024:
            size_str = f"{total_bytes} B"
        elif total_bytes < 1024 * 1024:
            size_str = f"{total_bytes/1024:.1f} KB"
        elif total_bytes < 1024 * 1024 * 1024:
            size_str = f"{total_bytes/(1024*1024):.1f} MB"
        else:
            size_str = f"{total_bytes/(1024*1024*1024):.2f} GB"
        
        # Format uptime
        days = int(uptime // (24 * 3600))
        uptime %= (24 * 3600)
        hours = int(uptime // 3600)
        uptime %= 3600
        minutes = int(uptime // 60)
        
        if days > 0:
            uptime_str = f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            uptime_str = f"{hours}h {minutes}m"
        else:
            uptime_str = f"{minutes}m"
        
        # Current downloads status
        active = len(self.active_downloads)
        queued = len(self.download_queue)
        
        return (
            f"📊 Download Statistics\n\n"
            f"🕒 Uptime: {uptime_str}\n"
            f"📥 Total downloads: {total}\n"
            f"✅ Successful: {successful}\n"
            f"❌ Failed: {failed}\n"
            f"💾 Total downloaded: {size_str}\n"
            f"⏳ Active downloads: {active}/{self.max_concurrent_downloads}\n"
            f"🔄 Queued files: {queued}"
        )

    async def process_media_group(self, media_group_id: str) -> None:
        """
        Process a complete media group after the collection period.

        Args:
            media_group_id: Media group ID
        """
        try:
            # Get the batch tracker
            if media_group_id not in self.batch_trackers:
                logger.error(f"Media group {media_group_id} not found in batch trackers")
                return

            batch_tracker = self.batch_trackers[media_group_id]

            # Check for duplicate file_ids in the batch tracker
            seen_file_ids = set()
            unique_files = []

            # Filter out any duplicates by file_id
            for file_data in batch_tracker.files:
                if file_data['file_id'] not in seen_file_ids:
                    seen_file_ids.add(file_data['file_id'])
                    unique_files.append(file_data)
                else:
                    logger.warning(f"Removing duplicate file: {file_data['file_name']} with ID: {file_data['file_id']}")

            # Update the files list with only unique files
            batch_tracker.files = unique_files
            batch_tracker.total_files = len(unique_files)

            # Update the batch message with the final file count
            try:
                await self.bot.edit_message_text(
                    chat_id=batch_tracker.chat_id,
                    message_id=batch_tracker.message_id,
                    text=f"Starting batch download of {len(batch_tracker.files)} files..."
                )
            except Exception as e:
                logger.error(f"Failed to update batch message: {str(e)}")

            # DEBUG: Log the file_ids and file_names to verify they are unique
            logger.debug(f"Media group {media_group_id} contains {len(batch_tracker.files)} unique files:")
            for i, file_data in enumerate(batch_tracker.files):
                logger.debug(f"File {i+1}: {file_data['file_name']} - ID: {file_data['file_id']}")

            # Create individual progress messages and download tasks for each file
            processed_file_ids = set()  # Track which file_ids we've already processed

            for i, file_data in enumerate(batch_tracker.files):
                # Double check that we're using the correct file data and not processing duplicates
                file_id = file_data['file_id']

                # Skip if we've already processed this file_id (extra safeguard)
                if file_id in processed_file_ids:
                    logger.warning(f"Skipping already processed file_id: {file_id}")
                    continue

                processed_file_ids.add(file_id)

                file_path = file_data['file_path']
                file_name = file_data['file_name']

                progress_message = await self.bot.send_message(
                    batch_tracker.chat_id,
                    f"Queued: {file_name} ({i+1}/{len(batch_tracker.files)})"
                )

                # Create download task with the specific file data
                task = DownloadTask(
                    file_id=file_id,
                    file_path=file_path,
                    file_size=file_data['file_size'],
                    chat_id=batch_tracker.chat_id,
                    message_id=progress_message.message_id,
                    original_message=file_data['original_message'],
                    file_name=file_name,
                    batch_id=media_group_id,
                    position=i,
                    total_files=len(batch_tracker.files)
                )

                # Add the task to the download queue
                await self.add_task(task)

            # Update the batch tracker (e.g., progress status)
            await batch_tracker.send_progress()

            # Remove the media group from pending groups
            if media_group_id in self.pending_media_groups:
                self.pending_media_groups.remove(media_group_id)

        except Exception as e:
            logger.error(f"Error processing media group {media_group_id}: {str(e)}")
            # Ensure the media group is removed from pending on error
            if media_group_id in self.pending_media_groups:
                self.pending_media_groups.remove(media_group_id)

class TelegramBot:
    """Handles Telegram bot interactions and commands."""
    
    def __init__(self, config_manager: ConfigManager, download_manager: DownloadManager):
        """
        Initialize Telegram bot.
        
        Args:
            config_manager: Configuration manager
            download_manager: Download manager
        """
        self.config = config_manager
        self.download_manager = download_manager
        self.bot = self.download_manager.bot
        self.running = False
        
        # Set up bot handlers
        self._setup_handlers()
        
    def _setup_handlers(self) -> None:
        """Set up Telegram bot message handlers."""
        
        @self.bot.message_handler(commands=['start', 'help'])
        async def send_welcome(message):
            welcome_text = (
                "👋 Welcome to the Jellyfin Media Downloader Bot!\n\n"
                "Send me any media file and I will download it to your Jellyfin library.\n\n"
                "Available commands:\n"
                "/start - Show this welcome message\n"
                "/stats - Show download statistics\n"
                "/queue - Show current download queue\n"
                "/test - Run a system test\n"
                "/help - Show this message"
            )
            await self.bot.reply_to(message, welcome_text)
        
        @self.bot.message_handler(commands=['stats'])
        async def show_stats(message):
            stats = self.download_manager.get_stats()
            await self.bot.reply_to(message, stats)
        
        @self.bot.message_handler(commands=['queue'])
        async def show_queue(message):
            queue_status = self.download_manager.get_queue_status()
            await self.bot.reply_to(message, queue_status)
        
        @self.bot.message_handler(commands=['test'])
        async def run_test(message):
            await self.bot.reply_to(message, "🔍 Running system test...")
            
            test_results = []
            
            # Test directory access
            try:
                test_file = os.path.join(self.download_manager.download_dir, ".test_file")
                with open(test_file, "w") as f:
                    f.write("test")
                os.remove(test_file)
                test_results.append("✅ Download directory is writable")
            except Exception as e:
                test_results.append(f"❌ Download directory test failed: {str(e)}")
            
            # Test bot API
            try:
                test_msg = await self.bot.send_message(message.chat.id, "Testing bot API...")
                await self.bot.delete_message(message.chat.id, test_msg.message_id)
                test_results.append("✅ Telegram Bot API is working")
            except Exception as e:
                test_results.append(f"❌ Telegram Bot API test failed: {str(e)}")
            
            # Test Telethon connection
            if await self.download_manager.client_manager.check_connection():
                test_results.append("✅ Telethon client is working")
            else:
                test_results.append("❌ Telethon client is not initialized or not connected")
            
            # Test internet connection
            try:
                session = aiohttp.ClientSession()
                async with session.get("https://www.google.com", timeout=5) as response:
                    if response.status == 200:
                        test_results.append("✅ Internet connection is working")
                    else:
                        test_results.append(f"❌ Internet connection test failed: HTTP {response.status}")
                await session.close()
            except Exception as e:
                test_results.append(f"❌ Internet connection test failed: {str(e)}")
            
            # Test queue system
            test_results.append(f"✅ Download queue system is active (limit: {self.download_manager.max_concurrent_downloads})")
            
            # Send test results
            await self.bot.send_message(message.chat.id, f"System Test Results:\n\n{chr(10).join(test_results)}")
        
        @self.bot.message_handler(content_types=['document', 'video', 'audio'])
async def handle_media(message):
    """Handle media file downloads, including multiple attachments."""
    # Check for media group (multiple files)
    media_group_id = message.media_group_id if hasattr(message, 'media_group_id') else None
    
    # DEBUG: Add logging for media group messages
    if media_group_id:
        logger.debug(f"Received media in group {media_group_id}: {message.content_type} - file_id: {getattr(message.document if message.content_type == 'document' else message.video if message.content_type == 'video' else message.audio, 'file_id', 'unknown')}")
    
    # Single file handling (no media group)
    if not media_group_id:
        if message.content_type == 'document':
            file_info = message.document
        elif message.content_type == 'video':
            file_info = message.video
        elif message.content_type == 'audio':
            file_info = message.audio
        else:
            await self.bot.reply_to(message, "Unsupported file type. Please send a media file.")
            return
        
        # Get file details
        file_id = file_info.file_id
        file_size = file_info.file_size if hasattr(file_info, 'file_size') else 0
        file_name = file_info.file_name if hasattr(file_info, 'file_name') else f"file_{file_id}"
        
        # Check if file exists and generate unique name if needed
        file_path = os.path.join(self.download_manager.download_dir, file_name)
        if os.path.exists(file_path):
            # Find a new unique name
            base, ext = os.path.splitext(file_path)
            counter = 1
            while os.path.exists(f"{base}_{counter}{ext}"):
                counter += 1
            file_path = f"{base}_{counter}{ext}"
            file_name = os.path.basename(file_path)
        
        # Send acknowledgment
        await self.bot.reply_to(message, f"📥 Received request to download: {file_name}")
        progress_message = await self.bot.send_message(message.chat.id, "Added to download queue...")
        
        # Create download task
        task = DownloadTask(
            file_id=file_id,
            file_path=file_path,
            file_size=file_size,
            chat_id=message.chat.id,
            message_id=progress_message.message_id,
            original_message=message,
            file_name=file_name
        )
        
        # Add to queue
        await self.download_manager.add_task(task)
    
    else:
        # Media group handling (using streamlined duplicate-check and batch updates)
        if message.content_type == 'document':
            file_info = message.document
        elif message.content_type == 'video':
            file_info = message.video
        elif message.content_type == 'audio':
            file_info = message.audio
        else:
            # Skip unsupported types
            return
        
        # Get file details
        file_id = file_info.file_id
        file_size = file_info.file_size if hasattr(file_info, 'file_size') else 0
        file_name = file_info.file_name if hasattr(file_info, 'file_name') else f"file_{file_id}"
        
        # Check if file exists and generate unique name if needed
        file_path = os.path.join(self.download_manager.download_dir, file_name)
        if os.path.exists(file_path):
            base, ext = os.path.splitext(file_path)
            counter = 1
            while os.path.exists(f"{base}_{counter}{ext}"):
                counter += 1
            file_path = f"{base}_{counter}{ext}"
            file_name = os.path.basename(file_path)
        
        # First, check if we've already seen this media group
        if media_group_id in self.download_manager.batch_trackers:
            # Add to existing batch if this file_id isn't already in the batch
            batch_tracker = self.download_manager.batch_trackers[media_group_id]
            
            # Check for duplicate file by file_id
            existing_file_ids = [f['file_id'] for f in batch_tracker.files]
            if file_id in existing_file_ids:
                logger.debug(f"Skipping duplicate file {file_name} in media group {media_group_id}")
                return
                
            # Update the batch message with the new file count
            try:
                await self.bot.edit_message_text(
                    chat_id=batch_tracker.chat_id,
                    message_id=batch_tracker.message_id,
                    text=f"Collecting files from group message... ({len(batch_tracker.files) + 1} files found)"
                )
            except Exception as e:
                logger.error(f"Failed to update batch message: {str(e)}")
                
        else:
            # First file in this media group: mark it as pending and create a batch tracker
            self.download_manager.pending_media_groups.add(media_group_id)
            
            batch_message = await self.bot.send_message(
                message.chat.id, 
                "Collecting files from group message... (1 file found)"
            )
            
            batch_tracker = BatchProgressTracker(
                bot=self.bot,
                chat_id=message.chat.id,
                message_id=batch_message.message_id,
                total_files=0,  # Start at 0; will update below
                batch_id=media_group_id,
                rate_limiter=self.download_manager.rate_limiter
            )
            batch_tracker.files = []  # Initialize empty list to hold file info
            
            self.download_manager.batch_trackers[media_group_id] = batch_tracker
            
            # Schedule processing of this media group after a short delay
            asyncio.get_event_loop().call_later(
                3.0, 
                lambda: asyncio.create_task(
                    self.download_manager.process_media_group(media_group_id)
                )
            )
        
        # Append this file to the batch data
        batch_tracker.files.append({
            'file_id': file_id,
            'file_path': file_path,
            'file_size': file_size,
            'file_name': file_name,
            'original_message': message
        })
        
        # Update total files count and size
        batch_tracker.total_files = len(batch_tracker.files)
        batch_tracker.total_size += file_size

    async def start(self) -> None:
        """Start the Telegram bot."""
        logger.info("Starting Telegram bot")
        self.running = True
        await self.download_manager.start()
        
        # Start polling in a separate task
        asyncio.create_task(self._polling_task())
        
        # Start connection check task
        asyncio.create_task(self._connection_check())
        
    async def _polling_task(self) -> None:
        """Task for bot polling with error handling."""
        while self.running:
            try:
                logger.info("Starting bot polling...")
                # Start polling
                await self.bot.polling(non_stop=True, timeout=30)
            except Exception as e:
                logger.error(f"Polling error: {str(e)}")
                if self.running:
                    logger.info("Restarting polling after error...")
                    await asyncio.sleep(5)
    
    async def _connection_check(self) -> None:
        """Periodic connection check task."""
        while self.running:
            try:
                # Test connection by getting bot info
                me = await self.bot.get_me()
                logger.debug("Bot connection check: OK")
            except Exception as e:
                logger.warning(f"Bot connection check failed: {str(e)}")
            
            # Sleep for 60 seconds before next check
            for _ in range(60):
                if not self.running:
                    break
                await asyncio.sleep(1)
    
    async def stop(self) -> None:
        """Stop the Telegram bot."""
        logger.info("Stopping Telegram bot")
        self.running = False
        
        # Close Telethon client
        await self.download_manager.client_manager.close()
        
        # Stop polling
        await self.bot.stop_polling()


class TelegramDownloader:
    """Main application class for Telegram Downloader."""
    
    def __init__(self, config_path: str = CONFIG_FILE):
        """
        Initialize the Telegram Downloader.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_manager = ConfigManager(config_path)
        self._setup_logging()
        self._setup_directories()
        
        # Initialize rate limiter with global scope
        self.rate_limiter = RateLimiter()
        
        # Initialize Telegram components
        self.bot = AsyncTeleBot(self.config_manager["telegram"]["bot_token"])
        
        # Initialize Telethon client manager
        self.client_manager = TelegramClientManager(
            self.config_manager["telegram"]["api_id"], 
            self.config_manager["telegram"]["api_hash"],
            self.config_manager["telegram"]["bot_token"]
        )
        
        # Initialize download manager
        self.download_manager = DownloadManager(
            self.config_manager, 
            self.client_manager,
            self.bot
        )
        
        # Initialize Telegram bot
        self.telegram_bot = TelegramBot(
            self.config_manager,
            self.download_manager
        )
        
        # Flag for controlled shutdown
        self.running = False

    def _setup_logging(self) -> None:
        """Set up logging with rotating file handler."""
        global logger
        
        log_level = getattr(logging, self.config_manager["logging"].get("level", "INFO"))
        max_size_mb = self.config_manager["logging"].get("max_size_mb", 10)
        backup_count = self.config_manager["logging"].get("backup_count", 5)
        
        # Create log directory if it doesn't exist
        log_dir = os.path.dirname(os.path.abspath("telegram_downloader.log"))
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                RotatingFileHandler(
                    "telegram_downloader.log", 
                    maxBytes=max_size_mb * 1024 * 1024, 
                    backupCount=backup_count
                ),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger("TelegramDownloader")

    def _setup_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        dirs = [
            self.config_manager["paths"]["telegram_download_dir"],
            self.config_manager["paths"]["movies_dir"],
            self.config_manager["paths"]["tv_shows_dir"],
            self.config_manager["paths"]["unmatched_dir"]
        ]
        
        for directory in dirs:
            try:
                os.makedirs(directory, exist_ok=True)
                logger.debug(f"Directory created or already exists: {directory}")
            except Exception as e:
                logger.error(f"Failed to create directory {directory}: {str(e)}")

    async def _startup(self) -> None:
        """Initialize and start all components."""
        try:
            # Initialize Telethon client
            await self.client_manager.initialize()
            
            # Start Telegram bot
            await self.telegram_bot.start()
            
            logger.info("Telegram Downloader started successfully")
        except Exception as e:
            logger.error(f"Startup error: {str(e)}")
            self.running = False

    async def _shutdown(self) -> None:
        """Shutdown all components."""
        try:
            # Stop Telegram bot
            await self.telegram_bot.stop()
            
            logger.info("Telegram Downloader shutdown complete")
        except Exception as e:
            logger.error(f"Shutdown error: {str(e)}")

    async def run_async(self) -> None:
        """Run the Telegram downloader with async/await pattern."""
        self.running = True
        
        try:
            # Startup
            await self._startup()
            
            # Keep running until stopped
            while self.running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
            self.running = False
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            self.running = False
        finally:
            # Cleanup
            await self._shutdown()

    def run(self) -> None:
        """Run the Telegram downloader with proper exit handling."""
        try:
            # Set up event loop
            if os.name == 'nt':  # Windows
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
            # Get or create event loop
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Run the async main function
            loop.run_until_complete(self.run_async())
            
        except KeyboardInterrupt:
            print("\nReceived shutdown signal (Ctrl+C)")
        except Exception as e:
            print(f"Error: {str(e)}")
        finally:
            print("Bot stopped")


async def run_async():
    """Run the application asynchronously."""
    downloader = TelegramDownloader()
    await downloader.run_async()

def main():
    """Main entry point for the application."""
    try:
        import aiohttp
        
        # Initialize and run the downloader
        downloader = TelegramDownloader()
        downloader.run()
        
    except KeyboardInterrupt:
        print("\nReceived shutdown signal (Ctrl+C)")
        print("Shutting down gracefully...")
    except ImportError as e:
        print(f"Missing required dependency: {e}")
        print("Please install required packages with: pip install telebot telethon aiohttp")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        print("Bot stopped")

if __name__ == "__main__":
    main()