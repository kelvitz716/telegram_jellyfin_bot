#!/usr/bin/env python3
"""
telegram_downloader.py - Downloads media files from Telegram

A modular, asynchronous Telegram downloader for media files with support for
batch downloads, progress tracking, and queuing.
"""
import os
from pathlib import Path
import shutil
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
from dataclasses import asdict, dataclass, field
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
        "verify_downloads": True,
        "max_speed_mbps": 0,  # 0 = unlimited
        "resume_support": True,
        "temp_download_dir": "temp_downloads"
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
    resume_position: int = 0
    temp_path: str = ""
    
    # Status tracking
    status: str = "queued"  # queued, downloading, completed, failed
    
    # Add timestamp for sorting/prioritization
    added_time: float = field(default_factory=time.time)
    
    # Progress tracking
    downloaded_bytes: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    retries: int = 0

    async def prepare_resume(self) -> None:
        """Prepare for resume by checking existing partial download."""
        if os.path.exists(self.temp_path):
            self.resume_position = os.path.getsize(self.temp_path)
            self.downloaded_bytes = self.resume_position

@dataclass
class DownloadStats:
    """Statistics for download operations."""
    total_downloads: int = 0
    successful_downloads: int = 0
    failed_downloads: int = 0
    total_bytes: int = 0
    start_time: float = 0.0
    peak_concurrent: int = 0
    last_saved: float = 0.0

class SpeedLimiter:
    def __init__(self, max_speed_mbps: float = None):
        self.max_speed_mbps = max_speed_mbps
        self._last_check = time.time()
        self._bytes_since_check = 0
        self._lock = asyncio.Lock()

    async def limit(self, chunk_size: int) -> None:
        if not self.max_speed_mbps:
            return

        async with self._lock:
            self._bytes_since_check += chunk_size
            current_time = time.time()
            elapsed = current_time - self._last_check
            
            if elapsed >= 1:  # Check every second
                current_speed_mbps = (self._bytes_since_check * 8) / (1024 * 1024 * elapsed)
                if current_speed_mbps > self.max_speed_mbps:
                    sleep_time = (current_speed_mbps / self.max_speed_mbps - 1) * elapsed
                    await asyncio.sleep(sleep_time)
                
                self._bytes_since_check = 0
                self._last_check = time.time()

class StatsManager:
    """Manages persistent download statistics."""
    
    def __init__(self, stats_file: str = "download_stats.json"):
        self.stats_file = Path(stats_file)
        self.stats = self._load_stats()
        self._save_lock = asyncio.Lock()
        
    def _load_stats(self) -> DownloadStats:
        """Load statistics from file or create new."""
        try:
            if self.stats_file.exists():
                with open(self.stats_file) as f:
                    data = json.load(f)
                    return DownloadStats(**data)
        except Exception as e:
            logger.error(f"Failed to load stats: {e}")
        return DownloadStats(start_time=time.time())
    
    async def save_stats(self) -> None:
        """Save current statistics to file asynchronously."""
        async with self._save_lock:
            try:
                self.stats.last_saved = time.time()
                stats_data = asdict(self.stats)
                await asyncio.to_thread(self._write_stats_to_file, stats_data)
            except Exception as e:
                logger.error(f"Failed to save stats: {e}")

    def _write_stats_to_file(self, stats_data: dict) -> None:
        """Helper method to write stats to file."""
        with open(self.stats_file, 'w') as f:
            json.dump(stats_data, f, indent=4)

    async def update(self, **kwargs) -> None:
        """Update statistics with new values."""
        for key, value in kwargs.items():
            if hasattr(self.stats, key):
                setattr(self.stats, key, value)
        
        # Auto-save every 5 minutes
        if time.time() - self.stats.last_saved > 300:
            await self.save_stats()

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
            self.active): 
            #and await self.rate_limiter.can_update(self.chat_id)):
            self.last_update = current_time
            await self.send_progress()
            
    async def send_progress(self) -> None:
        """Send progress update message."""
        try:
            if not self.active:
                return
                
            elapsed = time.time() - self.start_time
            percent = min(self.downloaded / self.total_size * 100, 100) if self.total_size > 0 else 0
            
            # Calculate current speed based on recent history
            speed = self._calculate_speed(elapsed)
            speed_text = self._format_size(speed) + "/s"
            
            # Progress bar
            bar_length = 20
            filled_length = int(bar_length * percent / 100)
            bar = '█' * filled_length + '▒' * (bar_length - filled_length)
            
            # Format batch information
            batch_info = f"File {self.position+1}/{self.total_files}\n" if self.total_files > 1 else ""
            
            # Calculate ETA with smoothing
            eta_text = self._calculate_eta(speed)
            
            message = (
                f"📥 Downloading: {self.file_name}\n"
                f"{batch_info}"
                f"Progress: {percent:.1f}%\n"
                f"[{bar}]\n\n"
                f"💾 {self._format_size(self.downloaded)} of {self._format_size(self.total_size)}\n"
                f"⚡ Speed: {speed_text}\n"
                f"⏱️ ETA: {eta_text}"
            )
            
            # Only update if message changed significantly
            if self._should_update_message(message):
                try:
                    await self.bot.edit_message_text(
                        chat_id=self.chat_id,
                        message_id=self.message_id,
                        text=message
                    )
                    self._last_progress_message = message
                    self._last_update_time = time.time()
                except Exception as e:
                    logger.debug(f"Progress update failed (might be rate limited): {e}")
                    
        except Exception as e:
            logger.error(f"Failed to update progress: {e}")

    def _calculate_speed(self, elapsed: float) -> float:
        """Calculate current download speed with smoothing."""
        if not hasattr(self, '_speed_history'):
            self._speed_history = []
            self._last_bytes = 0
            self._last_time = self.start_time
            
        current_time = time.time()
        current_speed = (self.downloaded - self._last_bytes) / (current_time - self._last_time)
        
        # Keep a history of speeds for smoothing
        self._speed_history.append(current_speed)
        if len(self._speed_history) > 5:  # Keep last 5 measurements
            self._speed_history.pop(0)
            
        # Update reference points
        self._last_bytes = self.downloaded
        self._last_time = current_time
        
        # Return smoothed speed
        return sum(self._speed_history) / len(self._speed_history)

    def _calculate_eta(self, speed: float) -> str:
        """Calculate ETA with improved accuracy."""
        if speed <= 0 or self.total_size <= 0:
            return "Calculating..."
            
        bytes_left = self.total_size - self.downloaded
        seconds_left = bytes_left / speed
        
        # Add uncertainty for large files
        if bytes_left > 100 * 1024 * 1024:  # 100MB
            seconds_left *= 1.1  # Add 10% margin
            
        return self._format_time(seconds_left)

    def _should_update_message(self, new_message: str) -> bool:
        """Determine if message should be updated based on changes and rate limiting."""
        now = time.time()
        
        # Always update if first message
        if not hasattr(self, '_last_update_time'):
            self._last_update_time = 0
            
        # Enforce minimum update interval
        if now - self._last_update_time < self.update_interval:
            return False
            
        # Check if progress changed significantly (>1% or 5 seconds passed)
        if (self._last_progress_message != new_message and 
            (now - self._last_update_time > 5 or 
             abs(self.downloaded / self.total_size * 100 - 
                 float(self._last_progress_message.split('%')[0].split(':')[-1].strip() 
                      if self._last_progress_message else 0)) > 1)):
            return True
            
        return False
    
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
                # Calculate average speed
                avg_speed = self.downloaded / elapsed if elapsed > 0 else 0
                avg_speed_text = f"{self._format_size(avg_speed)}/s"
                
                message = (
                    f"✅ Download Complete!\n\n"
                    f"📂 File: {self.file_name}\n"
                    f"📊 Size: {self._format_size(self.downloaded)}\n"
                    f"⏱️ Time: {self._format_time(elapsed)}\n"
                    f"🚀 Avg Speed: {avg_speed_text}\n\n"
                    f"The file will appear in your Jellyfin library shortly."
                )
            else:
                message = (
                    f"❌ Download Failed: {self.file_name}\n\n"
                    f"🔍 ERROR: Download could not be completed\n\n"
                    f"SUGGESTIONS:\n"
                    f"1️⃣ Check your internet connection\n"
                    f"2️⃣ The file may be unavailable from sender\n"
                    f"3️⃣ Try again later"
                )
                
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
        self.batch_id = batch_id or f"BDL-{random.randint(10000, 99999)}"
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
        self.current_file_name = ""
        self.current_file_progress = 0

    async def update(self, downloaded: int = 0, completed: int = 0, failed: int = 0, 
                     current_file: str = None, current_progress: float = None) -> None:
        """
        Update batch progress.
        
        Args:
            downloaded: Additional bytes downloaded
            completed: Additional files completed
            failed: Additional files failed
            current_file: Current file being downloaded
            current_progress: Current file progress percentage
        """
        self.downloaded_bytes += downloaded
        self.completed_files += completed
        self.failed_files += failed
        
        if current_file:
            self.current_file_name = current_file
        
        if current_progress is not None:
            self.current_file_progress = current_progress
        
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
            # Using block elements for more aesthetic progress bar
            bar = '█' * filled_length + '▒' * (bar_length - filled_length)
            
            # Calculate speed
            speed = self.downloaded_bytes / elapsed if elapsed > 0 else 0
            speed_text = self._format_size(speed) + "/s"
            
            # Calculate ETA for entire batch
            eta_text = ""
            if speed > 0 and self.total_size > self.downloaded_bytes:
                bytes_left = self.total_size - self.downloaded_bytes
                seconds_left = bytes_left / speed
                eta_text = self._format_time(seconds_left)
            
            current_file_info = ""
            current_position = self.completed_files + self.failed_files + 1
            if self.current_file_name and current_position <= self.total_files:
                current_progress_str = f"({self.current_file_progress:.0f}% complete)" if self.current_file_progress > 0 else ""
                current_file_info = (
                    f"\n🔽 CURRENT FILE ({current_position}/{self.total_files}):\n"
                    f"{self.current_file_name} {current_progress_str}"
                )
            
            message = (
                f"📦 BATCH DOWNLOAD PROGRESS (ID: {self.batch_id})\n\n"
                f"✅ Completed: {self.completed_files}  |  ❌ Failed: {self.failed_files}  |  ⏳ Remaining: {remaining}\n"
                f"[{bar}]  {percent:.1f}%\n\n"
                f"📊 Overall Progress:\n"
                f"Downloaded: {self._format_size(self.downloaded_bytes)}"
                f"{f' of {self._format_size(self.total_size)}' if self.total_size > 0 else ''}\n"
                f"Speed: {speed_text}"
                f"{f'\nETA: {eta_text}' if eta_text else ''}"
                f"{current_file_info}"
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
            
            # Calculate success rate
            success_rate = (self.completed_files / self.total_files) * 100 if self.total_files > 0 else 0
            
            # Calculate average speed
            avg_speed = self.downloaded_bytes / elapsed if elapsed > 0 else 0
            avg_speed_text = self._format_size(avg_speed) + "/s"
            
            message = (
                f"✅ BATCH DOWNLOAD COMPLETE\n\n"
                f"📊 Summary:\n"
                f"Total files: {self.total_files}\n"
                f"✓ Successfully downloaded: {self.completed_files} ({success_rate:.1f}%)\n"
                f"✗ Failed: {self.failed_files}\n"
                f"💾 Total downloaded: {self._format_size(self.downloaded_bytes)}\n"
                f"⏱️ Time taken: {self._format_time(elapsed)}\n"
                f"🚀 Average speed: {avg_speed_text}\n\n"
                f"All files will appear in your Jellyfin library shortly."
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
        
        # Stats tracking with StatsManager
        self.stats_manager = StatsManager()
        self.download_stats = self.stats_manager.stats
        
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
        asyncio.create_task(self._check_long_downloads())

    async def _check_long_downloads(self) -> None:
        """Periodically check and update status of long-running downloads."""
        while True:
            try:
                now = time.time()

                # Check each active download
                for key, download in list(self.active_downloads.items()):
                    start_time = download.get("start_time", 0)
                    tracker = download.get("tracker")
                    task = download.get("task")

                    # Consider a download "long-running" if it's been going for more than 30 minutes
                    if now - start_time > 1800 and tracker and task:
                        # Check if it's been at least 15 minutes since the last status update
                        last_status_update = download.get("last_status_update", 0)
                        if now - last_status_update > 900:  
                            # Send a status update message
                            try:
                                elapsed = now - start_time
                                progress = 0
                                if tracker.total_size > 0:
                                    progress = (tracker.downloaded / tracker.total_size) * 100

                                # Calculate current speed (based on the last 5 minutes)
                                last_bytes = download.get("last_bytes", 0)
                                last_time = download.get("last_time", start_time)

                                if now - last_time >= 300:  # At least 5 minutes of data
                                    current_speed = (tracker.downloaded - last_bytes) / (now - last_time)
                                    speed_text = self._format_size(current_speed) + "/s"

                                    # Update reference point
                                    download["last_bytes"] = tracker.downloaded
                                    download["last_time"] = now
                                else:
                                    # Calculate from the beginning
                                    current_speed = tracker.downloaded / elapsed if elapsed > 0 else 0
                                    speed_text = self._format_size(current_speed) + "/s"

                                    # Initialize reference point if not set
                                    if "last_bytes" not in download:
                                        download["last_bytes"] = tracker.downloaded
                                        download["last_time"] = now

                                # Calculate ETA
                                eta_text = "unknown"
                                if current_speed > 0 and tracker.total_size > 0:
                                    bytes_left = tracker.total_size - tracker.downloaded
                                    seconds_left = bytes_left / current_speed
                                    eta_text = self._format_time(seconds_left)

                                # Send status update
                                await self.bot.send_message(
                                    task.chat_id,
                                    f"📣 STATUS UPDATE - Large download in progress\n\n"
                                    f"📂 File: {task.file_name}\n"
                                    f"⏱️ Running for: {self._format_time(elapsed)}\n"
                                    f"✅ Progress: {progress:.1f}% complete\n"
                                    f"💾 Downloaded: {self._format_size(tracker.downloaded)} of {self._format_size(tracker. total_size)}\n"
                                    f"⚡ Current speed: {speed_text}\n"
                                    f"🕒 New ETA: {eta_text} remaining\n\n"
                                    f"Download continuing normally..."
                                )

                                # Update the last status update time
                                download["last_status_update"] = now
                            except Exception as e:
                                logger.error(f"Failed to send long download status update: {str(e)}")
            except Exception as e:
                logger.error(f"Error in long download checker: {str(e)}")

            # Check every 5 minutes
            await asyncio.sleep(300)
        
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
        success = False
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
            await self.stats_manager.update(
                total_downloads=self.download_stats.total_downloads + 1,
                successful_downloads=self.download_stats.successful_downloads + (1 if success else 0),
                failed_downloads=self.download_stats.failed_downloads + (0 if success else 1),
                total_bytes=self.download_stats.total_bytes + (task.file_size if success else 0),
                peak_concurrent=max(self.download_stats.peak_concurrent, 
                                  self.max_concurrent_downloads - self.download_semaphore._value)
            )

            if success:
                # Update download stats using attribute access
                self.download_stats.successful_downloads += 1
                try:
                    downloaded_size = os.path.getsize(task.file_path)
                    self.download_stats.total_bytes += downloaded_size
                    tracker.downloaded = downloaded_size  # Ensure tracker has correct final size
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
            self.download_stats.total_downloads += 1
            self.download_stats.failed_downloads += 1
            
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
                    # Add debug logging
                    logger.debug(f"Progress update: {current}/{total} bytes")

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
        speed_limiter = SpeedLimiter(
            max_speed_mbps=self.config.get("download", "max_speed_mbps", 0)
        )
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
                            error_msg = f"HTTP error {response.status}: {response.reason}"
                            await self.bot.edit_message_text(
                                chat_id=task.chat_id,
                                message_id=task.message_id,
                                text=(
                                    f"❌ Download Error: {task.file_name}\n\n"
                                    f"🔍 ERROR: {error_msg}\n\n"
                                    f"Retrying in a moment... ({attempt+1}/{self.max_retries})"
                                )
                            )
                            raise Exception(error_msg)

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
                                await speed_limiter.limit(len(chunk))
                                f.write(chunk)
                                await tracker.update(len(chunk))

                # Check if file exists and has content
                if os.path.exists(task.file_path) and os.path.getsize(task.file_path) > 0:
                    return True
                else:
                    error_msg = "Downloaded file is empty or doesn't exist"
                    await self.bot.edit_message_text(
                        chat_id=task.chat_id,
                        message_id=task.message_id,
                        text=(
                            f"❌ Download Failed: {task.file_name}\n\n"
                            f"🔍 ERROR: {error_msg}\n\n"
                            f"SUGGESTIONS:\n"
                            f"1️⃣ File might be corrupted\n"
                            f"2️⃣ The file may be unavailable from sender\n"
                            f"3️⃣ Storage issues on the server"
                        )
                    )
                    raise Exception(error_msg)

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
                        text=(
                            f"⚠️ Download Interrupted: {task.file_name}\n\n"
                            f"🔄 Retrying in {wait_time:.1f}s... ({attempt+1}/{self.max_retries})\n\n"
                            f"This could be due to:\n"
                            f"• Unstable internet connection\n"
                            f"• Telegram API limitations\n"
                            f"• Server load issues"
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to update retry message: {str(e)}")

                await asyncio.sleep(wait_time)
            else:
                # Final failure message
                try:
                    await self.bot.edit_message_text(
                        chat_id=task.chat_id,
                        message_id=task.message_id,
                        text=(
                            f"❌ Download Failed: {task.file_name}\n\n"
                            f"🔍 ERROR: Maximum retries reached\n\n"
                            f"SUGGESTIONS:\n"
                            f"1️⃣ Check your internet connection\n"
                            f"2️⃣ The file may be unavailable from sender\n" 
                            f"3️⃣ Try downloading again later\n\n"
                            f"Error details: {str(e)[:100]}"
                        )
                    )
                except:
                    pass
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

        status = f"📋 DOWNLOAD QUEUE STATUS\n\n"
        status += f"⏳ Active downloads: {active_count}/{self.max_concurrent_downloads}\n"
        status += f"🔄 Queued files: {queue_length}\n\n"

        if active_count > 0:
            status += "🔽 CURRENTLY DOWNLOADING:\n"
            for i, (key, download) in enumerate(self.active_downloads.items()):
                # Extract filename from path
                filename = os.path.basename(download["path"])

                # Get progress percentage if available
                progress = ""
                if "tracker" in download and hasattr(download["tracker"], "downloaded") and hasattr(download["tracker"],    "total_size"):
                    tracker = download["tracker"]
                    if tracker.total_size > 0:
                        percent = (tracker.downloaded / tracker.total_size) * 100
                        progress = f" ({percent:.0f}% complete)"

                status += f"{i+1}️⃣ {filename}{progress}\n"

        if queue_length > 0:
            # Show up to 5 queued files
            shown_count = min(5, queue_length)

            if shown_count < queue_length:
                status += f"\n⏭️ NEXT IN QUEUE (showing {shown_count} of {queue_length}):\n"
            else:
                status += f"\n⏭️ NEXT IN QUEUE:\n"

            for i, task in enumerate(list(self.download_queue)[:shown_count]):
                # Add file size if available
                size_info = ""
                if task.file_size > 0:
                    size_info = f" ({self._format_size(task.file_size)})"

                status += f"{i+1}. {task.file_name}{size_info}\n"

            if shown_count < queue_length:
                status += f"...and {queue_length - shown_count} more files"

        return status

    def get_stats(self) -> str:
        """
        Get formatted download statistics.

        Returns:
            Statistics string
        """
        # Get statistics from StatsManager
        total = self.download_stats.total_downloads
        successful = self.download_stats.successful_downloads
        failed = self.download_stats.failed_downloads
        total_bytes = self.download_stats.total_bytes
        uptime = time.time() - self.download_stats.start_time

        # Calculate success percentage
        success_rate = (successful / total * 100) if total > 0 else 0

        # Format total bytes
        size_str = self._format_size(total_bytes)

        # Format uptime
        uptime_str = self._format_time(uptime)

        # Current downloads status
        active = self.max_concurrent_downloads - self.download_semaphore._value
        queued = len(self.download_queue)

        # Calculate average download speed
        avg_speed = "N/A"
        avg_time = "N/A"

        if successful > 0 and total_bytes > 0 and uptime > 0:
            speed_bytes_per_sec = total_bytes / uptime
            avg_speed = self._format_size(speed_bytes_per_sec) + "/s"

            # Calculate average time per download
            if successful > 0:
                avg_time = self._format_time(uptime / successful)

        # Find peak concurrent downloads
        peak_concurrent = self.download_stats.peak_concurrent

        return (
            f"📊 DOWNLOAD STATISTICS\n\n"
            f"📆 Bot uptime: {uptime_str}\n"
            f"📥 Files handled: {total}\n\n"
            f"DOWNLOADS:\n"
            f"✅ Successful: {successful} ({success_rate:.1f}%)\n"
            f"❌ Failed: {failed} ({100-success_rate:.1f}%)\n"
            f"💾 Total data: {size_str}\n\n"
            f"PERFORMANCE:\n"
            f"⚡ Average speed: {avg_speed}\n"
            f"⏱️ Average time per file: {avg_time}\n"
            f"📊 Peak concurrent downloads: {peak_concurrent}/{self.max_concurrent_downloads}\n\n"
            f"⏳ Current status: {active} active, {queued} queued"
        )
    
    def _format_size(self, size_bytes: float) -> str:
        """Format bytes to human readable size."""
        if size_bytes < 1024:
            return f"{size_bytes:.0f} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.1f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f} GB"

    def _format_time(self, seconds: float) -> str:
        """Format seconds to human readable time."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            seconds %= 60
            return f"{minutes:.0f}m {seconds:.0f}s"
        else:
            days = int(seconds // (24 * 3600))
            seconds %= (24 * 3600)
            hours = int(seconds // 3600)
            seconds %= 3600
            minutes = int(seconds // 60)
            if days > 0:
                return f"{days}d {hours}h {minutes}m"
            else:
                return f"{hours}h {minutes}m"

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
                "📂 COMMANDS:\n"
                "/start - Show this welcome message\n"
                "/stats - 📊 Show download statistics\n"
                "/queue - 📋 View current download queue\n"
                "/test - 🔍 Run system test\n"
                "/help - ❓ Show usage help\n\n"
                "FILE WATCHER COMMANDS:\n"
                "/fw_unmatched - List and categorize unmatched files\n"
                "/fw_help - Show file watcher commands\n\n"
                "📱 SUPPORTED FORMATS:\n"
                "🎬 Videos - MP4, MKV, AVI, etc.\n"
                "🎵 Audio - MP3, FLAC, WAV, etc.\n"
                "📄 Documents - PDF, ZIP, etc.\n\n"
                "Just send me any supported file to begin!"
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
                test_results.append("✅ Telethon client is connected")
            else:
                test_results.append("❌ Telethon client is not initialized or not connected")

            # Test internet connection
            connection_speed = "Unknown"
            try:
                start_time = time.time()
                session = aiohttp.ClientSession()
                async with session.get("https://www.google.com", timeout=5) as response:
                    resp = await response.read()
                    download_time = time.time() - start_time
                    connection_speed = f"{len(resp) / download_time / 1024:.1f} KB/s"
                    if response.status == 200:
                        test_results.append(f"✅ Internet connection is working")
                    else:
                        test_results.append(f"❌ Internet connection test failed: HTTP {response.status}")
                await session.close()
            except Exception as e:
                test_results.append(f"❌ Internet connection test failed: {str(e)}")

            # Test disk space
            try:
                download_dir = self.download_manager.download_dir
                if os.path.exists(download_dir):
                    total, used, free = shutil.disk_usage(download_dir)
                    free_gb = free / (1024**3)
                    test_results.append(f"✅ Available disk space: {free_gb:.1f} GB")
                    if free_gb < 1:
                        test_results.append(f"⚠️ Warning: Less than 1GB free space remaining")
            except Exception as e:
                pass
            
            # Test queue system
            test_results.append(f"✅ Download queue system is active (limit: {self.download_manager.max_concurrent_downloads})")

            # Performance metrics section
            perf_results = [
                f"\nPERFORMANCE METRICS:",
                f"⚡ Network speed: {connection_speed}" + (" (Good)" if "KB" in connection_speed and float(connection_speed.    split()[0]) > 100 else ""),
                f"⏱️ API response time: {download_time*1000:.0f}ms" + (" (Normal)" if download_time < 1 else " (Slow)")
            ]

            # Send test results
            await self.bot.send_message(
                message.chat.id, 
                f"🔍 SYSTEM TEST RESULTS\n\n"
                f"SYSTEM CHECKS:\n"
                f"{chr(10).join(test_results)}\n"
                f"{chr(10).join(perf_results)}\n\n"
                f"📱 Your bot is {'working properly!' if all('✅' in r for r in test_results) else 'experiencing some issues.'}"
            )

        @self.bot.message_handler(content_types=['document', 'video', 'audio'])
        async def handle_media(message):
            """Handle media file downloads, including forwarded groups."""
            # Track unique file IDs to prevent duplication
            file_id = None
            content_type = ""

            # Get file details based on content type
            if message.content_type == 'document':
                file_info = message.document
                content_type = "📄 Document"
            elif message.content_type == 'video':
                file_info = message.video
                content_type = "🎬 Video"
            elif message.content_type == 'audio':
                file_info = message.audio
                content_type = "🎵 Audio"
            else:
                await self.bot.reply_to(message, "Unsupported file type.")
                return

            # Extract file details
            file_id = file_info.file_id
            file_size = getattr(file_info, 'file_size', 0)
            file_name = getattr(file_info, 'file_name', f"file_{file_id}")

            # Check for duplicates in download queue
            if any(task.file_id == file_id for task in self.download_manager.download_queue):
                logger.info(f"Skipping duplicate file: {file_name}")
                await self.bot.reply_to(message, f"⚠️ This file is already in the download queue.")
                return

            # Generate unique file path
            file_path = self._get_unique_file_path(file_name)

            # Send acknowledgment with improved formatting
            ack_message = await self.bot.send_message(
                message.chat.id, 
                f"📂 File detected: {file_name}\n\n"
                f"{content_type}\n"
                f"📁 Will be stored in: {os.path.dirname(file_path)}/\n\n"
                f"⏳ Added to download queue..."
            )

            # Create and add download task
            task = DownloadTask(
                file_id=file_id,
                file_path=file_path,
                file_size=file_size,
                chat_id=message.chat.id,
                message_id=ack_message.message_id,
                original_message=message,
                file_name=file_name
            )

            # Add to queue
            await self.download_manager.add_task(task)

    def _get_unique_file_path(self, file_name):
        """Generate unique file path."""
        file_path = os.path.join(self.download_manager.download_dir, file_name)
        if not os.path.exists(file_path):
            return file_path
        base, ext = os.path.splitext(file_path)
        counter = 1
        while os.path.exists(f"{base}_{counter}{ext}"):
            counter += 1
        return f"{base}_{counter}{ext}"

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
        offset = 0
        while self.running:
            try:
                logger.info("Starting custom polling...")
                while self.running:
                    updates = await self.bot.get_updates(offset=offset, timeout=30, allowed_updates=['message'])
                    for update in updates:
                        offset = update.update_id + 1
                        await self._process_update(update)
                    await asyncio.sleep(0.5)
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

        # Save stats if needed
        await self.stats_manager.save_stats()
        
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
            # Save stats before stopping
            await self.download_manager.stats_manager.save_stats()
            logger.info("Saving stats before shutdown")

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