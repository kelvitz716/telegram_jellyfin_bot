#!/usr/bin/env python3
"""
telegram_downloader.py - Downloads media files from Telegram
"""
import os
import time
import json
import logging
import telebot
import requests
import threading
from logging.handlers import RotatingFileHandler
from telethon import TelegramClient
import asyncio
from collections import deque
from dataclasses import dataclass, field

# Configuration
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
        "max_concurrent_downloads": 3  # New setting for concurrent downloads
    }
}

# Track active downloads
active_downloads = {}
download_stats = {
    "total_downloads": 0,
    "successful_downloads": 0,
    "failed_downloads": 0,
    "total_bytes": 0,
    "start_time": time.time()
}

# Global logger
logger = None

# Download queue 
download_queue = deque()
queue_lock = threading.Lock()
download_semaphore = None  # Will be initialized based on config

@dataclass
class DownloadTask:
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

class RateLimiter:
    def __init__(self):
        self.last_update = {}
        self.min_interval = 2  # Minimum seconds between updates for each chat
        self.lock = threading.Lock()
        
    def can_update(self, chat_id):
        with self.lock:
            now = time.time()
            if chat_id not in self.last_update:
                self.last_update[chat_id] = now
                return True
            
            # Check if enough time has passed
            if now - self.last_update[chat_id] >= self.min_interval:
                self.last_update[chat_id] = now
                return True
            return False

# Progress bar display
class ProgressTracker:
    def __init__(self, total_size, bot, chat_id, message_id, update_interval=5, 
                 file_name="", batch_id=None, position=0, total_files=1):
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
        self.rate_limiter = RateLimiter()

    def update(self, chunk_size):
        self.downloaded += chunk_size
        
        current_time = time.time()
        if (current_time - self.last_update > self.update_interval and 
            self.active and 
            self.rate_limiter.can_update(self.chat_id)):
            self.last_update = current_time
            self.send_progress()
            
    def send_progress(self):
        try:
            if not self.active:
                return
                
            elapsed = time.time() - self.start_time
            percent = min(self.downloaded / self.total_size * 100, 100) if self.total_size > 0 else 0
            
            # Calculate speed
            speed = self.downloaded / elapsed if elapsed > 0 else 0
            speed_text = self._format_size(speed) + "/s"
            
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
                f"Speed: {speed_text}"
            )
            
            # Update message if changed and rate limited
            if (message != getattr(self, "_last_progress_message", "") and 
                self.rate_limiter.can_update(self.chat_id)):
                self.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                    text=message
                )
                self._last_progress_message = message
        except Exception as e:
            logger.error(f"Failed to update progress: {str(e)}")

    def _format_size(self, size_bytes):
        """Format bytes into human-readable size."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.1f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f} GB"
            
    def complete(self, success=True):
        """Mark download as complete."""
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
                    f"Time: {elapsed:.1f} seconds"
                )
            else:
                message = f"❌ Download failed. {batch_info}{self.file_name}"
                
            # Final completion message ignores rate limiting
            self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=message
            )
        except Exception as e:
            logger.error(f"Failed to update completion status: {str(e)}")

class BatchProgressTracker:
    """Track progress for a batch of files"""
    def __init__(self, bot, chat_id, message_id, total_files, total_size=0, batch_id=None):
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
        self.rate_limiter = RateLimiter()

    def update(self, downloaded=0, completed=0, failed=0):
        """Update batch progress"""
        self.downloaded_bytes += downloaded
        self.completed_files += completed
        self.failed_files += failed
        
        current_time = time.time()
        if (current_time - self.last_update > self.update_interval and 
            self.active and 
            self.rate_limiter.can_update(self.chat_id)):
            self.last_update = current_time
            self.send_progress()

    def send_progress(self):
        """Send batch progress update"""
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
            
            message = (
                f"Batch Download Progress\n"
                f"Completed: {self.completed_files} | Failed: {self.failed_files} | Remaining: {remaining}\n"
                f"[{bar}] {percent:.1f}%\n"
                f"Total Downloaded: {self._format_size(self.downloaded_bytes)}"
            )
            
            # Update message if changed and rate limited
            if (message != getattr(self, "_last_progress_message", "") and 
                self.rate_limiter.can_update(self.chat_id)):
                self.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                    text=message
                )
                self._last_progress_message = message
        except Exception as e:
            logger.error(f"Failed to update batch progress: {str(e)}")

    def complete(self):
        """Mark batch as complete"""
        self.active = False
        try:
            elapsed = time.time() - self.start_time
            
            message = (
                f"✅ Batch Download Complete\n"
                f"Total Files: {self.total_files}\n"
                f"Successful: {self.completed_files}\n"
                f"Failed: {self.failed_files}\n"
                f"Total Downloaded: {self._format_size(self.downloaded_bytes)}\n"
                f"Time: {elapsed:.1f} seconds"
            )
            
            # Final completion message ignores rate limiting
            self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=message
            )
        except Exception as e:
            logger.error(f"Failed to update batch completion status: {str(e)}")

    def _format_size(self, size_bytes):
        """Format bytes into human-readable size."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.1f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f} GB"

class TelegramDownloader:
    def __init__(self, config_path: str = CONFIG_FILE):
        """Initialize the Telegram Downloader with configuration."""
        self.config = self._load_or_create_config(config_path)
        self._setup_logging()
        self._setup_directories()
        
        # Initialize Telegram bot
        self.bot_token = self.config["telegram"]["bot_token"]
        self.api_id = self.config["telegram"]["api_id"]
        self.api_hash = self.config["telegram"]["api_hash"]

        # Rate limiter for message updates
        self.rate_limiter = RateLimiter()
        
        # Download settings
        self.chunk_size = self.config["download"]["chunk_size"]
        self.progress_update_interval = self.config["download"]["progress_update_interval"]
        self.max_retries = self.config["download"]["max_retries"]
        self.retry_delay = self.config["download"]["retry_delay"]
        self.max_concurrent_downloads = self.config["download"].get("max_concurrent_downloads", 3)
        
        # Initialize download semaphore
        global download_semaphore
        download_semaphore = threading.Semaphore(self.max_concurrent_downloads)
        
        # Initialize batch trackers
        self.batch_trackers = {}
        
        # Initialize bot
        self.bot = telebot.TeleBot(self.bot_token)
        
        # Initialize Telethon client
        self.client = None
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._setup_telethon()
        
        # Set up bot handlers
        self._setup_handlers()
        
        # Start queue processor thread
        self.queue_processor_thread = threading.Thread(target=self._process_download_queue)
        self.queue_processor_thread.daemon = True
        self.queue_processor_thread.start()

    async def _async_setup_telethon(self):
        """Set up Telethon client asynchronously."""
        try:
            # Create a session file path
            session_file = "telegram_bot_session.session"

            # Start the client
            self.client = TelegramClient(
                session_file, 
                self.api_id, 
                self.api_hash, 
                loop=self.loop
            )

            # Connect the client
            await self.client.start(bot_token=self.bot_token)

            # Check if authorized
            if not await self.client.is_user_authorized():
                logger.warning("Telethon client not authorized")
                return False
            else:
                logger.info("Telethon client authorized successfully")
                return True

        except Exception as e:
            logger.error(f"Failed to initialize Telethon client: {str(e)}")
            return False
    
    def _setup_telethon(self):
        """Set up Telethon client for large file downloads."""
        try:
            success = self.loop.run_until_complete(self._async_setup_telethon())
            if not success:
                self.client = None
        except Exception as e:
            logger.error(f"Failed to initialize Telethon client: {str(e)}")
            self.client = None
    
    def _load_or_create_config(self, config_path: str):
        """Load configuration from file or create default if not exists."""
        if not os.path.exists(config_path):
            with open(config_path, 'w') as f:
                json.dump(DEFAULT_CONFIG, f, indent=4)
            print(f"Created default configuration file at {config_path}")
            print("Please edit this file with your Telegram API credentials")
            return DEFAULT_CONFIG
        
        with open(config_path, 'r') as f:
            return json.load(f)

    def _setup_logging(self):
        """Set up logging with rotating file handler."""
        global logger
        
        log_level = getattr(logging, self.config["logging"].get("level", "INFO"))
        max_size_mb = self.config["logging"].get("max_size_mb", 10)
        backup_count = self.config["logging"].get("backup_count", 5)
        
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

    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        self.download_dir = self.config["paths"]["telegram_download_dir"]
        print(f"Trying to create directory: {self.download_dir}")
        os.makedirs(self.download_dir, exist_ok=True)

    def _setup_handlers(self):
        """Set up Telegram bot message handlers."""
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
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
            self.bot.reply_to(message, welcome_text)
        
        @self.bot.message_handler(commands=['stats'])
        def show_stats(message):
            stats = self._get_stats()
            self.bot.reply_to(message, stats)
        
        @self.bot.message_handler(commands=['queue'])
        def show_queue(message):
            queue_status = self._get_queue_status()
            self.bot.reply_to(message, queue_status)
        
        @self.bot.message_handler(commands=['test'])
        def run_test(message):
            self.bot.reply_to(message, "🔍 Running system test...")
            
            # Test directory access
            try:
                print(f"Trying to create directory: {self.download_dir}")
                test_file = os.path.join(self.download_dir, ".test_file")
                with open(test_file, "w") as f:
                    f.write("test")
                os.remove(test_file)
                dir_test = "✅ Download directory is writable"
            except Exception as e:
                dir_test = f"❌ Download directory test failed: {str(e)}"
            
            # Test bot API
            try:
                test_msg = self.bot.send_message(message.chat.id, "Testing bot API...")
                self.bot.delete_message(message.chat.id, test_msg.message_id)
                api_test = "✅ Telegram Bot API is working"
            except Exception as e:
                api_test = f"❌ Telegram Bot API test failed: {str(e)}"
            
            # Test Telethon connection
            if self.client:
                try:
                    me = self.loop.run_until_complete(self.client.get_me())
                    if me:
                        telethon_test = "✅ Telethon client is working"
                    else:
                        telethon_test = "❌ Telethon client test failed: Could not get user info"
                except Exception as e:
                    telethon_test = f"❌ Telethon client test failed: {str(e)}"
            else:
                telethon_test = "❌ Telethon client is not initialized"
            
            # Test internet connection
            try:
                requests.get("https://www.google.com", timeout=5)
                net_test = "✅ Internet connection is working"
            except Exception as e:
                net_test = f"❌ Internet connection test failed: {str(e)}"
            
            # Test queue system
            queue_test = f"✅ Download queue system is active (limit: {self.max_concurrent_downloads})"
            
            # Send test results
            test_results = f"{dir_test}\n{api_test}\n{telethon_test}\n{net_test}\n{queue_test}"
            self.bot.send_message(message.chat.id, f"System Test Results:\n\n{test_results}")
        
        @self.bot.message_handler(content_types=['document', 'video', 'audio'])
        def handle_media(message):
            """Handle media file downloads, including multiple attachments."""
            # Check for media group (multiple files)
            media_group_id = message.media_group_id if hasattr(message, 'media_group_id') else None
            
            # Single file handling (no media group)
            if not media_group_id:
                if message.content_type == 'document':
                    file_info = message.document
                elif message.content_type == 'video':
                    file_info = message.video
                elif message.content_type == 'audio':
                    file_info = message.audio
                else:
                    self.bot.reply_to(message, "Unsupported file type. Please send a media file.")
                    return
                
                # Get file details
                file_id = file_info.file_id
                file_size = file_info.file_size if hasattr(file_info, 'file_size') else 0
                file_name = file_info.file_name if hasattr(file_info, 'file_name') else f"file_{file_id}"
                
                # Check if file exists
                file_path = os.path.join(self.download_dir, file_name)
                if os.path.exists(file_path):
                    # Find a new unique name
                    base, ext = os.path.splitext(file_path)
                    counter = 1
                    while os.path.exists(f"{base}_{counter}{ext}"):
                        counter += 1
                    file_path = f"{base}_{counter}{ext}"
                    file_name = os.path.basename(file_path)
                
                # Send acknowledgment
                self.bot.reply_to(message, f"📥 Received request to download: {file_name}")
                progress_message = self.bot.send_message(message.chat.id, "Added to download queue...")
                
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
                self._add_to_queue(task)
                
            else:
                # Media group handling - this is a bit tricky since media groups 
                # might send multiple messages that need to be grouped together
                
                # First, check if we've already seen this media group
                if media_group_id in self.batch_trackers:
                    # Add to existing batch
                    batch_tracker = self.batch_trackers[media_group_id]
                    
                    # Update the batch message
                    try:
                        self.bot.edit_message_text(
                            chat_id=batch_tracker.chat_id,
                            message_id=batch_tracker.message_id,
                            text=f"Collecting files from group message... ({batch_tracker.total_files + 1} files found)"
                        )
                    except Exception as e:
                        logger.error(f"Failed to update batch message: {str(e)}")
                        
                    # Increment total files count
                    batch_tracker.total_files += 1
                else:
                    # Create a new batch tracker
                    batch_message = self.bot.send_message(
                        message.chat.id, 
                        "Collecting files from group message... (1 file found)"
                    )
                    
                    # Initialize a new batch tracker
                    batch_tracker = BatchProgressTracker(
                        bot=self.bot,
                        chat_id=message.chat.id,
                        message_id=batch_message.message_id,
                        total_files=1,
                        batch_id=media_group_id
                    )
                    
                    # Store the batch tracker
                    self.batch_trackers[media_group_id] = batch_tracker
                    
                    # Set a timer to process this batch after collection
                    # This is needed because Telegram sends group messages as separate updates
                    threading.Timer(3.0, self._process_media_group, args=[media_group_id]).start()
                
                # Process this file
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
                
                # Check if file exists
                file_path = os.path.join(self.download_dir, file_name)
                if os.path.exists(file_path):
                    # Find a new unique name
                    base, ext = os.path.splitext(file_path)
                    counter = 1
                    while os.path.exists(f"{base}_{counter}{ext}"):
                        counter += 1
                    file_path = f"{base}_{counter}{ext}"
                    file_name = os.path.basename(file_path)
                
                # Store file in the batch data
                if not hasattr(batch_tracker, 'files'):
                    batch_tracker.files = []
                    
                batch_tracker.files.append({
                    'file_id': file_id,
                    'file_path': file_path,
                    'file_size': file_size,
                    'file_name': file_name,
                    'original_message': message
                })
                
                # Update total size
                batch_tracker.total_size += file_size

    def _process_media_group(self, media_group_id):
        """Process a complete media group after collection period."""
        try:
            # Get the batch tracker
            if media_group_id not in self.batch_trackers:
                logger.error(f"Media group {media_group_id} not found in batch trackers")
                return
                
            batch_tracker = self.batch_trackers[media_group_id]
            
            # Update the batch message with final count
            try:
                self.bot.edit_message_text(
                    chat_id=batch_tracker.chat_id,
                    message_id=batch_tracker.message_id,
                    text=f"Starting batch download of {len(batch_tracker.files)} files..."
                )
            except Exception as e:
                logger.error(f"Failed to update batch message: {str(e)}")
            
            # Create individual progress messages for each file
            for i, file_data in enumerate(batch_tracker.files):
                progress_message = self.bot.send_message(
                    batch_tracker.chat_id, 
                    f"Queued: {file_data['file_name']} ({i+1}/{len(batch_tracker.files)})"
                )
                
                # Create download task
                task = DownloadTask(
                    file_id=file_data['file_id'],
                    file_path=file_data['file_path'],
                    file_size=file_data['file_size'],
                    chat_id=batch_tracker.chat_id,
                    message_id=progress_message.message_id,
                    original_message=file_data['original_message'],
                    file_name=file_data['file_name'],
                    batch_id=media_group_id,
                    position=i,
                    total_files=len(batch_tracker.files)
                )
                
                # Add to queue
                self._add_to_queue(task)
            
            # Update batch tracker
            batch_tracker.send_progress()
            
        except Exception as e:
            logger.error(f"Error processing media group {media_group_id}: {str(e)}")

    def _add_to_queue(self, task):
        """Add a download task to the queue."""
        with queue_lock:
            download_queue.append(task)
            
        # Update the progress message to show queue position
        queue_position = len(download_queue)
        try:
            if queue_position > 1:
                self.bot.edit_message_text(
                    chat_id=task.chat_id,
                    message_id=task.message_id,
                    text=f"In download queue... (position: {queue_position})"
                )
        except Exception as e:
            logger.error(f"Failed to update queue position message: {str(e)}")
            
        logger.info(f"Added {task.file_name} to download queue (position {queue_position})")

    def _process_download_queue(self):
        """Process the download queue in the background."""
        logger.info("Download queue processor started")
        
        while True:
            task = None
            
            # Get next task from queue
            with queue_lock:
                if download_queue:
                    task = download_queue.popleft()
            
            if task:
                # Wait for a download slot to become available
                with download_semaphore:
                    # Update task status
                    task.status = "downloading"
                    
                    # Update message to show download is starting
                    try:
                        self.bot.edit_message_text(
                            chat_id=task.chat_id,
                            message_id=task.message_id,
                            text=f"Starting download: {task.file_name}..."
                        )
                    except Exception as e:
                        logger.error(f"Failed to update task status message: {str(e)}")
                    
                    # Process the download
                    self._download_file(
                        task.file_id, 
                        task.file_path, 
                        task.file_size, 
                        task.chat_id, 
                        task.message_id, 
                        task.original_message,
                        task.file_name,
                        task.batch_id,
                        task.position,
                        task.total_files
                    )
            
            # Sleep to avoid CPU spinning
            time.sleep(0.5)

    def _get_queue_status(self):
        """Get formatted status of the download queue."""
        with queue_lock:
            queue_length = len(download_queue)
            
        active_count = self.max_concurrent_downloads - download_semaphore._value
        
        if queue_length == 0 and active_count == 0:
            return "No active downloads or queued files."
            
        status = f"📥 Download Queue Status\n\n"
        status += f"⏳ Active downloads: {active_count}/{self.max_concurrent_downloads}\n"
        status += f"🔄 Queued files: {queue_length}\n\n"
        
        if active_count > 0:
            status += "Currently downloading:\n"
            for key, download in active_downloads.items():
                # Extract filename from path
                filename = os.path.basename(download["path"])
                status += f"- {filename}\n"
        
        if queue_length > 0 and queue_length <= 5:
            # Show up to 5 queued files
            status += "\nNext in queue:\n"
            with queue_lock:
                for i, task in enumerate(list(download_queue)[:5]):
                    status += f"{i+1}. {task.file_name}\n"
        elif queue_length > 5:
            status += f"\nShowing first 5 of {queue_length} queued files:\n"
            with queue_lock:
                for i, task in enumerate(list(download_queue)[:5]):
                    status += f"{i+1}. {task.file_name}\n"
            
        return status
    

    async def _check_telethon_connection(self, max_retries=3):
        """Check Telethon connection and reconnect if needed."""
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
                    logger.debug("Telethon connection verified")
                    return True
                    
            except Exception as e:
                retry_count += 1
                logger.warning(f"Telethon connection check failed (attempt {retry_count}/{max_retries}): {str(e)}")
                if retry_count < max_retries:
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                    
            return False
        return False

    async def _download_with_telethon(self, message, file_path, progress_callback):
        """Download a file using Telethon for better large file handling."""
        try:
            # Check and restore connection if needed
            if not await self._check_telethon_connection():
                logger.error("Failed to establish Telethon connection")
                return False
                
            # For message objects from Bot API, we need to get the real message via Telethon
            if hasattr(message, 'chat') and hasattr(message, 'message_id'):
                try:
                    # Try to get the original message through Telethon
                    telethon_message = await self.client.get_messages(
                        message.chat.id, ids=message.message_id
                    )
                    if telethon_message:
                        message = telethon_message
                except Exception as e:
                    logger.error(f"Failed to get original message via Telethon: {str(e)}")
                    return False
            
            # Set up retries for download
            max_download_retries = 3
            retry_count = 0
            
            while retry_count < max_download_retries:
                try:
                    # Download the file
                    downloaded_file = await self.client.download_media(
                        message,
                        file_path,
                        progress_callback=progress_callback
                    )
                    
                    # Verify download
                    if downloaded_file and os.path.exists(downloaded_file):
                        logger.info(f"Telethon download successful: {downloaded_file}")
                        return True
                    else:
                        raise Exception("No file was downloaded")
                        
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"Download attempt {retry_count} failed: {str(e)}")
                    
                    if retry_count < max_download_retries:
                        # Check connection before retry
                        if not await self._check_telethon_connection():
                            logger.error("Failed to reconnect Telethon client")
                            return False
                        await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                        continue
                    else:
                        logger.error(f"All download attempts failed after {max_download_retries} retries")
                        return False
                        
        except Exception as e:
            logger.error(f"Telethon download error: {str(e)}")
            return False

    def _download_file(self, file_id, file_path, file_size, chat_id, message_id, 
                      original_message=None, file_name="", batch_id=None, position=0, total_files=1):
        """Download a file with progress tracking."""
        try:
            # Create progress tracker
            tracker = ProgressTracker(
                file_size, self.bot, chat_id, message_id, 
                update_interval=self.progress_update_interval,
                file_name=file_name,
                batch_id=batch_id,
                position=position,
                total_files=total_files
            )
            
            # Track download
            download_key = f"{chat_id}_{file_id}"
            active_downloads[download_key] = {
                "path": file_path,
                "tracker": tracker,
                "start_time": time.time(),
                "batch_id": batch_id
            }
            
            # Update batch tracker if this is part of a batch
            if batch_id and batch_id in self.batch_trackers:
                self.batch_trackers[batch_id].send_progress()
            
            # First try Telethon for larger files if available
            logger.debug(f"Telethon client status: exists={self.client is not None}, connected={self.client.is_connected() if self.client else False}")
            use_telethon = (self.client is not None and 
                self.client.is_connected() and 
                file_size > 20 * 1024 * 1024 and 
                original_message)
            
            success = False
            retries = 0
            
            while retries <= self.max_retries and not success:
                try:
                    tracker.send_progress()  # Initial progress message
                    
                    if use_telethon:
                        logger.info(f"Using Telethon for large file ({file_size / (1024*1024):.2f} MB): {file_path}")
                        
                        # Create a callback for progress updates
                        async def progress_callback(current, total):
                            tracker.downloaded = current
                            tracker.total_size = total
                            # Don't call update() to avoid double counting
                            tracker.send_progress()
                        
                        # Use Telethon for download
                        success = self.loop.run_until_complete(
                            self._download_with_telethon(original_message, file_path, progress_callback)
                        )
                    else:
                        # Use standard Bot API for download
                        logger.info(f"Using Bot API for download ({file_size / (1024*1024):.2f} MB): {file_path}")
                        # For small files, you can safely call get_file now:
                        file_info = self.bot.get_file(file_id)
                        file_url = f"https://api.telegram.org/file/bot{self.bot_token}/{file_info.file_path}"
                        
                        # Start download
                        with requests.get(file_url, stream=True) as response:
                            response.raise_for_status()
                            
                            # Get actual file size if not known
                            if file_size == 0 and 'content-length' in response.headers:
                                file_size = int(response.headers['content-length'])
                                tracker.total_size = file_size
                            
                            # Save file
                            with open(file_path, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=self.chunk_size):
                                    if chunk:
                                        f.write(chunk)
                                        tracker.update(len(chunk))
                        
                        success = True
                    
                except (requests.RequestException, IOError) as e:
                    logger.error(f"Download error (attempt {retries+1}/{self.max_retries}): {str(e)}")
                    retries += 1
                    
                    if retries <= self.max_retries:
                        time.sleep(self.retry_delay)
                        self.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=f"⚠️ Download interrupted. Retrying... ({retries}/{self.max_retries})"
                        )
                        time.sleep(1)  # Give time for message to be displayed
            
            # Update stats
            download_stats["total_downloads"] += 1
            if success:
                download_stats["successful_downloads"] += 1
                file_size = os.path.getsize(file_path)
                download_stats["total_bytes"] += file_size
                tracker.downloaded = file_size  # Ensure tracker has correct final size
                tracker.complete(success=True)
                logger.info(f"Download complete: {file_path}")
                
                # Only notify on success if this is not part of a batch
                if not batch_id:
                    self.bot.send_message(
                        chat_id=chat_id,
                        text=f"✅ Download complete! Media sorted by the file watcher will appear in your Jellyfin library."
                    )
            else:
                download_stats["failed_downloads"] += 1
                tracker.complete(success=False)
                logger.error(f"Download failed after {self.max_retries} retries: {file_path}")
                
                # Try to delete partially downloaded file
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as e:
                    logger.error(f"Failed to remove partial file: {str(e)}")
            
            # Update batch tracker if this is part of a batch
            if batch_id and batch_id in self.batch_trackers:
                batch_tracker = self.batch_trackers[batch_id]
                if success:
                    batch_tracker.update(downloaded=file_size, completed=1)
                else:
                    batch_tracker.update(failed=1)
                    
                # Check if this was the last file in the batch
                completed_and_failed = batch_tracker.completed_files + batch_tracker.failed_files
                if completed_and_failed >= batch_tracker.total_files:
                    # Final batch notification
                    batch_tracker.complete()
                    
                    # Send a final notification about the batch
                    if batch_tracker.completed_files > 0:
                        self.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"✅ Batch download complete! {batch_tracker.completed_files}/{batch_tracker.total_files} files downloaded.\n"
                                f"Media sorted by the file watcher will appear in your Jellyfin library."
                            )
                        )
                    
                    # Clean up batch tracker
                    del self.batch_trackers[batch_id]
            
            # Remove from active downloads
            if download_key in active_downloads:
                del active_downloads[download_key]
                
        except Exception as e:
            logger.error(f"Error downloading file: {str(e)}")
            
            try:
                self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=f"❌ Download failed: {str(e)}"
                )
            except:
                pass
            
            # Update stats
            download_stats["total_downloads"] += 1
            download_stats["failed_downloads"] += 1
            
            # Update batch tracker if this is part of a batch
            if batch_id and batch_id in self.batch_trackers:
                self.batch_trackers[batch_id].update(failed=1)
            
            # Remove from active downloads
            download_key = f"{chat_id}_{file_id}"
            if download_key in active_downloads:
                del active_downloads[download_key]

    def _get_stats(self):
        """Get formatted download statistics."""
        total = download_stats["total_downloads"]
        successful = download_stats["successful_downloads"]
        failed = download_stats["failed_downloads"]
        total_bytes = download_stats["total_bytes"]
        uptime = time.time() - download_stats["start_time"]
        
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
        active = len(active_downloads)
        queued = len(download_queue)
        
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

    def run(self):
        """Run the Telegram bot with timeout handling, connection checks, and proper exit."""
        logger.info("Starting Telegram Downloader Bot")
        print("Bot started. Press Ctrl+C to exit.")
        
        # Flag for controlled shutdown
        self.running = True
        
        # Start connection check thread
        check_thread = threading.Thread(target=self._connection_check)
        check_thread.daemon = True
        check_thread.start()
        
        while self.running:
            try:
                logger.info("Starting bot polling...")
                # Use timeout parameter and skip pending updates on reconnection
                self.bot.polling(none_stop=True, 
                               interval=1, 
                               timeout=30,
                               skip_pending=True)
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                self.running = False
                break
            except telebot.apihelper.ApiException as e:
                logger.error(f"Telegram API error: {str(e)}")
                time.sleep(5)  # Wait before retry on API error
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error: {str(e)}")
                time.sleep(10)  # Wait longer on network errors
            except Exception as e:
                logger.error(f"Unexpected error in bot polling: {str(e)}")
                time.sleep(3)  # Brief wait before retry
            finally:
                if self.running:  # Only attempt reconnect if not shutting down
                    try:
                        self.bot.delete_webhook()
                    except:
                        pass
                    logger.info("Bot disconnected, attempting to reconnect...")
                    time.sleep(1)
        
        # Cleanup on exit
        logger.info("Shutting down...")
        try:
            if self.client:
                self.loop.run_until_complete(self.client.disconnect())
            self.bot.stop_polling()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        
        logger.info("Bot stopped")
    
    def _connection_check(self):
        """Periodic connection check thread."""
        while self.running:
            try:
                # Test connection by getting bot info
                self.bot.get_me()
                logger.debug("Connection check: OK")
            except Exception as e:
                logger.warning(f"Connection check failed: {str(e)}")
                try:
                    # Attempt to reset connection
                    self.bot.delete_webhook()
                except:
                    pass
            finally:
                # Sleep for 60 seconds before next check
                for _ in range(60):
                    if not self.running:
                        break
                    time.sleep(1)

def main():
    downloader = None
    try:
        downloader = TelegramDownloader()
        downloader.run()
    except KeyboardInterrupt:
        print("\nReceived shutdown signal (Ctrl+C)")
        if downloader:
            # Set running flag to False to stop the bot properly
            downloader.running = False
            # Allow time for threads to clean up
            print("Shutting down gracefully...")
            time.sleep(2)
    except Exception as e:
        print(f"Error: {str(e)}")
        if downloader:
            downloader.running = False
    finally:
        print("Bot stopped")

if __name__ == "__main__":
    main()