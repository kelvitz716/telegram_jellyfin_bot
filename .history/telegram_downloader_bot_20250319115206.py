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
from pathlib import Path
from logging.handlers import RotatingFileHandler
from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio

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
        "retry_delay": 5  # seconds
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

# Progress bar display
class ProgressTracker:
    def __init__(self, total_size, bot, chat_id, message_id, update_interval=5):
        self.total_size = total_size
        self.downloaded = 0
        self.bot = bot
        self.chat_id = chat_id
        self.message_id = message_id
        self.last_update = 0
        self.update_interval = update_interval
        self.start_time = time.time()
        self.active = True

    def update(self, chunk_size):
        self.downloaded += chunk_size
        
        current_time = time.time()
        if current_time - self.last_update > self.update_interval and self.active:
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
            
            # Format progress message
            message = (
                f"Downloading file...\n"
                f"{self._format_size(self.downloaded)} / {self._format_size(self.total_size)}\n"
                f"[{bar}] {percent:.1f}%\n"
                f"Speed: {speed_text}"
            )
            
            # Update message
            if message != getattr(self, "_last_progress_message", ""):
                self.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                    text=message
                )
                
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
            
            if success:
                message = (
                    f"✅ Download complete!\n"
                    f"File size: {self._format_size(self.downloaded)}\n"
                    f"Time: {elapsed:.1f} seconds"
                )
            else:
                message = "❌ Download failed. Please try again."
                
            self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=message
            )
        except Exception as e:
            logger.error(f"Failed to update completion status: {str(e)}")

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
        
        # Download settings
        self.chunk_size = self.config["download"]["chunk_size"]
        self.progress_update_interval = self.config["download"]["progress_update_interval"]
        self.max_retries = self.config["download"]["max_retries"]
        self.retry_delay = self.config["download"]["retry_delay"]
        
        # Initialize bot
        self.bot = telebot.TeleBot(self.bot_token)
        
        # Initialize Telethon client
        self.client = None
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._setup_telethon()
        
        # Set up bot handlers
        self._setup_handlers()
    
    def _setup_telethon(self):
        """Set up Telethon client for large file downloads."""
        try:
            # Create a session file path
            session_file = "telegram_downloader_session"

            # Start the client
            self.client = TelegramClient(
                session_file, 
                self.api_id, 
                self.api_hash, 
                loop=self.loop
            )

            # Connect the client
            self.loop.run_until_complete(self.client.start(bot_token=self.bot_token))
            logger.info("Telethon bot client started and authorized.")

            # Check if already authorized
            if not self.loop.run_until_complete(self.client.is_user_authorized()):
                logger.warning("Telethon client not authorized. Please run the auth script separately.")
                # Disconnect since we can't use it
                self.loop.run_until_complete(self.client.disconnect())
                self.client = None
            else:
                logger.info("Telethon client authorized successfully.")

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
                "/test - Run a system test\n"
                "/help - Show this message"
            )
            self.bot.reply_to(message, welcome_text)
        
        @self.bot.message_handler(commands=['stats'])
        def show_stats(message):
            stats = self._get_stats()
            self.bot.reply_to(message, stats)
        
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
            
            # Send test results
            test_results = f"{dir_test}\n{api_test}\n{telethon_test}\n{net_test}"
            self.bot.send_message(message.chat.id, f"System Test Results:\n\n{test_results}")
        
        @self.bot.message_handler(content_types=['document', 'video', 'audio'])
        def handle_media(message):
            """Handle media file downloads."""
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
            file_size = file_info.file_size
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
            progress_message = self.bot.send_message(message.chat.id, "Preparing download...")
            
            # Start download in background
            download_thread = threading.Thread(
                target=self._download_file,
                args=(file_id, file_path, file_size, message.chat.id, progress_message.message_id, message)
            )
            download_thread.daemon = True
            download_thread.start()

    async def _download_with_telethon(self, message, file_path, progress_callback):
        """Download a file using Telethon for better large file handling."""
        try:
            # Ensure client is connected
            if not self.client.is_connected():
                await self.client.connect()
                
            if not await self.client.is_user_authorized():
                logger.error("Telethon client is not authorized. Cannot download large files.")
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
                logger.error("Telethon download failed: No file was downloaded")
                return False
                
        except Exception as e:
            logger.error(f"Telethon download error: {str(e)}")
            return False

    def _download_file(self, file_id, file_path, file_size, chat_id, message_id, original_message=None):
        """Download a file with progress tracking."""
        try:
            # Get file info
            #file_info = self.bot.get_file(file_id)
            #file_size = file_info.file_size if hasattr(file_info, 'file_size') else 0
            
            # Create progress tracker
            tracker = ProgressTracker(
                file_size, self.bot, chat_id, message_id, 
                update_interval=self.progress_update_interval
            )
            
            # Track download
            download_key = f"{chat_id}_{file_id}"
            active_downloads[download_key] = {
                "path": file_path,
                "tracker": tracker,
                "start_time": time.time()
            }
            
            # First try Telethon for larger files if available
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
                
                # Notify about successful download
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
        
        # Current active downloads
        active = len(active_downloads)
        
        return (
            f"📊 Download Statistics\n\n"
            f"🕒 Uptime: {uptime_str}\n"
            f"📥 Total downloads: {total}\n"
            f"✅ Successful: {successful}\n"
            f"❌ Failed: {failed}\n"
            f"💾 Total downloaded: {size_str}\n"
            f"⏳ Active downloads: {active}"
        )

    def run(self):
        """Run the Telegram bot."""
        logger.info("Starting Telegram Downloader Bot")
        print("Bot started. Press Ctrl+C to exit.")
        
        try:
            self.bot.polling(none_stop=True, interval=1)
        except Exception as e:
            logger.error(f"Error in bot polling: {str(e)}")
        finally:
            # Clean up Telethon client
            if self.client:
                self.loop.run_until_complete(self.client.disconnect())
            logger.info("Bot stopped")

def main():
    downloader = TelegramDownloader()
    downloader.run()

if __name__ == "__main__":
    main()