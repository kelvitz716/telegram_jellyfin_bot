#main.py

import asyncio
import os
import sys
import argparse
import time
from typing import Optional

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from config.configuration_manager import ConfigurationManager
from services.logging_service import get_logger
from telegram.downloader import MultiSourceDownloader, TelegramMediaDownloader
from file_processing.watcher import FileWatcher
from file_processing.categorizer import MediaCategorizer
from services.notification_service import send_notification
from services.concurrent_service import ConcurrentService
from utils.progress_tracker import ProgressTracker

class MediaManagerApp:
    """
    Central application orchestrator for media management system.
    """
    def __init__(
        self, 
        config_manager: Optional[ConfigurationManager] = None
    ):
        """
        Initialize media manager application.
        
        Args:
            config_manager: Configuration management instance
        """
        self.config_manager = config_manager or ConfigurationManager()
        self.logger = get_logger('MediaManagerApp')
        
        # Safely get download directory
        self.download_dir = self._get_download_directory()
        
        # Initialize core services
        self.downloader = MultiSourceDownloader(self.config_manager)
        self.categorizer = MediaCategorizer(base_dir=self.download_dir)
        self.concurrent_service = ConcurrentService()
        
        # Telegram client setup
        self.telegram_client = None

    def _get_download_directory(self) -> str:
        """
        Safely retrieve and create download directory.
        
        Returns:
            Absolute path to download directory
        """
        # Get download directory from config
        download_dir = self.config_manager.get('paths', 'download_dir')
        
        # Fallback to default if not set
        if not download_dir:
            download_dir = os.path.expanduser('~/Downloads/MediaManager')
        
        # Expand and absolutize path
        download_dir = os.path.abspath(os.path.expanduser(download_dir))
        
        # Create directory if it doesn't exist
        os.makedirs(download_dir, exist_ok=True)
        
        return download_dir
    
    def _setup_telegram_client(self) -> Optional[TelegramClient]:
        """
        Set up Telegram client from configuration.
        
        Returns:
            Configured Telegram client or None
        """
        # Retrieve Telegram credentials
        api_id = self.config_manager.get('telegram', 'api_id')
        api_hash = self.config_manager.get('telegram', 'api_hash')
        bot_token = self.config_manager.get('telegram', 'bot_token')
        session_string = self.config_manager.get('telegram', 'session_string')
        
        # Log credential status for debugging
        self.logger.info("Telegram Credentials Check:")
        self.logger.info(f"API ID: {bool(api_id)}")
        self.logger.info(f"API Hash: {bool(api_hash)}")
        self.logger.info(f"Bot Token: {bool(bot_token)}")
        self.logger.info(f"Session String: {bool(session_string)}")
        
        # Validate credentials
        if not (api_id and api_hash):
            self.logger.warning("Incomplete Telegram API credentials")
            return None
        
        try:
            # Use session string if available, otherwise use default session
            if session_string:
                client = TelegramClient(
                    StringSession(session_string), 
                    api_id, 
                    api_hash
                )
            else:
                client = TelegramClient(
                    'media_manager_session', 
                    api_id, 
                    api_hash
                )
            
            return client
        except Exception as e:
            self.logger.error(f"Failed to create Telegram client: {e}")
            return None
    
    async def setup_file_watcher(self) -> FileWatcher:
        """
        Configure and start file system watcher.
        
        Returns:
            Configured file watcher
        """
        watcher = FileWatcher(self.download_dir)
        
        watcher.create_event_handler(
            on_created=self.process_new_file,
            extensions=['.mkv', '.mp4', '.avi']
        )
        
        watcher.start()
        return watcher
    
    async def process_new_file(self, filepath: str):
        """
        Process newly detected media file.
        
        Args:
            filepath: Path to new file
        """
        try:
            # Log the file detection
            self.logger.info(f"New file detected: {filepath}")

            # Categorize file
            categorized_path = self.categorizer.categorize(filepath)
            
            # Send notification
            await send_notification(
                f"New media file processed: {filepath}"
            )
            
            self.logger.info(f"Processed file: {categorized_path}")
        except Exception as e:
            self.logger.error(f"File processing error: {e}")
            await send_notification(
                f"Error processing file: {filepath}\n{str(e)}",
                channels=['telegram']
            )
    
    async def start(self):
        """
        Start the media manager application.
        """
        self.logger.info("Starting Media Manager Application")

        # Setup Telegram client
        self.telegram_client = self._setup_telegram_client()

        # Attempt to start Telegram client if created
        if self.telegram_client:
            try:
                await self.telegram_client.start(
                    bot_token=self.config_manager.get('telegram', 'bot_token')
                )

                # Get bot/user information
                me = await self.telegram_client.get_me()
                self.logger.info(f"Logged in as: {me.username or me.first_name}")

                # Comprehensive event handler
                @self.telegram_client.on(events.NewMessage(pattern=None))
                async def handle_all_messages(event):
                    try:
                        # Log all incoming messages
                        self.logger.info(f"Received message from {event.sender_id}:     {event.text}")

                        # Handle specific commands
                        if event.text:
                            if event.text.lower() == '/start':
                                await event.reply("Welcome to Media Manager Bot!")
                                self.logger.info("Responded to /start command")

                            elif event.text.lower() == '/help':
                                help_message = (
                                    "Available commands:\n"
                                    "/start - Start the bot\n"
                                    "/help - Show this help message\n"
                                    "/status - Check bot status\n"
                                    "/tests - Run system tests"
                                )
                                await event.reply(help_message)
                                self.logger.info("Responded to /help command")

                            elif event.text.lower() == '/status':
                                status_message = (
                                    "Bot is running!\n"
                                    f"Download Directory: {self.download_dir}\n"
                                    "Services: Active"
                                )
                                await event.reply(status_message)
                                self.logger.info("Responded to /status command")

                            elif event.text.lower() == '/tests':
                                # Comprehensive system tests
                                test_results = await self.run_system_tests()
                                await event.reply(test_results)
                                self.logger.info("Ran system tests")

                            elif event.text and event.media:
                                # Handle file downloads
                                await self.handle_media_download(event)

                            else:
                                # Optional: handle unknown commands
                                await event.reply("Unknown command. Type /help for  available commands.")

                    except Exception as e:
                        self.logger.error(f"Error handling message: {e}")
                        await event.reply(f"An error occurred: {e}")

                # Setup file watcher
                file_watcher = await self.setup_file_watcher()

                # Run client and watcher concurrently
                await asyncio.gather(
                    self.telegram_client.run_until_disconnected(),
                    file_watcher.run()
                )

            except Exception as e:
                self.logger.error(f"Error in Telegram client: {e}")
                import traceback
                traceback.print_exc()
            finally:
                if self.telegram_client:
                    await self.telegram_client.disconnect()

    async def run_system_tests(self) -> str:
        """
        Run comprehensive system tests.
        
        Returns:
            Test results as a string
        """
        tests = []
        
        # Test 1: Download Directory
        try:
            test_dir = self.download_dir
            tests.append("✅ Download Directory: Configured")
            assert os.path.exists(test_dir), "Download directory does not exist"
        except Exception as e:
            tests.append(f"❌ Download Directory: {e}")
        
        # Test 2: Downloader
        try:
            assert self.downloader is not None, "Downloader not initialized"
            tests.append("✅ Downloader: Initialized")
        except Exception as e:
            tests.append(f"❌ Downloader: {e}")
        
        # Test 3: Categorizer
        try:
            assert self.categorizer is not None, "Categorizer not initialized"
            tests.append("✅ Categorizer: Initialized")
        except Exception as e:
            tests.append(f"❌ Categorizer: {e}")
        
        # Test 4: Notification Service
        try:
            # Simulate a test notification
            await send_notification("System test notification")
            tests.append("✅ Notification Service: Working")
        except Exception as e:
            tests.append(f"❌ Notification Service: {e}")
        
        # Compile results
        return "System Tests Results:\n" + "\n".join(tests)

    async def handle_media_download(self, event):
        """
        Handle media downloads from Telegram with detailed progress tracking.

        Args:
            event: Telegram event with media
        """
        try:
            # Retrieve file attributes
            media = event.media
            file_size = media.document.size if hasattr(media, 'document') else 0

            # Validate file size
            if file_size == 0:
                await event.reply("❌ Unable to determine file size")
                return None

            # Initial notification
            status_message = await event.reply("🔄 Starting download...")

            # Logging for debugging
            self.logger.info(f"Starting download of file: {event.file.name}")
            self.logger.info(f"File size: {file_size} bytes")

            # Create progress tracker
            progress_tracker = ProgressTracker(
                total_size=file_size, 
                description=event.file.name or "Media Download"
            )

            # Track download progress with more detailed logging
            async def progress_callback(current, total):
                """
                Comprehensive progress tracking callback.

                Args:
                    current: Current downloaded bytes
                    total: Total file size
                """
                try:
                    # Update progress tracker
                    await progress_tracker.update(current)

                    # Periodic logging for debugging
                    if current % (total // 10) == 0:
                        progress_percent = (current / total) * 100
                        self.logger.info(
                            f"Download progress: {progress_percent:.1f}% "
                            f"({current}/{total} bytes)"
                        )

                    # Attempt to update status message periodically
                    try:
                        # Create progress bar
                        progress_bar = self._create_progress_bar(
                            (current / total) * 100
                        )

                        # Format sizes
                        current_size = progress_tracker.get_human_readable_size (current)
                        total_size = progress_tracker.get_human_readable_size(total)

                        # Calculate speed
                        elapsed_time = time.time() - progress_tracker.start_time
                        speed = (
                            progress_tracker.get_human_readable_size(
                                current / elapsed_time if elapsed_time > 0 else 0
                            ) + '/s'
                        )

                        # Estimate remaining time
                        remaining_time = progress_tracker.get_estimated_time_remaining  ()

                        # Construct detailed status message
                        status_text = (
                            f"⬇️ Downloading: {event.file.name}\n"
                            f"{progress_bar}\n"
                            f"📊 {current_size} / {total_size}\n"
                            f"🚀 Speed: {speed}\n"
                            f"⏳ Est. Time Remaining: {remaining_time}"
                        )

                        await status_message.edit(status_text)
                    except Exception as edit_error:
                        # Silently log editing errors to avoid interrupting download
                        self.logger.info(f"Status message update error: {edit_error}")

                except Exception as callback_error:
                    self.logger.error(f"Progress callback error: {callback_error}")

            # Determine correct download directory
            download_dirs = {
                "movies_dir": "/home/kelvitz/Videos/Jellyfin/Movies",
                "tv_shows_dir": "/home/kelvitz/Videos/Jellyfin/TV Shows",
                "unmatched_dir": "/home/kelvitz/Videos/Jellyfin/Unmatched"
            }

            # Download media to specified directory
            try:
                downloaded_file = await event.download_media(
                    file=download_dirs['unmatched_dir'],
                    progress_callback=progress_callback
                )

                # Log successful download
                self.logger.info(f"Download completed: {downloaded_file}")

            except Exception as download_error:
                # Comprehensive error logging
                self.logger.error(f"Download error: {download_error}")
                await status_message.edit(f"❌ Download failed: {download_error}")
                return None

            # Ensure download is marked as complete
            await progress_tracker.complete()

            # Update status to processing
            await status_message.edit("🔍 Download complete. Processing file...")

            # Categorize the downloaded file
            if downloaded_file:
                try:
                    categorized_path = self.categorizer.categorize(downloaded_file)

                    # Determine media type (TV Show or Movie)
                    media_type = "TV Show" if "/TV Shows/" in categorized_path else     "Movie"

                    # Get filename
                    filename = os.path.basename(categorized_path)

                    # Prepare detailed report
                    report = (
                        f"✅ Successfully processed {media_type}:\n"
                        f"📁 File: {filename}\n"
                        f"📦 Total Size: {progress_tracker.get_human_readable_size  (file_size)}\n"
                        f"⏱️ Time Taken: {progress_tracker.get_estimated_time_remaining  ()}\n"
                        f"📍 Location: {categorized_path}"
                    )

                    # Send detailed success message
                    await status_message.edit(report)

                    # Optional: Send notification through other channels
                    await send_notification(
                        f"New {media_type} downloaded: {filename}",
                        channels=['telegram']
                    )

                except Exception as categorize_error:
                    self.logger.error(f"Categorization error: {categorize_error}")
                    await status_message.edit(f"❌ Categorization failed:   {categorize_error}")

            self.logger.info(f"Media downloaded and processed: {downloaded_file}")

            return downloaded_file

        except Exception as e:
            self.logger.error(f"Media download error: {e}")
            await event.reply(f"❌ Download and processing failed: {e}")
            return None
    
    async def update_status_message(self, status_message, progress_tracker):
        """
        Update status message with progress details.
        
        Args:
            status_message: Message to update
            progress_tracker: ProgressTracker instance
        """
        try:
            # Create progress bar
            progress_percent = (
                progress_tracker.current_size / progress_tracker.total_size * 100
            )
            progress_bar = self._create_progress_bar(progress_percent)
            
            # Format sizes and speed
            current_size = progress_tracker.get_human_readable_size(
                progress_tracker.current_size
            )
            total_size = progress_tracker.get_human_readable_size(
                progress_tracker.total_size
            )
            
            # Calculate speed
            elapsed_time = time.time() - progress_tracker.start_time
            speed = (
                progress_tracker.get_human_readable_size(
                    progress_tracker.current_size / elapsed_time if elapsed_time > 0    else 0
                ) + '/s'
            )
            
            # Estimate remaining time
            remaining_time = progress_tracker.get_estimated_time_remaining()
            
            # Construct detailed status message
            status_text = (
                f"⬇️ Downloading: {progress_tracker.description}\n"
                f"{progress_bar}\n"
                f"📊 {current_size} / {total_size}\n"
                f"🚀 Speed: {speed}\n"
                f"⏳ Est. Time Remaining: {remaining_time}"
            )
            
            await status_message.edit(status_text)
        
        except Exception as e:
            self.logger.error(f"Status update error: {e}")
    
    def _create_progress_bar(
    self, 
    progress_percent: float, 
    width: int = 30, 
    filled_char: str = '█', 
    empty_char: str = '░'
    ) -> str:
        """
        Create a text-based progress bar.

        Args:
            progress_percent: Current progress percentage
            width: Width of the progress bar
            filled_char: Character for filled portion
            empty_char: Character for empty portion

        Returns:
            Formatted progress bar string
        """
        filled_length = int(width * progress_percent / 100)
        bar = filled_char * filled_length + empty_char * (width - filled_length)
        return f"[{bar}] {progress_percent:.1f}%"
    
    def _create_download_progress_callback(self, event):
        """
        Create a progress callback for file downloads.
        
        Args:
            event: Telegram event
        
        Returns:
            Progress callback function
        """
        async def progress(current, total):
            # Optional: Send periodic progress updates
            if current % (total // 10) == 0:
                progress_percent = (current / total) * 100
                await event.reply(f"Download progress: {progress_percent:.2f}%")
        
        return progress


def create_cli_parser() -> argparse.ArgumentParser:
    """
    Create CLI argument parser.
    
    Returns:
        Configured argument parser
    """
    parser = argparse.ArgumentParser(description="Media Manager Application")
    
    # Core commands
    parser.add_argument(
        'command', 
        choices=['start', 'config', 'download', 'watch'],
        help='Application command'
    )
    
    # Optional arguments
    parser.add_argument(
        '-c', '--config', 
        help='Custom configuration file path'
    )
    parser.add_argument(
        '-v', '--verbose', 
        action='store_true', 
        help='Enable verbose logging'
    )
    
    # Download specific arguments
    parser.add_argument(
        '--source', 
        help='Download source (telegram, http)'
    )
    parser.add_argument(
        '--url', 
        help='Media download URL'
    )
    
    return parser

def main():
    """
    Main entry point for Media Manager CLI.
    """
    # Parse CLI arguments
    parser = create_cli_parser()
    args = parser.parse_args()
    
    # Configure logging based on verbosity
    logger = get_logger('MediaManagerCLI')
    
    try:
        # Load configuration
        config_manager = ConfigurationManager(
            config_path=args.config if args.config else None
        )

        # Validate download directory
        download_dir = config_manager.get('paths', 'download_dir')
        if not download_dir:
            logger.warning("No download directory configured. Using default.")
            default_dir = os.path.expanduser('~/Downloads/MediaManager')
            config_manager.set('paths', 'download_dir', default_dir)
        
        # Execute command
        if args.command == 'start':
            app = MediaManagerApp(config_manager)
            asyncio.run(app.start())
        
        elif args.command == 'config':
            # Configuration management logic
            logger.info("Configuration management not implemented")
        
        elif args.command == 'download':
            # Download management logic
            if not args.source or not args.url:
                logger.error("Download requires --source and --url")
                sys.exit(1)
            
            # Placeholder for download logic
            logger.info(f"Downloading from {args.source}: {args.url}")
        
        elif args.command == 'watch':
            # File watching logic
            watcher = FileWatcher(
                config_manager.get('paths', 'download_dir')
            )
            watcher.start()
            asyncio.run(watcher.run())
    
    except Exception as e:
        logger.error(f"Application error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()