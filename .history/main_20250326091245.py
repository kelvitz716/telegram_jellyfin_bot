import asyncio
import sys
import argparse
from typing import Optional

from telethon import TelegramClient

from config.configuration_manager import ConfigurationManager
from services.logging_service import get_logger
from telegram.downloader import MultiSourceDownloader, TelegramMediaDownloader
from file_processing.watcher import FileWatcher
from file_processing.categorizer import MediaCategorizer
from services.notification_service import send_notification
from services.concurrent_service import ConcurrentService

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
        download_dir = self._get_download_directory()
        
        # Initialize core services
        self.downloader = MultiSourceDownloader(self.config_manager)
        self.categorizer = MediaCategorizer(base_dir=download_dir)
        self.concurrent_service = ConcurrentService()
        
        # Telegram client setup
        self.telegram_client = self._setup_telegram_client()

    
    
    def _setup_telegram_client(self) -> Optional[TelegramClient]:
        """
        Set up Telegram client from configuration.
        
        Returns:
            Configured Telegram client or None
        """
        api_id = self.config_manager.get('telegram', 'api_id')
        api_hash = self.config_manager.get('telegram', 'api_hash')
        
        if not (api_id and api_hash):
            self.logger.warning("Telegram API credentials not configured")
            return None
        
        return TelegramClient(
            'media_manager_session', 
            api_id, 
            api_hash
        )
    
    async def setup_file_watcher(self) -> FileWatcher:
        """
        Configure and start file system watcher.
        
        Returns:
            Configured file watcher
        """
        download_dir = self.config_manager.get('paths', 'download_dir')
        watcher = FileWatcher(download_dir)
        
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
        
        # Setup file watcher
        file_watcher = await self.setup_file_watcher()
        
        try:
            # Run indefinitely
            await file_watcher.run()
        except KeyboardInterrupt:
            self.logger.info("Application shutting down")
        finally:
            file_watcher.stop()

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
        sys.exit(1)

if __name__ == "__main__":
    main()