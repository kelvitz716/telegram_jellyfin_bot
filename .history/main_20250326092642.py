# main.py
import asyncio
import os
import sys
import argparse
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
        self.telegram_client = None
    
    def _get_download_directory(self) -> str:
        """
        Safely retrieve and create download directory.
        
        Returns:
            Absolute path to download directory
        """
        # Get download directory from config
        download_dir = self.config_manager.get('paths', 'download_dir')
        
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
        self.logger.debug("Telegram Credentials Check:")
        self.logger.debug(f"API ID: {bool(api_id)}")
        self.logger.debug(f"API Hash: {bool(api_hash)}")
        self.logger.debug(f"Bot Token: {bool(bot_token)}")
        self.logger.debug(f"Session String: {bool(session_string)}")
        
        # Validate credentials
        if not (api_id and api_hash):
            self.logger.error("Incomplete Telegram API credentials")
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
    
    async def start(self):
        """
        Start the media manager application.
        """
        self.logger.info("Starting Media Manager Application")
        
        # Setup Telegram client
        self.telegram_client = self._setup_telegram_client()
        if not self.telegram_client:
            self.logger.error("Failed to setup Telegram client")
            # Continue with file watcher even without Telegram client
        else:
            try:
                # Start the client
                await self.telegram_client.start(
                    bot_token=self.config_manager.get('telegram', 'bot_token')
                )
                
                # Get bot information
                me = await self.telegram_client.get_me()
                self.logger.info(f"Logged in as: {me.username or me.first_name}")
                
                # Add basic event handler
                @self.telegram_client.on(events.NewMessage(pattern='/start'))
                async def start_handler(event):
                    await event.reply("Welcome to Media Manager Bot!")
            
            except Exception as e:
                self.logger.error(f"Error starting Telegram client: {e}")
        
        # Setup file watcher
        file_watcher = await self.setup_file_watcher()
        
        try:
            # Run indefinitely
            await file_watcher.run()
        except KeyboardInterrupt:
            self.logger.info("Application shutting down")
        finally:
            file_watcher.stop()
            
            # Disconnect Telegram client if it exists
            if self.telegram_client:
                await self.telegram_client.disconnect()

# Rest of the file remains the same