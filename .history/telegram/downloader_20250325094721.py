#telegram/downloader.py:


import os
import asyncio
import time
from typing import List, Optional, Callable, Dict, Any
from dataclasses import dataclass, field

import aiohttp
from telethon import TelegramClient
from telethon.tl.types import Document

from utils.progress_tracker import ProgressTracker
from config.configuration_manager import ConfigurationManager
from services.logging_service import get_logger
from file_processing.categorizer import MediaCategorizer

@dataclass
class DownloadConfig:
    """Configuration for media downloads."""
    chunk_size: int = 1024 * 1024  # 1 MB
    max_concurrent_downloads: int = 3
    download_timeout: int = 300  # 5 minutes
    retry_attempts: int = 3

class TelegramMediaDownloader:
    """
    Advanced Telegram media downloader with robust error handling.
    """
    def __init__(
        self, 
        config_manager: Optional[ConfigurationManager] = None,
        download_config: Optional[DownloadConfig] = None,
        categorizer: Optional[MediaCategorizer] = None
    ):
        """
        Initialize Telegram media downloader.
        
        Args:
            config_manager: Configuration manager
            download_config: Download configuration
            categorizer: Media categorizer
        """
        self.config_manager = config_manager
        self.download_config = download_config or DownloadConfig()
        self.categorizer = categorizer
        self.logger = get_logger('TelegramDownloader')
        
        # Semaphore for concurrent downloads
        self.download_semaphore = asyncio.Semaphore(
            self.download_config.max_concurrent_downloads
        )
    
    async def download_media(
        self, 
        client: TelegramClient,
        media: Document,
        destination_dir: str,
        progress_callback: Optional[Callable] = None
    ) -> Optional[str]:
        """
        Download media file from Telegram with advanced features.
        
        Args:
            client: Telegram client
            media: Media document
            destination_dir: Download destination
            progress_callback: Optional progress tracking callback
            
        Returns:
            Downloaded file path or None
        """
        async with self.download_semaphore:
            try:
                # Generate filename
                filename = self._generate_filename(media)
                filepath = os.path.join(destination_dir, filename)
                
                # Create progress tracker
                progress_tracker = ProgressTracker(
                    total_size=media.size,
                    description=filename,
                    update_callback=progress_callback
                )
                
                # Download media
                await client.download_media(
                    media, 
                    file=filepath,
                    progress_callback=self._create_progress_handler(progress_tracker)
                )
                
                # Categorize media if categorizer is available
                if self.categorizer:
                    filepath = str(self.categorizer.categorize(filepath))
                
                await progress_tracker.complete()
                return filepath
            
            except Exception as e:
                self.logger.error(f"Download failed: {e}")
                return None
    
    def _create_progress_handler(
        self, 
        progress_tracker: ProgressTracker
    ) -> Callable:
        """
        Create progress callback for download.
        
        Args:
            progress_tracker: Progress tracking instance
            
        Returns:
            Progress callback function
        """
        async def progress_handler(current, total):
            await progress_tracker.update(current - progress_tracker.current_size)
        
        return progress_handler
    
    def _generate_filename(self, media: Document) -> str:
        """
        Generate unique filename for downloaded media.
        
        Args:
            media: Telegram media document
            
        Returns:
            Generated filename
        """
        # Extract file attributes
        mime_type = media.mime_type or 'application/octet-stream'
        ext = {
            'video/mp4': '.mp4',
            'video/x-matroska': '.mkv',
            'video/avi': '.avi'
        }.get(mime_type, '.media')
        
        # Generate unique filename
        return f"{media.id}_{int(time.time())}{ext}"
    
    async def download_batch(
        self, 
        client: TelegramClient,
        media_list: List[Document],
        destination_dir: str
    ) -> List[str]:
        """
        Download multiple media files concurrently.
        
        Args:
            client: Telegram client
            media_list: List of media documents
            destination_dir: Download destination
            
        Returns:
            List of downloaded file paths
        """
        download_tasks = [
            self.download_media(client, media, destination_dir)
            for media in media_list
        ]
        
        return await asyncio.gather(*download_tasks)

class MultiSourceDownloader:
    """
    Centralized downloader supporting multiple sources.
    """
    def __init__(
        self, 
        config_manager: Optional[ConfigurationManager] = None
    ):
        """
        Initialize multi-source downloader.
        
        Args:
            config_manager: Configuration manager
        """
        self.telegram_downloader = TelegramMediaDownloader(config_manager)
        self.http_downloader = HTTPMediaDownloader(config_manager)
        
        # Extensible source downloaders can be added here
    
    async def download(
        self, 
        source: str, 
        media_info: Dict[str, Any],
        destination_dir: str
    ) -> Optional[str]:
        """
        Download media from various sources.
        
        Args:
            source: Download source type
            media_info: Media information
            destination_dir: Download destination
            
        Returns:
            Downloaded file path
        """
        downloaders = {
            'telegram': self.telegram_downloader.download_media,
            'http': self.http_downloader.download
        }
        
        downloader = downloaders.get(source.lower())
        if not downloader:
            raise ValueError(f"Unsupported download source: {source}")
        
        return await downloader(media_info, destination_dir)

class HTTPMediaDownloader:
    """
    HTTP/HTTPS media downloader with resumable downloads and advanced features.
    """
    def __init__(
        self, 
        config_manager: Optional[ConfigurationManager] = None,
        download_config: Optional[DownloadConfig] = None,
        categorizer: Optional[MediaCategorizer] = None
    ):
        """
        Initialize HTTP media downloader.
        
        Args:
            config_manager: Configuration manager
            download_config: Download configuration
            categorizer: Media categorizer
        """
        self.config_manager = config_manager or ConfigurationManager()
        self.download_config = download_config or DownloadConfig()
        self.categorizer = categorizer
        self.logger = get_logger('HTTPDownloader')
    
    def _generate_filename(self, url: str, headers: Dict[str, str]) -> str:
        """
        Generate a unique filename for the downloaded file.
        
        Args:
            url: Download URL
            headers: Response headers
            
        Returns:
            Generated filename
        """
        # Try to extract filename from Content-Disposition header
        content_disposition = headers.get('Content-Disposition', '')
        if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[-1].strip('"\'')
        else:
            # Fallback to generating filename from URL
            filename = os.path.basename(url.split('?')[0])
        
        # Ensure unique filename
        base, ext = os.path.splitext(filename)
        timestamp = int(time.time())
        return f"{base}_{timestamp}{ext or '.media'}"
    
    async def download(
        self, 
        url: str, 
        destination_dir: str,
        progress_callback: Optional[Callable] = None
    ) -> Optional[str]:
        """
        Download file from HTTP/HTTPS source with advanced features.
        
        Args:
            url: Media URL
            destination_dir: Download destination
            progress_callback: Optional progress tracking callback
            
        Returns:
            Downloaded file path
        """
        try:
            # Ensure destination directory exists
            os.makedirs(destination_dir, exist_ok=True)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=self.download_config.download_timeout) as response:
                    # Check for successful response
                    response.raise_for_status()
                    
                    # Get total file size
                    total_size = int(response.headers.get('Content-Length', 0))
                    
                    # Generate filename
                    filename = self._generate_filename(url, response.headers)
                    filepath = os.path.join(destination_dir, filename)
                    
                    # Create progress tracker
                    progress_tracker = ProgressTracker(
                        total_size=total_size,
                        description=filename,
                        update_callback=progress_callback
                    )
                    
                    # Download file with progress tracking
                    async with aiofiles.open(filepath, 'wb') as f:
                        downloaded = 0
                        async for chunk in response.content.iter_chunked(self.download_config.chunk_size):
                            await f.write(chunk)
                            downloaded += len(chunk)
                            await progress_tracker.update(len(chunk))
                    
                    # Categorize media if categorizer is available
                    if self.categorizer:
                        filepath = str(self.categorizer.categorize(filepath))
                    
                    # Mark download as complete
                    await progress_tracker.complete()
                    
                    return filepath
        
        except aiohttp.ClientError as e:
            self.logger.error(f"HTTP download failed: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected download error: {e}")
            return None
    
    async def download_batch(
        self, 
        urls: List[str],
        destination_dir: str
    ) -> List[Optional[str]]:
        """
        Download multiple files concurrently.
        
        Args:
            urls: List of download URLs
            destination_dir: Download destination
            
        Returns:
            List of downloaded file paths
        """
        download_tasks = [
            self.download(url, destination_dir)
            for url in urls
        ]
        
        return await asyncio.gather(*download_tasks)