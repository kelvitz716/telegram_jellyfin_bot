# file_processing/watcher.py

import asyncio
import os
import time
import logging
import threading
from typing import List, Callable, Optional, Union, Awaitable
from pathlib import Path
from functools import partial
import queue

import watchdog.observers
import watchdog.events
from watchdog.events import FileSystemEventHandler

class AsyncFileSystemEventHandler(watchdog.events.FileSystemEventHandler):
    """
    Enhanced async-friendly file system event handler with robust processing.
    """
    def __init__(
        self, 
        on_created: Optional[Callable] = None,
        on_modified: Optional[Callable] = None,
        on_deleted: Optional[Callable] = None,
        extensions: Optional[List[str]] = None,
        event_loop=None
    ):
        """
        Initialize file system event handler.
        
        Args:
            on_created: Callback for file creation
            on_modified: Callback for file modification
            on_deleted: Callback for file deletion
            extensions: List of monitored file extensions
            event_loop: Asyncio event loop to use
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self._on_created_callback = on_created
        self._on_modified_callback = on_modified
        self._on_deleted_callback = on_deleted
        
        # Default video extensions if not provided
        self.extensions = extensions or ['.mkv', '.mp4', '.avi', '.mov', '.wmv']
        
        self.processed_files = set()
        self._event_queue = queue.Queue()
        self._running = False
        self._processor_thread = None
        self._event_loop = event_loop or asyncio.get_event_loop()
    
    def _is_valid_file(self, path: Union[str, Path]) -> bool:
        """
        Check if file matches monitored extensions.
        
        Args:
            path: File path
            
        Returns:
            True if file is valid
        """
        # Convert to string if Path object
        path_str = str(path)
        return any(path_str.lower().endswith(ext) for ext in self.extensions)

    def start_processing(self):
        """Start event processing thread"""
        self._running = True
        self._processor_thread = threading.Thread(target=self._process_events)
        self._processor_thread.daemon = True
        self._processor_thread.start()
        self.logger.info("Event processor thread started")
    
    def stop_processing(self):
        """Stop event processing thread"""
        self._running = False
        if self._processor_thread:
            self._processor_thread.join(timeout=2.0)
        self.logger.info("Event processor thread stopped")
        
    def _process_events(self):
        """Process events from queue in a dedicated thread"""
        while self._running:
            try:
                # Get item with timeout to allow thread termination
                event_type, filepath = self._event_queue.get(timeout=0.5)
                
                # Ensure filepath is a string
                filepath = str(filepath)
                
                # Validate file
                if not self._is_valid_file(filepath):
                    self._event_queue.task_done()
                    continue
                
                # Prevent duplicate processing
                if filepath in self.processed_files:
                    self._event_queue.task_done()
                    continue
                
                # Add to processed files
                self.processed_files.add(filepath)
                
                # Wait briefly to ensure file is fully written
                time.sleep(0.5)
                
                # Determine callback based on event type
                callback = None
                if event_type == 'created' and self._on_created_callback:
                    callback = self._on_created_callback
                elif event_type == 'modified' and self._on_modified_callback:
                    callback = self._on_modified_callback
                elif event_type == 'deleted' and self._on_deleted_callback:
                    callback = self._on_deleted_callback
                
                # Run the callback
                if callback:
                    try:
                        # Use run_coroutine_threadsafe for async callbacks
                        if asyncio.iscoroutinefunction(callback):
                            future = asyncio.run_coroutine_threadsafe(
                                callback(filepath), 
                                self._event_loop
                            )
                            future.result(timeout=30)  # Wait for completion with timeout
                        else:
                            # Synchronous callback
                            callback(filepath)
                    except Exception as e:
                        self.logger.error(f"Error processing {filepath}: {e}")
                
                # Remove from processed files
                self.processed_files.discard(filepath)
                self._event_queue.task_done()
                
            except queue.Empty:
                # Timeout, just continue
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error in event processing: {e}")
    
    def on_created(self, event):
        """Handle file creation events."""
        # Safely get filepath and check if it's a file
        try:
            if not event.is_directory:
                self._event_queue.put(('created', event.src_path))
        except Exception as e:
            self.logger.error(f"Error in on_created: {e}")
    
    def on_modified(self, event):
        """Handle file modification events."""
        try:
            if not event.is_directory:
                self._event_queue.put(('modified', event.src_path))
        except Exception as e:
            self.logger.error(f"Error in on_modified: {e}")
    
    def on_deleted(self, event):
        """Handle file deletion events."""
        try:
            if not event.is_directory:
                self._event_queue.put(('deleted', event.src_path))
        except Exception as e:
            self.logger.error(f"Error in on_deleted: {e}")

class FileWatcher:
    """
    Advanced async file system watcher with multiple monitoring strategies.
    """
    def __init__(
        self, 
        paths: Union[str, List[str]],
        recursive: bool = True,
        debounce_interval: float = 1.0,
        event_loop = None
    ):
        """
        Initialize file watcher.
        
        Args:
            paths: Directories to monitor
            recursive: Monitor subdirectories
            debounce_interval: Minimum time between event processing
            event_loop: Asyncio event loop to use
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.paths = [paths] if isinstance(paths, str) else paths
        self.recursive = recursive
        self.debounce_interval = debounce_interval
        
        self._observers = []
        self._event_handler = None
        self._last_event_time = 0
        self._event_loop = event_loop or asyncio.get_event_loop()
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def create_event_handler(
        self, 
        on_created: Optional[Callable] = None,
        on_modified: Optional[Callable] = None,
        on_deleted: Optional[Callable] = None,
        extensions: Optional[List[str]] = None
    ) -> AsyncFileSystemEventHandler:
        """
        Create a configured event handler.
        
        Args:
            on_created: Callback for file creation
            on_modified: Callback for file modification
            on_deleted: Callback for file deletion
            extensions: List of monitored file extensions
            
        Returns:
            Configured event handler
        """
        self._event_handler = AsyncFileSystemEventHandler(
            on_created=on_created,
            on_modified=on_modified,
            on_deleted=on_deleted,
            extensions=extensions,
            event_loop=self._event_loop
        )
        return self._event_handler
    
    def start(self):
        """Start file system monitoring."""
        import watchdog.observers
        
        # Start the event handler's processing thread
        if self._event_handler:
            self._event_handler.start_processing()
        
        for path in self.paths:
            # Validate path exists
            if not os.path.exists(path):
                self.logger.warning(f"Monitoring path does not exist: {path}")
                continue
            
            observer = watchdog.observers.Observer()
            observer.schedule(
                self._event_handler, 
                path, 
                recursive=self.recursive
            )
            observer.start()
            self._observers.append(observer)
        
        self.logger.info(f"Started monitoring {len(self._observers)} paths")
    
    def stop(self):
        """Stop file system monitoring."""
        # Stop the event handler's processing thread
        if self._event_handler:
            self._event_handler.stop_processing()
            
        for observer in self._observers:
            observer.stop()
            observer.join()
        self._observers.clear()
        self.logger.info("Stopped file system monitoring")
    
    async def run(
        self, 
        on_file_detected: Optional[Callable] = None,
        poll_interval: float = 0.5
    ):
        """
        Run monitoring with custom file detection logic.
        
        Args:
            on_file_detected: Custom file detection callback
            poll_interval: Polling interval
        """
        try:
            self.logger.info("Starting file watcher run loop")
            while True:
                await asyncio.sleep(poll_interval)
                
                # Custom file detection logic
                if on_file_detected:
                    current_time = time.time()
                    if current_time - self._last_event_time >= self.debounce_interval:
                        try:
                            await on_file_detected()
                            self._last_event_time = current_time
                        except Exception as e:
                            self.logger.error(f"Error in file detection callback: {e}")
        except asyncio.CancelledError:
            self.logger.info("File watcher run loop cancelled")
            self.stop()
        except Exception as e:
            self.logger.error(f"Unexpected error in file watcher: {e}")
            self.stop()

# Diagnostic function for testing
async def diagnostic_file_handler(filepath: str):
    """
    Diagnostic file detection handler with comprehensive logging.
    
    Args:
        filepath: Path of detected file
    """
    logger = logging.getLogger('diagnostic_file_handler')
    try:
        logger.info(f"Detected new file: {filepath}")
        
        # Additional file info
        file_stats = os.stat(filepath)
        logger.info(f"File size: {file_stats.st_size} bytes")
        logger.info(f"File modified: {time.ctime(file_stats.st_mtime)}")
    except Exception as e:
        logger.error(f"Error processing file {filepath}: {e}")

# Example usage
async def main():
    """Demonstration of file watcher."""
    download_path = "/path/to/downloads"
    
    watcher = FileWatcher(download_path)
    watcher.create_event_handler(
        on_created=diagnostic_file_handler,
        extensions=['.mkv', '.mp4']
    )
    
    watcher.start()
    
    try:
        await watcher.run()
    except KeyboardInterrupt:
        watcher.stop()

if __name__ == "__main__":
    asyncio.run(main())