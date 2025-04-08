#file_processing/watcher.py:


import asyncio
import os
import time
from typing import List, Callable, Optional, Awaitable, Union
from pathlib import Path

import watchdog.observers
import watchdog.events
from watchdog.events import FileSystemEventHandler

class AsyncFileSystemEventHandler(FileSystemEventHandler):
    """
    Enhanced async-friendly file system event handler.
    Supports multiple event types and custom async callbacks.
    """
    def __init__(
        self, 
        on_created: Optional[Union[Callable, Awaitable[None]]] = None,
        on_modified: Optional[Union[Callable, Awaitable[None]]] = None,
        on_deleted: Optional[Union[Callable, Awaitable[None]]] = None,
        extensions: Optional[List[str]] = None
    ):
        """
        Initialize file system event handler.
        
        Args:
            on_created: Callback for file creation
            on_modified: Callback for file modification
            on_deleted: Callback for file deletion
            extensions: List of monitored file extensions
        """
        self.on_created = on_created
        self.on_modified = on_modified
        self.on_deleted = on_deleted
        self.extensions = extensions or ['.mkv', '.mp4', '.avi', '.mov']
    
    def _is_valid_file(self, path: str) -> bool:
        """
        Check if file matches monitored extensions.
        
        Args:
            path: File path
            
        Returns:
            True if file is valid
        """
        return any(path.lower().endswith(ext) for ext in self.extensions)
    
    async def _handle_event(
        self, 
        event_type: str, 
        event: watchdog.events.FileSystemEvent
    ):
        """
        Handle async event processing.
        
        Args:
            event_type: Type of event
            event: File system event
        """
        if not self._is_valid_file(event.src_path):
            return
        
        handler = getattr(self, f'on_{event_type}', None)
        if handler:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event.src_path)
                else:
                    handler(event.src_path)
            except Exception as e:
                print(f"Error processing {event_type} event: {e}")
    
    def on_created(self, event):
        """Sync wrapper for file creation event."""
        if not event.is_directory:
            asyncio.create_task(self._handle_event('created', event))
    
    def on_modified(self, event):
        """Sync wrapper for file modification event."""
        if not event.is_directory:
            asyncio.create_task(self._handle_event('modified', event))
    
    def on_deleted(self, event):
        """Sync wrapper for file deletion event."""
        if not event.is_directory:
            asyncio.create_task(self._handle_event('deleted', event))

class FileWatcher:
    """
    Advanced async file system watcher with multiple monitoring strategies.
    """
    def __init__(
        self, 
        paths: Union[str, List[str]],
        recursive: bool = True,
        debounce_interval: float = 1.0
    ):
        """
        Initialize file watcher.
        
        Args:
            paths: Directories to monitor
            recursive: Monitor subdirectories
            debounce_interval: Minimum time between event processing
        """
        self.paths = [paths] if isinstance(paths, str) else paths
        self.recursive = recursive
        self.debounce_interval = debounce_interval
        
        self._observers = []
        self._event_handler = None
        self._last_event_time = 0
    
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
            extensions=extensions
        )
        return self._event_handler
    
    def start(self):
        """Start file system monitoring."""
        import watchdog.observers
        
        for path in self.paths:
            observer = watchdog.observers.Observer()
            observer.schedule(
                self._event_handler, 
                path, 
                recursive=self.recursive
            )
            observer.start()
            self._observers.append(observer)
    
    def stop(self):
        """Stop file system monitoring."""
        for observer in self._observers:
            observer.stop()
            observer.join()
        self._observers.clear()
    
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
            while True:
                await asyncio.sleep(poll_interval)
                
                # Custom file detection logic
                if on_file_detected:
                    current_time = time.time()
                    if current_time - self._last_event_time >= self.debounce_interval:
                        await on_file_detected()
                        self._last_event_time = current_time
        except asyncio.CancelledError:
            self.stop()

# Example usage
async def example_file_handler(filepath: str):
    """
    Example file detection handler.
    
    Args:
        filepath: Path of detected file
    """
    print(f"Detected new file: {filepath}")

async def main():
    """Demonstration of file watcher."""
    download_path = "/path/to/downloads"
    
    watcher = FileWatcher(download_path)
    watcher.create_event_handler(
        on_created=example_file_handler,
        extensions=['.mkv', '.mp4']
    )
    
    watcher.start()
    
    try:
        await watcher.run()
    except KeyboardInterrupt:
        watcher.stop()

if __name__ == "__main__":
    asyncio.run(main())