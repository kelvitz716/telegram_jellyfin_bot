#utils/progress_tracker.py:

import time
import asyncio
from dataclasses import dataclass, field
from typing import Optional, Callable, Awaitable, Union

@dataclass
class ProgressTracker:
    """
    Comprehensive progress tracking for file operations.
    """
    total_size: int
    update_callback: Optional[Union[Callable, Awaitable[None]]] = None
    description: str = ''
    
    # Internal tracking
    current_size: int = 0
    start_time: float = field(default_factory=time.time)
    last_update_time: float = field(default_factory=time.time)
    
    # Configuration
    update_interval: float = 1.0  # seconds between updates
    
    async def update(self, chunk_size: int):
        """
        Update progress and trigger callback if needed.
        
        Args:
            chunk_size: Size of the processed chunk
        """
        self.current_size += chunk_size
        current_time = time.time()
        
        # Check if update interval has passed
        if current_time - self.last_update_time >= self.update_interval:
            await self._trigger_update()
            self.last_update_time = current_time
    
    async def _trigger_update(self):
        """
        Trigger progress update callback.
        """
        if self.update_callback is None:
            return
        
        # Calculate progress metrics
        elapsed_time = time.time() - self.start_time
        progress_percent = (self.current_size / self.total_size * 100) if self.total_size > 0 else 0
        speed = self.current_size / elapsed_time if elapsed_time > 0 else 0
        
        # Prepare update payload
        update_data = {
            'description': self.description,
            'current_size': self.current_size,
            'total_size': self.total_size,
            'progress_percent': progress_percent,
            'speed': speed,
            'elapsed_time': elapsed_time
        }
        
        # Handle different callback types
        if asyncio.iscoroutinefunction(self.update_callback):
            await self.update_callback(update_data)
        elif callable(self.update_callback):
            self.update_callback(update_data)
    
    async def complete(self):
        """
        Mark progress as complete and trigger final update.
        """
        # Ensure 100% progress
        self.current_size = self.total_size
        await self._trigger_update()
    
    def get_human_readable_size(self, size_bytes: int) -> str:
        """
        Convert bytes to human-readable format.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Formatted size string
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    def get_estimated_time_remaining(self) -> str:
        """
        Calculate estimated time remaining.
        
        Returns:
            Formatted time remaining
        """
        elapsed_time = time.time() - self.start_time
        speed = self.current_size / elapsed_time if elapsed_time > 0 else 0
        
        if speed == 0 or self.total_size == 0:
            return "Unknown"
        
        remaining_bytes = self.total_size - self.current_size
        remaining_seconds = remaining_bytes / speed
        
        return self._format_seconds(remaining_seconds)
    
    def _format_seconds(self, seconds: float) -> str:
        """
        Format seconds into human-readable time.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted time string
        """
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"

# Example usage demonstration
async def example_progress_callback(progress_data):
    """
    Example progress callback function.
    
    Args:
        progress_data: Dictionary containing progress information
    """
    print(
        f"Progress: {progress_data['progress_percent']:.2f}% | "
        f"Speed: {progress_data['speed'] / 1024 / 1024:.2f} MB/s | "
        f"Remaining: {progress_data['description']}"
    )

async def main():
    """
    Demonstrate ProgressTracker usage.
    """
    # Simulating a file download of 100MB
    total_size = 100 * 1024 * 1024
    tracker = ProgressTracker(
        total_size=total_size, 
        update_callback=example_progress_callback,
        description="Downloading file"
    )
    
    # Simulate download
    chunk_size = 1024 * 1024  # 1MB chunks
    for _ in range(total_size // chunk_size):
        await tracker.update(chunk_size)
        await asyncio.sleep(0.1)  # Simulate network delay
    
    await tracker.complete()

if __name__ == "__main__":
    asyncio.run(main())