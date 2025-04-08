#services/concurrent_service.py:


import asyncio
from typing import List, Callable, Any, Optional, Coroutine
from concurrent.futures import ThreadPoolExecutor

class ConcurrentService:
    """
    Advanced concurrent processing service.
    """
    def __init__(
        self, 
        max_workers: Optional[int] = None
    ):
        """
        Initialize concurrent service.
        
        Args:
            max_workers: Maximum number of worker threads
        """
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.event_loop = asyncio.get_event_loop()
    
    async def run_async(
        self, 
        func: Callable[..., Any], 
        *args, 
        **kwargs
    ) -> Any:
        """
        Run synchronous function asynchronously.
        
        Args:
            func: Function to run
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Function result
        """
        return await self.event_loop.run_in_executor(
            self.thread_pool, 
            func, 
            *args, 
            **kwargs
        )
    
    async def run_concurrent(
        self, 
        coroutines: List[Coroutine],
        max_concurrent: Optional[int] = None
    ) -> List[Any]:
        """
        Run multiple coroutines concurrently with optional limit.
        
        Args:
            coroutines: List of coroutines
            max_concurrent: Maximum concurrent executions
            
        Returns:
            List of results
        """
        if max_concurrent:
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def limited_coro(coro):
                async with semaphore:
                    return await coro
            
            return await asyncio.gather(
                *[limited_coro(coro) for coro in coroutines]
            )
        
        return await asyncio.gather(*coroutines)
    
    async def process_in_chunks(
        self, 
        items: List[Any], 
        processor: Callable,
        chunk_size: int = 10
    ) -> List[Any]:
        """
        Process items in chunks with concurrent execution.
        
        Args:
            items: List of items to process
            processor: Processing function
            chunk_size: Number of items per chunk
            
        Returns:
            Processed results
        """
        results = []
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i+chunk_size]
            chunk_coroutines = [processor(item) for item in chunk]
            chunk_results = await self.run_concurrent(chunk_coroutines)
            results.extend(chunk_results)
        
        return results