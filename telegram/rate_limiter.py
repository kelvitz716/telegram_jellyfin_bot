#telegram/rate_limiter.py:

import asyncio
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

@dataclass
class RateLimiterConfig:
    """Configuration for rate limiting."""
    default_limit: float = 1.0  # requests per second
    burst_limit: int = 3  # max burst requests
    max_wait: float = 10.0  # maximum wait time

class RateLimiter:
    """
    Advanced async rate limiter with configurable limits and burst support.
    """
    def __init__(self, config: Optional[RateLimiterConfig] = None):
        """
        Initialize rate limiter.
        
        Args:
            config: Rate limiter configuration
        """
        self.config = config or RateLimiterConfig()
        self._limits: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    async def acquire(self, key: str = 'default') -> bool:
        """
        Acquire a rate limit token.
        
        Args:
            key: Unique identifier for rate limit group
            
        Returns:
            True if token acquired, False if rate limit exceeded
        """
        async with self._lock:
            now = time.time()
            
            # Initialize limit group if not exists
            if key not in self._limits:
                self._limits[key] = {
                    'tokens': self.config.burst_limit,
                    'last_refill': now
                }
            
            limit_group = self._limits[key]
            
            # Refill tokens based on time elapsed
            time_since_last_refill = now - limit_group['last_refill']
            tokens_to_add = time_since_last_refill * self.config.default_limit
            limit_group['tokens'] = min(
                self.config.burst_limit, 
                limit_group['tokens'] + tokens_to_add
            )
            limit_group['last_refill'] = now
            
            # Check if token can be acquired
            if limit_group['tokens'] >= 1:
                limit_group['tokens'] -= 1
                return True
            
            return False
    
    async def wait(self, key: str = 'default', timeout: Optional[float] = None) -> bool:
        """
        Wait for a rate limit token to become available.
        
        Args:
            key: Unique identifier for rate limit group
            timeout: Maximum wait time
            
        Returns:
            True if token acquired, False if timeout reached
        """
        timeout = timeout or self.config.max_wait
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if await self.acquire(key):
                return True
            
            # Exponential backoff
            await asyncio.sleep(0.1)
        
        return False

class AsyncRateLimiter:
    """
    Simplified async rate limiter with interval-based limiting.
    """
    def __init__(self, interval: float = 1.0):
        """
        Initialize with a minimum interval between calls.
        
        Args:
            interval: Minimum seconds between calls
        """
        self._interval = interval
        self._last_call: Dict[str, float] = {}
        self._lock = asyncio.Lock()
    
    async def call(self, func, *args, key: str = 'default', **kwargs):
        """
        Execute a function with rate limiting.
        
        Args:
            func: Function to call
            *args: Positional arguments
            key: Unique identifier for rate limit group
            **kwargs: Keyword arguments
            
        Returns:
            Function result
        """
        async with self._lock:
            now = time.time()
            last_call = self._last_call.get(key, 0)
            
            # Wait if needed
            wait_time = max(0, self._interval - (now - last_call))
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            # Update last call time
            self._last_call[key] = time.time()
            
            # Execute function
            return await func(*args, **kwargs)

# Convenience factory functions
def create_telegram_rate_limiter() -> RateLimiter:
    """Create a rate limiter configured for Telegram API."""
    return RateLimiter(RateLimiterConfig(
        default_limit=0.5,  # 2 requests per second
        burst_limit=3,
        max_wait=30.0
    ))

def create_api_rate_limiter() -> RateLimiter:
    """Create a rate limiter configured for general API calls."""
    return RateLimiter(RateLimiterConfig(
        default_limit=1.0,  # 1 request per second
        burst_limit=5,
        max_wait=10.0
    ))