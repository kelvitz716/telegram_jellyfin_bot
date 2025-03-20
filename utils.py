import time
import threading

class RateLimiter:
    def __init__(self, min_interval: float = 2.0):
        """Initialize rate limiter with minimum interval between updates."""
        self.last_update = {}
        self.min_interval = min_interval
        self.lock = threading.Lock()
        
    def can_update(self, identifier):
        """Check if update is allowed for given identifier."""
        with self.lock:
            now = time.time()
            if identifier not in self.last_update:
                self.last_update[identifier] = now
                return True
            
            if now - self.last_update[identifier] >= self.min_interval:
                self.last_update[identifier] = now
                return True
            return False
            
    def wait_if_needed(self, identifier):
        """Wait until update is allowed."""
        while not self.can_update(identifier):
            time.sleep(0.1)

# Global rate limiter instances
telegram_message_limiter = RateLimiter(min_interval=2.0)  # 2 seconds between messages
api_rate_limiter = RateLimiter(min_interval=0.25)  # 250ms between API calls