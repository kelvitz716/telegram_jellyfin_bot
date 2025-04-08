#services/logging_service.py:

import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, List, Union

class LoggingService:
    """
    Centralized logging service with flexible configuration.
    """
    _instance = None
    
    def __new__(cls, config=None):
        """
        Singleton implementation to ensure consistent logging across application.
        
        Args:
            config: Configuration dictionary
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, 
                 config: Optional[dict] = None, 
                 log_file: Optional[str] = None, 
                 log_level: Union[str, int] = logging.DEBUG):
        """
        Initialize logging service.
        
        Args:
            config: Configuration dictionary
            log_file: Custom log file path
            log_level: Logging level
        """
        # Prevent re-initialization
        if hasattr(self, '_initialized'):
            return
        
        # Default configuration
        self.config = config or {}
        self.log_file = log_file or self.config.get('log_file', 'media_manager.log')
        self.log_level = self._parse_log_level(log_level)
        
        # Ensure log directory exists
        self._prepare_log_directory()
        
        # Configure logging
        self._setup_logging()
        
        # Mark as initialized
        self._initialized = True
    
    def _parse_log_level(self, level: Union[str, int]) -> int:
        """
        Convert log level to integer representation.
        
        Args:
            level: Log level as string or integer
            
        Returns:
            Integer log level
        """
        if isinstance(level, int):
            return level
        
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        
        return level_map.get(level.upper(), logging.INFO)
    
    def _prepare_log_directory(self):
        """
        Ensure log directory exists.
        """
        log_dir = os.path.dirname(os.path.abspath(self.log_file))
        os.makedirs(log_dir, exist_ok=True)
    
    def _setup_logging(self):
        """
        Configure logging with rotating file handler and console output.
        """
        # Root logger configuration
        logging.basicConfig(
            level=self.log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Create root logger
        root_logger = logging.getLogger()
        
        # Clear any existing handlers to prevent duplicate logging
        root_logger.handlers.clear()
        
        # Rotating File Handler
        file_handler = RotatingFileHandler(
            self.log_file,
            maxBytes=self.config.get('max_file_size', 10 * 1024 * 1024),  # 10 MB default
            backupCount=self.config.get('backup_count', 5)
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        file_handler.setLevel(self.log_level)
        
        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(name)s - %(levelname)s - %(message)s'
        ))
        console_handler.setLevel(self.log_level)
        
        # Add handlers to root logger
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
    
    def get_logger(self, name: str = 'MediaManager') -> logging.Logger:
        """
        Get a named logger.
        
        Args:
            name: Logger name
            
        Returns:
            Configured logger
        """
        return logging.getLogger(name)
    
    def set_log_level(self, level: Union[str, int]):
        """
        Dynamically change log level.
        
        Args:
            level: New log level
        """
        new_level = self._parse_log_level(level)
        logging.getLogger().setLevel(new_level)
        
        for handler in logging.getLogger().handlers:
            handler.setLevel(new_level)


# Utility function for easy logger access
def get_logger(name: str = 'MediaManager') -> logging.Logger:
    """
    Quick access to logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger
    """
    return LoggingService().get_logger(name)