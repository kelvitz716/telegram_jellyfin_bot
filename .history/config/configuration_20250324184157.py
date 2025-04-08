

import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict, field

@dataclass
class TelegramConfig:
    api_id: str = "your_api_id"
    api_hash: str = "your_api_hash"
    bot_token: str = "your_bot_token"
    enabled: bool = True

@dataclass
class PathConfig:
    download_dir: str = "downloads"
    movies_dir: str = "movies"
    tv_shows_dir: str = "tv_shows"
    unmatched_dir: str = "unmatched"

@dataclass
class LoggingConfig:
    level: str = "INFO"
    max_size_mb: int = 10
    backup_count: int = 5
    log_file: str = "media_manager.log"

@dataclass
class DownloadConfig:
    chunk_size: int = 1024 * 1024  # 1MB
    max_concurrent_downloads: int = 3
    progress_update_interval: float = 5.0
    max_retries: int = 3

@dataclass
class MediaManagerConfig:
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)

class ConfigurationManager:
    """
    Manages application configuration with robust loading and validation.
    """
    DEFAULT_CONFIG_PATH = os.path.expanduser("~/.media_manager/config.json")

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Optional path to configuration file
        """
        self.config_path = config_path or self.DEFAULT_CONFIG_PATH
        self.config = self._load_or_create_config()

    def _load_or_create_config(self) -> MediaManagerConfig:
        """
        Load existing configuration or create a default one.
        
        Returns:
            Validated configuration object
        """
        # Ensure config directory exists
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)

        # If config file doesn't exist, create it
        if not os.path.exists(self.config_path):
            config = MediaManagerConfig()
            self._save_config(config)
            return config

        # Load existing configuration
        try:
            with open(self.config_path, 'r') as f:
                data = json.load(f)
                return self._deserialize_config(data)
        except (json.JSONDecodeError, TypeError):
            print(f"Invalid configuration at {self.config_path}. Using default.")
            config = MediaManagerConfig()
            self._save_config(config)
            return config

    def _deserialize_config(self, data: Dict[str, Any]) -> MediaManagerConfig:
        """
        Deserialize configuration data, handling partial configurations.
        
        Args:
            data: Configuration dictionary
            
        Returns:
            Fully populated MediaManagerConfig
        """
        # Create a default config to use as base
        config = MediaManagerConfig()

        # Update Telegram config
        if 'telegram' in data:
            config.telegram = TelegramConfig(**{
                **asdict(config.telegram),
                **data['telegram']
            })

        # Update paths config
        if 'paths' in data:
            config.paths = PathConfig(**{
                **asdict(config.paths),
                **data['paths']
            })

        # Update logging config
        if 'logging' in data:
            config.logging = LoggingConfig(**{
                **asdict(config.logging),
                **data['logging']
            })

        # Update download config
        if 'download' in data:
            config.download = DownloadConfig(**{
                **asdict(config.download),
                **data['download']
            })

        return config

    def _save_config(self, config: MediaManagerConfig) -> None:
        """
        Save configuration to file.
        
        Args:
            config: Configuration object to save
        """
        try:
            with open(self.config_path, 'w') as f:
                json.dump(
                    {
                        'telegram': asdict(config.telegram),
                        'paths': asdict(config.paths),
                        'logging': asdict(config.logging),
                        'download': asdict(config.download)
                    }, 
                    f, 
                    indent=4
                )
        except IOError as e:
            print(f"Error saving configuration: {e}")

    def get(self, section: str, key: str, default: Any = None) -> Any:
        """
        Retrieve a specific configuration value.
        
        Args:
            section: Configuration section name
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        config_map = {
            'telegram': self.config.telegram,
            'paths': self.config.paths,
            'logging': self.config.logging,
            'download': self.config.download
        }

        try:
            section_config = config_map.get(section)
            return getattr(section_config, key, default)
        except AttributeError:
            return default

    def update(self, section: str, **kwargs) -> None:
        """
        Update configuration section.
        
        Args:
            section: Configuration section to update
            kwargs: Key-value pairs to update
        """
        config_map = {
            'telegram': self.config.telegram,
            'paths': self.config.paths,
            'logging': self.config.logging,
            'download': self.config.download
        }

        if section not in config_map:
            raise ValueError(f"Invalid configuration section: {section}")

        section_config = config_map[section]
        for key, value in kwargs.items():
            if hasattr(section_config, key):
                setattr(section_config, key, value)
            else:
                raise ValueError(f"Invalid key for {section}: {key}")

        # Save updated configuration
        self._save_config(self.config)