#config/configuration.py:

import os
import json
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict, field

class ConfigurationError(Exception):
    """Base configuration exception."""
    pass

class ConfigMigrationError(ConfigurationError):
    """Configuration migration error."""
    pass

@dataclass
class TelegramConfig:
    api_id: str = None
    api_hash: str = None
    bot_token: str = None
    enabled: bool = False

@dataclass
class PathConfig:
    download_dir: str = os.path.expanduser('~/Downloads/MediaManager')
    movies_dir: str = os.path.expanduser('~/Videos/Movies')
    tv_shows_dir: str = os.path.expanduser('~/Videos/TV Shows')
    unmatched_dir: str = os.path.expanduser('~/Videos/Unmatched')

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
class APIConfig:
    tmdb_api_key: str = None
    omdb_api_key: str = None

@dataclass
class MediaManagerConfig:
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)
    apis: APIConfig = field(default_factory=APIConfig)
    version: int = 1

class ConfigurationManager:
    """
    Advanced configuration management with migration and validation support.
    """
    VERSION = 1
    DEFAULT_CONFIG_PATH = os.path.expanduser("~/.media_manager/config.json")

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Optional path to configuration file
        """
        self.logger = logging.getLogger('ConfigurationManager')
        self.config_path = config_path or self.DEFAULT_CONFIG_PATH
        self.config = self._load_or_create_config()

    def _load_or_create_config(self) -> MediaManagerConfig:
        """
        Load existing configuration or create default.
        
        Returns:
            Loaded or default configuration
        """
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)

        if not os.path.exists(self.config_path):
            config = MediaManagerConfig()
            self._save_config(config)
            return config

        try:
            with open(self.config_path, 'r') as f:
                data = json.load(f)
            return self._validate_and_migrate_config(data)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.error(f"Configuration load error: {e}")
            config = MediaManagerConfig()
            self._save_config(config)
            return config

    def _validate_and_migrate_config(self, data: Dict[str, Any]) -> MediaManagerConfig:
        """
        Validate and migrate configuration data.
        
        Args:
            data: Raw configuration dictionary
            
        Returns:
            Validated configuration object
        """
        try:
            current_version = data.get('version', 0)
            if current_version < self.VERSION:
                self.logger.info(f"Migrating config from v{current_version} to v{self.VERSION}")
                data = self._migrate_config(data, current_version)

            config = self._deserialize_config(data)
            self._validate_configuration(config)
            return config
        except Exception as e:
            self.logger.error(f"Configuration validation error: {e}")
            raise ConfigMigrationError(f"Failed to validate configuration: {e}")

    def _deserialize_config(self, data: Dict[str, Any]) -> MediaManagerConfig:
        """
        Deserialize configuration data, handling partial configurations.
        
        Args:
            data: Configuration dictionary
            
        Returns:
            Fully populated MediaManagerConfig
        """
        config = MediaManagerConfig()

        # Update each section if present
        if 'telegram' in data:
            config.telegram = TelegramConfig(**{
                **asdict(config.telegram),
                **data['telegram']
            })

        if 'paths' in data:
            config.paths = PathConfig(**{
                **asdict(config.paths),
                **data['paths']
            })

        if 'logging' in data:
            config.logging = LoggingConfig(**{
                **asdict(config.logging),
                **data['logging']
            })

        if 'download' in data:
            config.download = DownloadConfig(**{
                **asdict(config.download),
                **data['download']
            })

        if 'apis' in data:
            config.apis = APIConfig(**{
                **asdict(config.apis),
                **data['apis']
            })

        config.version = self.VERSION
        return config

    def _validate_configuration(self, config: MediaManagerConfig):
        """
        Validate configuration values.
        
        Args:
            config: Configuration object to validate
        """
        # Validate telegram configuration
        if config.telegram.enabled:
            if not all([
                config.telegram.api_id,
                config.telegram.api_hash,
                config.telegram.bot_token
            ]):
                self.logger.warning("Telegram configuration incomplete")

        # Validate paths
        for path in [
            config.paths.download_dir,
            config.paths.movies_dir,
            config.paths.tv_shows_dir,
            config.paths.unmatched_dir
        ]:
            if path:
                os.makedirs(path, exist_ok=True)
                self.logger.info(f"Ensured directory exists: {path}")

    def _migrate_config(self, data: Dict[str, Any], from_version: int) -> Dict[str, Any]:
        """
        Perform configuration data migrations.
        
        Args:
            data: Current configuration data
            from_version: Current configuration version
            
        Returns:
            Migrated configuration data
        """
        if from_version < 1:
            # Handle legacy configuration formats
            if 'download_directory' in data:
                data['paths'] = data.get('paths', {})
                data['paths']['download_dir'] = data.pop('download_directory')

        return data

    def _save_config(self, config: MediaManagerConfig):
        """
        Save configuration to file.
        
        Args:
            config: Configuration object to save
        """
        try:
            with open(self.config_path, 'w') as f:
                json.dump({
                    'version': config.version,
                    'telegram': asdict(config.telegram),
                    'paths': asdict(config.paths),
                    'logging': asdict(config.logging),
                    'download': asdict(config.download),
                    'apis': asdict(config.apis)
                }, f, indent=4)
        except IOError as e:
            self.logger.error(f"Failed to save configuration: {e}")
            raise ConfigurationError(f"Configuration save failed: {e}")

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
            'download': self.config.download,
            'apis': self.config.apis
        }

        try:
            section_config = config_map.get(section)
            return getattr(section_config, key, default)
        except AttributeError:
            return default

    def update(self, section: str, **kwargs):
        """
        Update configuration section.
        
        Args:
            section: Configuration section to update
            kwargs: Key-value pairs to update
        """
        try:
            config_map = {
                'telegram': self.config.telegram,
                'paths': self.config.paths,
                'logging': self.config.logging,
                'download': self.config.download,
                'apis': self.config.apis
            }

            if section not in config_map:
                raise ValueError(f"Invalid configuration section: {section}")

            section_config = config_map[section]
            for key, value in kwargs.items():
                if hasattr(section_config, key):
                    setattr(section_config, key, value)
                else:
                    raise ValueError(f"Invalid key for {section}: {key}")

            self._validate_configuration(self.config)
            self._save_config(self.config)

        except Exception as e:
            self.logger.error(f"Configuration update error: {e}")
            raise ConfigurationError(f"Failed to update configuration: {e}")