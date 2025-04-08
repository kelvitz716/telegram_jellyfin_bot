# config/configurationmanager.py
import os
import json
from typing import Dict, Any, Optional

class ConfigurationManager:
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file
        """
        # Default config path
        self.default_config_path = os.path.join(
            os.path.dirname(__file__), 
            'config.json'
        )
        
        # Use provided path or default
        self.config_path = config_path or self.default_config_path
        
        # Load configuration
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from JSON file.
        
        Returns:
            Configuration dictionary
        """
        # Default empty configuration
        default_config = {
            'telegram': {
                'api_id': None,
                'api_hash': None,
                'bot_token': None
            },
            'paths': {
                'download_dir': os.path.expanduser('~/Downloads')
            }
        }
        
        # Check if config file exists
        if not os.path.exists(self.config_path):
            self._save_config(default_config)
            return default_config
        
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                # Merge default config with loaded config
                return {**default_config, **config}
        except json.JSONDecodeError:
            print(f"Error decoding {self.config_path}. Using default configuration.")
            return default_config
    
    def _save_config(self, config: Dict[str, Any]):
        """
        Save configuration to JSON file.
        
        Args:
            config: Configuration dictionary
        """
        with open(self.config_path, 'w') as f:
            json.dump(config, f, indent=4)
    
    def get(self, *keys, default: Any = None) -> Any:
        """
        Get nested configuration value.
        
        Args:
            *keys: Nested keys
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        current = self.config
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key, {})
            else:
                return default
        
        return current if current else default
    
    def set(self, *keys, value: Any):
        """
        Set nested configuration value.
        
        Args:
            *keys: Nested keys
            value: Configuration value
        """
        current = self.config
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
        self._save_config(self.config)
    
    def update(self, updates: Dict[str, Any]):
        """
        Update multiple configuration values.
        
        Args:
            updates: Dictionary of configuration updates
        """
        def deep_update(original, update):
            for key, value in update.items():
                if isinstance(value, dict):
                    original[key] = deep_update(original.get(key, {}), value)
                else:
                    original[key] = value
            return original
        
        self.config = deep_update(self.config, updates)
        self._save_config(self.config)