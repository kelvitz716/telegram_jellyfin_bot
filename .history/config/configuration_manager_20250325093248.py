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
        default_config = {}
        
        # Check if config file exists
        if not os.path.exists(self.config_path):
            self._save_config(default_config)
            return default_config
        
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
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
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any):
        """
        Set configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self.config[key] = value
        self._save_config(self.config)
    
    def update(self, updates: Dict[str, Any]):
        """
        Update multiple configuration values.
        
        Args:
            updates: Dictionary of configuration updates
        """
        self.config.update(updates)
        self._save_config(self.config)