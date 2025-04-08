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
            # Default configuration
            self.config = {
                'paths': {
                    'download_dir': os.path.expanduser('~/Downloads/MediaManager')
                },
                'telegram': {
                    'api_id': None,
                    'api_hash': None,
                    'bot_token': None
                }
            }
            
            # Set config path (use default if not provided)
            self.config_path = config_path or os.path.join(
                os.path.dirname(__file__), 
                'config.json'
            )
            
            # Load existing configuration
            self._load_config()
        
        def _load_config(self):
            """
            Load configuration from file, merging with default.
            """
            try:
                if os.path.exists(self.config_path):
                    with open(self.config_path, 'r') as f:
                        loaded_config = json.load(f)
                        self.config = self._deep_merge(self.config, loaded_config)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading config: {e}")
            
            # Ensure config file exists
            self._save_config()
        
        def _deep_merge(self, base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
            """
            Recursively merge dictionaries.
            
            Args:
                base: Base dictionary
                update: Dictionary to merge into base
            
            Returns:
                Merged dictionary
            """
            for key, value in update.items():
                if isinstance(value, dict):
                    base[key] = self._deep_merge(base.get(key, {}), value)
                else:
                    base[key] = value
            return base
        
        def get(self, *keys, default=None):
            """
            Retrieve nested configuration with fallback.
            
            Args:
                *keys: Nested keys to retrieve
                default: Default value if key not found
            
            Returns:
                Configuration value or default
            """
            current = self.config
            for key in keys:
                if isinstance(current, dict):
                    current = current.get(key, {})
                else:
                    return default
            
            return current if current else default
        
        def set(self, *keys, value):
            """
            Set nested configuration value.
            
            Args:
                *keys: Nested keys
                value: Value to set
            """
            current = self.config
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]
            
            current[keys[-1]] = value
            self._save_config()
        
        def _save_config(self):
            """
            Save configuration to file.
            """
            try:
                with open(self.config_path, 'w') as f:
                    json.dump(self.config, f, indent=4)
            except IOError as e:
                print(f"Error saving config: {e}")