#cli/interactive_config.py:

import os
import sys
import getpass
from typing import Dict, Any

class InteractiveConfigManager:
    """
    Interactive configuration setup and management.
    """
    @classmethod
    def prompt_config(cls) -> Dict[str, Any]:
        """
        Interactively collect configuration details.
        
        Returns:
            Configuration dictionary
        """
        config = {}
        
        # Telegram Configuration
        print("\n=== Telegram Configuration ===")
        config['telegram'] = cls._prompt_telegram_config()
        
        # Path Configuration
        print("\n=== Path Configuration ===")
        config['paths'] = cls._prompt_path_config()
        
        # API Configuration
        print("\n=== API Configuration ===")
        config['apis'] = cls._prompt_api_config()
        
        return config
    
    @classmethod
    def _prompt_telegram_config(cls) -> Dict[str, str]:
        """
        Collect Telegram configuration details.
        
        Returns:
            Telegram configuration dictionary
        """
        return {
            'api_id': input("Enter Telegram API ID: "),
            'api_hash': getpass.getpass("Enter Telegram API Hash: "),
            'bot_token': input("Enter Telegram Bot Token (optional): ") or None
        }
    
    @classmethod
    def _prompt_path_config(self) -> Dict[str, str]:
        """
        Collect path configuration details.
        
        Returns:
            Path configuration dictionary
        """
        default_download = os.path.expanduser("~/Downloads/MediaManager")
        
        return {
            'download_dir': input(f"Download Directory [{default_download}]: ") or default_download,
            'movies_dir': input("Movies Directory [default]: ") or None,
            'tv_shows_dir': input("TV Shows Directory [default]: ") or None
        }
    
    @classmethod
    def _prompt_api_config(cls) -> Dict[str, str]:
        """
        Collect API configuration details.
        
        Returns:
            API configuration dictionary
        """
        return {
            'tmdb_api_key': input("TMDB API Key (optional): ") or None,
            'omdb_api_key': input("OMDB API Key (optional): ") or None
        }
    
    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> bool:
        """
        Validate collected configuration.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            True if configuration is valid
        """
        # Telegram configuration validation
        if not (config['telegram'].get('api_id') and 
                config['telegram'].get('api_hash')):
            print("Error: Telegram API ID and Hash are required")
            return False
        
        return True
    
    @classmethod
    def save_config(cls, config: Dict[str, Any], path: str):
        """
        Save configuration to file.
        
        Args:
            config: Configuration dictionary
            path: Configuration file path
        """
        import json
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        with open(path, 'w') as f:
            json.dump(config, f, indent=4)
        
        print(f"Configuration saved to {path}")

def run_interactive_setup():
    """
    Run interactive configuration setup.
    """
    print("=== Media Manager Configuration Setup ===")
    
    config_manager = InteractiveConfigManager()
    config = config_manager.prompt_config()
    
    if config_manager.validate_config(config):
        default_config_path = os.path.expanduser("~/.media_manager/config.json")
        config_manager.save_config(config, default_config_path)
        sys.exit(0)
    else:
        print("Configuration validation failed.")
        sys.exit(1)

if __name__ == "__main__":
    run_interactive_setup()