DEFAULT_CONFIG = {
    "paths": {
        "telegram_download_dir": str(Path.home() / "telegram-jellyfin-bot" / "downloads"),
        "movies_dir": str(Path.home() / "telegram-jellyfin-bot" / "movies"),
        "tv_shows_dir": str(Path.home() / "telegram-jellyfin-bot" / "tvshows"),
        "unmatched_dir": str(Path.home() / "telegram-jellyfin-bot" / "unmatched")
    },
    "tmdb": {
        "api_key": "YOUR_TMDB_API_KEY"
    },
    "telegram": {
        "bot_token": "YOUR_TELEGRAM_BOT_TOKEN",
        "enabled": False
    },
    "logging": {
        "level": "INFO",
        "max_size_mb": 10,
        "backup_count": 5
    }
}
