import os

# Configuration settings for the Telegram Media Manager project

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
SORTED_MEDIA_DIR = os.path.join(BASE_DIR, "sorted_media")
TELEGRAM_DOWNLOADS_DIR = "/home/kelvitz/Videos/Telegram/Downloads"
JELLYFIN_DIR = "/home/kelvitz/Videos/Jellyfin"

# API Keys
TMDB_API_KEY = "9bff30355935e6a42153d1999e784794"

# Other settings
MAX_DOWNLOAD_SIZE = 2 * 1024 * 1024 * 1024  # 2GB