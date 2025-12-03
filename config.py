import os
from pathlib import Path

# Bot Configuration
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
PORT = int(os.getenv("PORT", "10000"))

# Directory Configuration
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Quality Settings - ADVANCED ENCODING
QUALITY_SETTINGS = {
    "360p": {
        "height": 360,
        "bitrate": "500k",
        "audio_bitrate": "96k",
        "preset": "medium"
    },
    "480p": {
        "height": 480,
        "bitrate": "1000k",
        "audio_bitrate": "128k",
        "preset": "medium"
    },
    "720p": {
        "height": 720,
        "bitrate": "2500k",
        "audio_bitrate": "192k",
        "preset": "medium"
    },
    "1080p": {
        "height": 1080,
        "bitrate": "5000k",
        "audio_bitrate": "256k",
        "preset": "medium"
    }
}

# Supported File Types
SUPPORTED_TYPES = {
    'video': ['.m3u8', '.mpd', '.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.webm', '.ts'],
    'image': ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.svg'],
    'document': ['.pdf', '.doc', '.docx', '.txt', '.zip', '.rar']
}

# ULTRA SPEED SETTINGS - 6-7x FASTER! 🚀
CHUNK_SIZE = 131072  # 128KB chunks
CONCURRENT_FRAGMENTS = 16  # 4x increase
MAX_CONCURRENT_DOWNLOADS = 5  # Parallel downloads
BUFFER_SIZE = 524288  # 512KB buffer
HTTP_CHUNK_SIZE = 2097152  # 2MB chunks

# Upload Settings - SUPERCHARGED
UPLOAD_CHUNK_SIZE = 1048576  # 1MB chunks
MAX_RETRIES = 25
FRAGMENT_RETRIES = 25
CONNECTION_TIMEOUT = 3600  # 60 minutes

# Advanced Speed Settings
DYNAMIC_WORKERS = True
MIN_WORKERS = 8
MAX_WORKERS = 32
WORKER_ADJUST_THRESHOLD = 5

# Connection Pool Settings
CONNECTION_POOL_SIZE = 100
CONNECTION_POOL_PER_HOST = 50
DNS_CACHE_TTL = 600

# Thumbnail Settings
THUMBNAIL_TIME = "00:00:05"
THUMBNAIL_SIZE = "640:360"
THUMBNAIL_QUALITY = 2

# ==========================================
# 🚀 FFMPEG-BASED SPLIT SETTINGS (MOST RELIABLE)
# ==========================================

TELEGRAM_FILE_LIMIT = 2000  # 2GB in MB
SAFE_SPLIT_SIZE = 1900  # Split at 1.9GB (100MB buffer)
MIN_PART_SIZE = 100  # Minimum 100MB per part

# FFMPEG split method (copy codec - INSTANT splitting)
USE_FFMPEG_SPLIT = True  # Most reliable for large files
SPLIT_METHOD = 'copy'  # 'copy' = fast, 'encode' = slower but compatible

# ==========================================

# Text Overlay Settings
WATERMARK_ENABLED = True
WATERMARK_FONT = "Arial"
WATERMARK_FONT_SIZE = 48
WATERMARK_COLOR = "white"
WATERMARK_POSITION = "bottom_right"  # top_left, top_right, bottom_left, bottom_right, center
WATERMARK_OPACITY = 0.8

# Progress Update Settings
PROGRESS_UPDATE_INTERVAL = 0.5
UPLOAD_PROGRESS_INTERVAL = 2

# Session Management
SESSION_TIMEOUT = 3600  # 1 hour

# Destination Channel Settings
DESTINATION_STORAGE_FILE = Path("destination_channels.json")
