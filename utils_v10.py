import re
import os
import json
import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from config import SUPPORTED_TYPES, SAFE_SPLIT_SIZE, DESTINATION_STORAGE_FILE

logger = logging.getLogger(__name__)


def get_file_type(url: str) -> str:
    """Determine file type from URL with enhanced detection"""
    url_lower = url.lower()
    
    # Check for YouTube links
    if is_youtube_url(url):
        return 'video'
    
    for ftype, extensions in SUPPORTED_TYPES.items():
        if any(ext in url_lower for ext in extensions):
            return ftype
    
    return 'unknown'


def parse_content(text: str) -> List[Dict]:
    """Parse content and identify all supported file types"""
    lines = text.strip().split('\n')
    items = []
    
    for line in lines:
        if ':' in line and ('http://' in line or 'https://' in line):
            parts = line.split(':', 1)
            if len(parts) == 2:
                title = parts[0].strip()
                url = parts[1].strip()
                
                file_type = get_file_type(url)
                
                if file_type != 'unknown':
                    items.append({
                        'title': title, 
                        'url': url, 
                        'type': file_type
                    })
    
    return items


def format_size(bytes_size: int) -> str:
    """Format bytes to human readable size"""
    if bytes_size < 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def format_time(seconds: int) -> str:
    """Format seconds to human readable time"""
    if seconds < 0:
        return "0s"
    
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"


def sanitize_filename(filename: str, max_length: int = 50) -> str:
    """Sanitize filename for safe file system usage"""
    safe = re.sub(r'[^\w\s-]', '', filename)
    safe = safe.replace(' ', '_')
    return safe[:max_length].strip('_')


def create_progress_bar(percent: float, length: int = 20) -> str:
    """Create a visual progress bar"""
    filled = int(length * percent / 100)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {percent:.1f}%"


async def split_large_file(file_path: str, max_size_mb: int = SAFE_SPLIT_SIZE) -> List[str]:
    """
    SMART file splitting with intelligent part sizing
    Handles 2GB+ files perfectly
    """
    try:
        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)
        
        logger.info(f"File size: {file_size_mb:.2f}MB, Split threshold: {max_size_mb}MB")
        
        if file_size_mb <= max_size_mb:
            logger.info("File within limit, no splitting needed")
            return [file_path]
        
        # Calculate optimal part size and count
        chunk_size = int(max_size_mb * 1024 * 1024)
        num_parts = (file_size + chunk_size - 1) // chunk_size
        
        logger.info(f"🔪 Splitting {file_path} into {num_parts} parts")
        
        base_name = os.path.basename(file_path)
        name, ext = os.path.splitext(base_name)
        dir_path = os.path.dirname(file_path)
        
        parts = []
        
        # Read and split file with buffer
        with open(file_path, 'rb') as source:
            for i in range(num_parts):
                part_name = f"{name}_part{i+1:03d}_of_{num_parts:03d}{ext}"
                part_path = os.path.join(dir_path, part_name)
                
                bytes_written = 0
                with open(part_path, 'wb') as part:
                    while bytes_written < chunk_size:
                        chunk = source.read(min(1024 * 1024, chunk_size - bytes_written))
                        if not chunk:
                            break
                        part.write(chunk)
                        bytes_written += len(chunk)
                
                if bytes_written > 0:
                    parts.append(part_path)
                    part_size_mb = bytes_written / (1024 * 1024)
                    logger.info(f"✅ Created part {i+1}/{num_parts}: {part_name} ({part_size_mb:.2f}MB)")
        
        logger.info(f"🎉 File split successfully into {len(parts)} parts")
        return parts
        
    except Exception as e:
        logger.error(f"❌ File splitting error: {e}", exc_info=True)
        return []


def is_youtube_url(url: str) -> bool:
    """Check if URL is a YouTube video"""
    youtube_patterns = [
        r'youtube\.com/watch\?v=',
        r'youtu\.be/',
        r'youtube\.com/embed/',
        r'youtube\.com/v/',
        r'youtube\.com/shorts/',
    ]
    
    return any(re.search(pattern, url, re.IGNORECASE) for pattern in youtube_patterns)


def is_unsupported_platform(url: str) -> bool:
    """Check if URL is from unsupported platforms"""
    unsupported = [
        'instagram.com',
        'facebook.com',
        'fb.watch',
        'twitter.com',
        'x.com',
        'tiktok.com',
        'snapchat.com',
    ]
    
    url_lower = url.lower()
    return any(platform in url_lower for platform in unsupported)


def extract_channel_id(text: str) -> Optional[int]:
    """
    Extract channel/group ID from:
    - Forwarded message
    - Message link
    - Direct ID input
    """
    try:
        # Check if it's a direct ID (negative for groups/channels)
        if text.startswith('-') or text.isdigit():
            return int(text)
        
        # Extract from telegram link
        # Format: t.me/c/1234567890/123 or t.me/channelname
        link_patterns = [
            r't\.me/c/(\d+)',  # Private channel
            r't\.me/([a-zA-Z0-9_]+)',  # Public channel
        ]
        
        for pattern in link_patterns:
            match = re.search(pattern, text)
            if match:
                # For public channels, return the username
                return match.group(1)
        
        return None
        
    except Exception as e:
        logger.error(f"Error extracting channel ID: {e}")
        return None


async def save_destination_channel(user_id: int, channel_id: int, channel_name: str = ""):
    """Save destination channel for user"""
    try:
        data = {}
        if DESTINATION_STORAGE_FILE.exists():
            with open(DESTINATION_STORAGE_FILE, 'r') as f:
                data = json.load(f)
        
        data[str(user_id)] = {
            'channel_id': channel_id,
            'channel_name': channel_name,
            'timestamp': asyncio.get_event_loop().time()
        }
        
        with open(DESTINATION_STORAGE_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"✅ Saved destination channel for user {user_id}: {channel_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving destination: {e}")
        return False


async def get_destination_channel(user_id: int) -> Optional[Tuple[int, str]]:
    """Get saved destination channel for user"""
    try:
        if not DESTINATION_STORAGE_FILE.exists():
            return None
        
        with open(DESTINATION_STORAGE_FILE, 'r') as f:
            data = json.load(f)
        
        user_data = data.get(str(user_id))
        if user_data:
            return (user_data['channel_id'], user_data.get('channel_name', ''))
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting destination: {e}")
        return None


async def clear_destination_channel(user_id: int):
    """Clear destination channel for user"""
    try:
        if not DESTINATION_STORAGE_FILE.exists():
            return
        
        with open(DESTINATION_STORAGE_FILE, 'r') as f:
            data = json.load(f)
        
        if str(user_id) in data:
            del data[str(user_id)]
            
            with open(DESTINATION_STORAGE_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Cleared destination for user {user_id}")
    
    except Exception as e:
        logger.error(f"Error clearing destination: {e}")


def get_video_extension(url: str) -> str:
    """Get appropriate video extension from URL"""
    url_lower = url.lower()
    
    if '.m3u8' in url_lower or '/m3u8' in url_lower:
        return '.mp4'
    elif '.mpd' in url_lower:
        return '.mp4'
    elif '.mkv' in url_lower:
        return '.mkv'
    elif '.webm' in url_lower:
        return '.webm'
    else:
        return '.mp4'


def estimate_download_time(file_size_mb: float, speed_mbps: float) -> int:
    """Estimate download time in seconds"""
    if speed_mbps <= 0:
        return 0
    
    time_seconds = (file_size_mb * 8) / speed_mbps
    return int(time_seconds)


def validate_url(url: str) -> bool:
    """Validate if URL is properly formatted"""
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return url_pattern.match(url) is not None
