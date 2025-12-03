import os
import json
import subprocess
import logging
from pathlib import Path
from typing import Dict, Optional
from config import (
    THUMBNAIL_TIME, THUMBNAIL_SIZE, THUMBNAIL_QUALITY,
    WATERMARK_ENABLED, WATERMARK_FONT, WATERMARK_FONT_SIZE,
    WATERMARK_COLOR, WATERMARK_POSITION, WATERMARK_OPACITY,
    QUALITY_SETTINGS
)

logger = logging.getLogger(__name__)


def get_video_info(filepath: str) -> Dict:
    """Get video duration and dimensions with enhanced error handling"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet',
            '-print_format', 'json',
            '-show_format', '-show_streams',
            filepath
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            logger.error(f"FFprobe failed: {result.stderr}")
            return {'duration': 0, 'width': 1280, 'height': 720}
        
        data = json.loads(result.stdout)
        
        # Get duration
        duration = 0
        if 'format' in data and 'duration' in data['format']:
            duration = int(float(data['format']['duration']))
        
        # Get video stream
        video_stream = next(
            (s for s in data.get('streams', []) if s.get('codec_type') == 'video'), 
            {}
        )
        
        width = video_stream.get('width', 1280)
        height = video_stream.get('height', 720)
        
        if duration == 0 and 'duration' in video_stream:
            duration = int(float(video_stream['duration']))
        
        if width <= 0 or height <= 0:
            width, height = 1280, 720
        
        # Ensure even dimensions
        width = width - (width % 2)
        height = height - (height % 2)
        
        logger.info(f"Video info: {width}x{height}, {duration}s")
        return {'duration': duration, 'width': width, 'height': height}
        
    except Exception as e:
        logger.error(f"FFprobe error: {e}")
        return {'duration': 0, 'width': 1280, 'height': 720}


def generate_thumbnail_with_text(
    video_path: str, 
    thumb_path: str, 
    watermark_text: str = "",
    video_duration: int = 0
) -> bool:
    """
    Generate thumbnail with optional text overlay
    6 FALLBACK METHODS + Text overlay support
    """
    try:
        # Determine best time for thumbnail
        if video_duration > 10:
            thumb_time = min(video_duration // 4, 15)
        elif video_duration > 5:
            thumb_time = 3
        else:
            thumb_time = 1
        
        thumb_time_str = f"00:00:{thumb_time:02d}"
        
        # Build filter complex for thumbnail + text overlay
        filter_parts = [f'scale={THUMBNAIL_SIZE}:force_original_aspect_ratio=decrease']
        
        if watermark_text and WATERMARK_ENABLED:
            # Position mapping
            positions = {
                'top_left': 'x=10:y=10',
                'top_right': 'x=w-tw-10:y=10',
                'bottom_left': 'x=10:y=h-th-10',
                'bottom_right': 'x=w-tw-10:y=h-th-10',
                'center': 'x=(w-tw)/2:y=(h-th)/2'
            }
            
            pos = positions.get(WATERMARK_POSITION, positions['bottom_right'])
            
            # Escape special characters in text
            escaped_text = watermark_text.replace("'", "'\\\\\\''").replace(":", "\\:")
            
            text_filter = (
                f"drawtext=text='{escaped_text}':"
                f"fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:"
                f"fontsize={WATERMARK_FONT_SIZE}:"
                f"fontcolor={WATERMARK_COLOR}@{WATERMARK_OPACITY}:"
                f"{pos}:"
                f"box=1:boxcolor=black@0.5:boxborderw=5"
            )
            filter_parts.append(text_filter)
        
        filter_complex = ','.join(filter_parts)
        
        # Method 1: Primary extraction with text overlay
        cmd = [
            'ffmpeg', '-ss', thumb_time_str,
            '-i', video_path,
            '-vframes', '1',
            '-vf', filter_complex,
            '-q:v', str(THUMBNAIL_QUALITY),
            thumb_path,
            '-y'
        ]
        
        logger.info(f"🎨 Thumbnail Method 1: {thumb_time_str} with text overlay")
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 2048:
            logger.info(f"✅ Thumbnail created: {os.path.getsize(thumb_path)} bytes")
            return True
        
        # Method 2: Try without seeking
        logger.warning("Method 1 failed, trying Method 2")
        cmd[1] = '00:00:00'
        subprocess.run(cmd, capture_output=True, timeout=60)
        
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 2048:
            logger.info("✅ Method 2 success")
            return True
        
        # Method 3: Middle of video
        if video_duration > 5:
            mid_time = f"00:00:{video_duration // 2:02d}"
            logger.warning(f"Trying Method 3: {mid_time}")
            cmd[1] = mid_time
            subprocess.run(cmd, capture_output=True, timeout=60)
            
            if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 2048:
                logger.info("✅ Method 3 success")
                return True
        
        # Method 4: Simple extraction without complex filters
        logger.warning("Trying Method 4: Simple extraction")
        simple_cmd = [
            'ffmpeg', '-i', video_path,
            '-vframes', '1',
            '-vf', f'scale={THUMBNAIL_SIZE}',
            '-q:v', '2',
            thumb_path,
            '-y'
        ]
        subprocess.run(simple_cmd, capture_output=True, timeout=60)
        
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 2048:
            logger.info("✅ Method 4 success")
            # Add text overlay separately if needed
            if watermark_text and WATERMARK_ENABLED:
                add_text_to_thumbnail(thumb_path, watermark_text)
            return True
        
        # Method 5: Raw extraction
        logger.warning("Trying Method 5: Raw")
        raw_cmd = ['ffmpeg', '-i', video_path, '-vframes', '1', thumb_path, '-y']
        subprocess.run(raw_cmd, capture_output=True, timeout=60)
        
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 2048:
            logger.info("✅ Method 5 success")
            if watermark_text and WATERMARK_ENABLED:
                add_text_to_thumbnail(thumb_path, watermark_text)
            return True
        
        # Method 6: End of video
        if video_duration > 10:
            end_time = f"00:00:{max(video_duration - 5, 1):02d}"
            logger.warning(f"Trying Method 6: {end_time}")
            cmd[1] = end_time
            subprocess.run(cmd, capture_output=True, timeout=60)
            
            if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 2048:
                logger.info("✅ Method 6 success")
                if watermark_text and WATERMARK_ENABLED:
                    add_text_to_thumbnail(thumb_path, watermark_text)
                return True
        
        logger.error("❌ All thumbnail methods failed")
        return False
        
    except Exception as e:
        logger.error(f"Thumbnail error: {e}")
        return False


def add_text_to_thumbnail(thumb_path: str, text: str) -> bool:
    """Add text overlay to existing thumbnail"""
    try:
        temp_path = thumb_path + ".temp.jpg"
        
        positions = {
            'top_left': 'x=10:y=10',
            'top_right': 'x=w-tw-10:y=10',
            'bottom_left': 'x=10:y=h-th-10',
            'bottom_right': 'x=w-tw-10:y=h-th-10',
            'center': 'x=(w-tw)/2:y=(h-th)/2'
        }
        
        pos = positions.get(WATERMARK_POSITION, positions['bottom_right'])
        escaped_text = text.replace("'", "'\\\\\\''").replace(":", "\\:")
        
        cmd = [
            'ffmpeg', '-i', thumb_path,
            '-vf', (
                f"drawtext=text='{escaped_text}':"
                f"fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:"
                f"fontsize={WATERMARK_FONT_SIZE}:"
                f"fontcolor={WATERMARK_COLOR}@{WATERMARK_OPACITY}:"
                f"{pos}:"
                f"box=1:boxcolor=black@0.5:boxborderw=5"
            ),
            temp_path,
            '-y'
        ]
        
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        
        if result.returncode == 0 and os.path.exists(temp_path):
            os.replace(temp_path, thumb_path)
            logger.info("✅ Text overlay added to thumbnail")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Text overlay error: {e}")
        return False


async def convert_video_quality(
    input_path: str,
    output_path: str,
    quality: str,
    progress_callback=None
) -> bool:
    """
    ADVANCED quality conversion with real encoding
    Actually changes video quality, not just resolution
    """
    try:
        if quality not in QUALITY_SETTINGS:
            logger.error(f"Invalid quality: {quality}")
            return False
        
        settings = QUALITY_SETTINGS[quality]
        height = settings['height']
        video_bitrate = settings['bitrate']
        audio_bitrate = settings['audio_bitrate']
        preset = settings['preset']
        
        logger.info(f"🎬 Converting to {quality}: {height}p, {video_bitrate} video, {audio_bitrate} audio")
        
        cmd = [
            'ffmpeg', '-i', input_path,
            '-vf', f'scale=-2:{height}',  # -2 maintains aspect ratio
            '-c:v', 'libx264',  # H.264 codec
            '-b:v', video_bitrate,
            '-maxrate', video_bitrate,
            '-bufsize', f"{int(video_bitrate[:-1]) * 2}k",
            '-preset', preset,
            '-c:a', 'aac',
            '-b:a', audio_bitrate,
            '-movflags', '+faststart',
            '-threads', '0',  # Use all CPU threads
            output_path,
            '-y'
        ]
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True,
            timeout=3600  # 1 hour max
        )
        
        if result.returncode == 0 and os.path.exists(output_path):
            input_size = os.path.getsize(input_path) / (1024 * 1024)
            output_size = os.path.getsize(output_path) / (1024 * 1024)
            logger.info(f"✅ Quality conversion complete: {input_size:.2f}MB → {output_size:.2f}MB")
            return True
        else:
            logger.error(f"FFmpeg conversion failed: {result.stderr}")
            return False
        
    except subprocess.TimeoutExpired:
        logger.error("Video conversion timeout")
        return False
    except Exception as e:
        logger.error(f"Conversion error: {e}")
        return False


def validate_video_file(filepath: str) -> bool:
    """Validate if video file is playable"""
    try:
        if not os.path.exists(filepath):
            logger.error(f"File does not exist: {filepath}")
            return False
        
        file_size = os.path.getsize(filepath)
        if file_size < 10240:
            logger.error(f"File too small: {file_size} bytes")
            return False
        
        cmd = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=codec_type',
            '-of', 'json',
            filepath
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=15)
        
        if result.returncode != 0:
            logger.error("Video validation failed")
            return False
        
        try:
            data = json.loads(result.stdout)
            if 'streams' in data and len(data['streams']) > 0:
                logger.info(f"✅ Video validated: {filepath}")
                return True
        except:
            pass
        
        return False
        
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False


def get_video_codec_info(filepath: str) -> Dict:
    """Get detailed codec information"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet',
            '-print_format', 'json',
            '-show_streams',
            '-select_streams', 'v:0',
            filepath
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            if 'streams' in data and len(data['streams']) > 0:
                stream = data['streams'][0]
                return {
                    'codec': stream.get('codec_name', 'unknown'),
                    'profile': stream.get('profile', 'unknown'),
                    'bit_rate': stream.get('bit_rate', '0'),
                    'fps': eval(stream.get('r_frame_rate', '0/1'))
                }
        
        return {}
        
    except Exception as e:
        logger.error(f"Codec info error: {e}")
        return {}
