import os
import asyncio
import logging
import time
import subprocess
import json
from typing import Optional, List
from pathlib import Path
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError
from utils import format_size, format_time, create_progress_bar
from config import (
    UPLOAD_CHUNK_SIZE, SAFE_SPLIT_SIZE, 
    UPLOAD_PROGRESS_INTERVAL, DOWNLOAD_DIR
)

logger = logging.getLogger(__name__)


class UploadProgressTracker:
    """Enhanced upload progress tracker"""
    
    def __init__(self, progress_msg: Message, filename: str, part_num: int = 0, total_parts: int = 1):
        self.progress_msg = progress_msg
        self.filename = filename
        self.part_num = part_num
        self.total_parts = total_parts
        self.last_update = 0
        self.start_time = time.time()
        self.last_percent = -1
        self.speeds = []
    
    async def progress_callback(self, current: int, total: int):
        """Real-time progress"""
        try:
            now = time.time()
            
            if now - self.last_update < UPLOAD_PROGRESS_INTERVAL:
                return
            
            percent = (current / total) * 100 if total > 0 else 0
            
            if abs(int(percent) - self.last_percent) >= 2:
                self.last_percent = int(percent)
                self.last_update = now
                
                elapsed = now - self.start_time
                speed = current / elapsed if elapsed > 0 else 0
                
                self.speeds.append(speed)
                if len(self.speeds) > 10:
                    self.speeds.pop(0)
                
                avg_speed = sum(self.speeds) / len(self.speeds)
                eta = int((total - current) / avg_speed) if avg_speed > 0 else 0
                
                bar = create_progress_bar(percent)
                
                part_info = ""
                if self.total_parts > 1:
                    part_info = f"📊 Part {self.part_num}/{self.total_parts}\n"
                
                await self.progress_msg.edit_text(
                    f"📤 **UPLOADING**\n\n"
                    f"{part_info}"
                    f"{bar}\n\n"
                    f"📦 {format_size(current)} / {format_size(total)}\n"
                    f"🚀 {format_size(int(avg_speed))}/s\n"
                    f"⏱️ {format_time(eta)}"
                )
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except:
            pass


def get_video_metadata(video_path: str) -> dict:
    """
    ⚡ FAST metadata extraction for ALL parts
    Gets duration, width, height for proper display
    """
    try:
        cmd = [
            'ffprobe', '-v', 'quiet',
            '-print_format', 'json',
            '-show_format', '-show_streams',
            video_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode != 0:
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
        
        return {
            'duration': max(duration, 1),  # At least 1 second
            'width': width if width > 0 else 1280,
            'height': height if height > 0 else 720
        }
        
    except Exception as e:
        logger.error(f"Metadata error: {e}")
        return {'duration': 0, 'width': 1280, 'height': 720}


def generate_thumbnail_for_part(video_path: str, thumb_path: str) -> bool:
    """
    ⚡ Generate thumbnail for ANY part (even if short duration)
    """
    try:
        # Try at 1 second
        cmd = [
            'ffmpeg', '-y',
            '-ss', '00:00:01',
            '-i', video_path,
            '-vframes', '1',
            '-vf', 'scale=320:180',
            '-q:v', '2',
            thumb_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, timeout=15)
        
        if result.returncode == 0 and os.path.exists(thumb_path):
            if os.path.getsize(thumb_path) > 1024:
                return True
        
        # Fallback: Try at start (0s)
        cmd[2] = '00:00:00'
        result = subprocess.run(cmd, capture_output=True, timeout=15)
        
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 1024:
            return True
        
        return False
        
    except:
        return False


def find_all_parts(base_path: str) -> List[str]:
    """
    🔍 Find all parts - OPTIMIZED
    """
    base_file = Path(base_path)
    
    # Check if part file
    if '_part' in base_file.stem and '_of_' in base_file.stem:
        parts = base_file.stem.split('_part')
        base_name = parts[0]
        ext = base_file.suffix
        
        pattern = f"{base_name}_part*_of_*{ext}"
        all_parts = sorted(DOWNLOAD_DIR.glob(pattern))
        
        if len(all_parts) > 1:
            logger.info(f"🔍 Multi-part: {len(all_parts)} parts")
            return [str(p) for p in all_parts]
    
    # Check for parts
    base_name = base_file.stem
    ext = base_file.suffix
    pattern = f"{base_name}_part*_of_*{ext}"
    possible_parts = sorted(DOWNLOAD_DIR.glob(pattern))
    
    if possible_parts:
        logger.info(f"🔍 Found {len(possible_parts)} parts")
        return [str(p) for p in possible_parts]
    
    # Single file
    if os.path.exists(base_path):
        return [base_path]
    
    return []


async def upload_video(
    client: Client,
    chat_id: int,
    video_path: str,
    caption: str,
    progress_msg: Message,
    thumb_path: Optional[str] = None,
    duration: int = 0,
    width: int = 1280,
    height: int = 720
) -> bool:
    """
    🚀 OPTIMIZED upload with proper metadata for ALL parts
    """
    try:
        # Find parts
        all_parts = find_all_parts(video_path)
        
        if not all_parts:
            logger.error("❌ No files found!")
            await progress_msg.edit_text("❌ File not found!")
            return False
        
        # SINGLE FILE - FAST PATH
        if len(all_parts) == 1:
            file_path = all_parts[0]
            
            if not os.path.exists(file_path):
                return False
            
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            logger.info(f"📹 Single file: {file_size:.2f}MB")
            
            tracker = UploadProgressTracker(progress_msg, os.path.basename(file_path))
            
            try:
                await client.send_video(
                    chat_id=chat_id,
                    video=file_path,
                    caption=caption,
                    supports_streaming=True,
                    duration=duration,
                    width=width,
                    height=height,
                    thumb=thumb_path,
                    progress=tracker.progress_callback
                )
                
                logger.info(f"✅ Upload success")
                
                # Cleanup
                try:
                    os.remove(file_path)
                    if thumb_path and os.path.exists(thumb_path):
                        os.remove(thumb_path)
                except:
                    pass
                
                return True
                
            except FloodWait as e:
                await asyncio.sleep(e.value)
                await client.send_video(
                    chat_id=chat_id,
                    video=file_path,
                    caption=caption,
                    supports_streaming=True,
                    duration=duration,
                    width=width,
                    height=height,
                    thumb=thumb_path,
                    progress=tracker.progress_callback
                )
                return True
        
        # MULTI-PART - Extract metadata for EACH part
        logger.info(f"📦 Multi-part: {len(all_parts)} parts")
        
        total_size = sum(os.path.getsize(p) for p in all_parts if os.path.exists(p))
        total_size_mb = total_size / (1024 * 1024)
        
        await progress_msg.edit_text(
            f"📦 **MULTI-PART UPLOAD**\n\n"
            f"Parts: {len(all_parts)}\n"
            f"Total: {total_size_mb:.1f}MB\n\n"
            f"⚡ Uploading..."
        )
        
        uploaded_count = 0
        
        # Upload each part with PROPER metadata
        for i, part_path in enumerate(all_parts, 1):
            if not os.path.exists(part_path):
                logger.warning(f"⚠️ Part {i} not found")
                continue
            
            part_size = os.path.getsize(part_path) / (1024 * 1024)
            part_name = os.path.basename(part_path)
            
            logger.info(f"📤 Part {i}/{len(all_parts)}: {part_name} ({part_size:.1f}MB)")
            
            # 🔥 KEY: Get metadata for THIS part
            metadata = get_video_metadata(part_path)
            part_duration = metadata['duration']
            part_width = metadata['width']
            part_height = metadata['height']
            
            logger.info(f"   Duration: {part_duration}s, Size: {part_width}x{part_height}")
            
            # Generate thumbnail for THIS part
            part_thumb_path = str(DOWNLOAD_DIR / f"thumb_part{i}_{os.getpid()}.jpg")
            has_thumb = generate_thumbnail_for_part(part_path, part_thumb_path)
            
            # Caption
            part_caption = f"{caption}\n\n📦 **Part {i}/{len(all_parts)}**\n💾 {part_size:.1f}MB"
            
            tracker = UploadProgressTracker(progress_msg, part_name, i, len(all_parts))
            
            # Upload with retry
            retry_count = 0
            upload_success = False
            
            while retry_count < 3 and not upload_success:
                try:
                    await client.send_video(
                        chat_id=chat_id,
                        video=part_path,
                        caption=part_caption,
                        supports_streaming=True,
                        duration=part_duration,  # Correct duration
                        width=part_width,         # Correct width
                        height=part_height,       # Correct height
                        thumb=part_thumb_path if has_thumb else None,
                        progress=tracker.progress_callback
                    )
                    
                    upload_success = True
                    uploaded_count += 1
                    logger.info(f"✅ Part {i} uploaded!")
                    
                except FloodWait as e:
                    logger.warning(f"⏳ FloodWait {e.value}s")
                    await asyncio.sleep(e.value)
                    retry_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Part {i} error: {e}")
                    retry_count += 1
                    if retry_count < 3:
                        await asyncio.sleep(5)
            
            # Cleanup part & thumb
            try:
                if os.path.exists(part_path):
                    os.remove(part_path)
                if has_thumb and os.path.exists(part_thumb_path):
                    os.remove(part_thumb_path)
            except:
                pass
            
            await asyncio.sleep(0.5)
        
        # Cleanup main thumbnail
        try:
            if thumb_path and os.path.exists(thumb_path):
                os.remove(thumb_path)
        except:
            pass
        
        # Summary
        if uploaded_count == len(all_parts):
            await progress_msg.edit_text(
                f"✅ **ALL PARTS UPLOADED!**\n\n"
                f"📦 Parts: {len(all_parts)}\n"
                f"💾 Size: {total_size_mb:.1f}MB\n\n"
                f"🎉 Complete!"
            )
            return True
        else:
            await progress_msg.edit_text(
                f"⚠️ **PARTIAL UPLOAD**\n\n"
                f"✔️ Success: {uploaded_count}/{len(all_parts)}\n"
                f"❌ Failed: {len(all_parts) - uploaded_count}"
            )
            return False
        
    except Exception as e:
        logger.error(f"❌ Upload error: {e}", exc_info=True)
        return False


async def upload_photo(
    client: Client,
    chat_id: int,
    photo_path: str,
    caption: str,
    progress_msg: Message
) -> bool:
    """Upload photo - FAST"""
    try:
        if not os.path.exists(photo_path):
            return False
        
        tracker = UploadProgressTracker(progress_msg, os.path.basename(photo_path))
        
        await client.send_photo(
            chat_id=chat_id,
            photo=photo_path,
            caption=caption,
            progress=tracker.progress_callback
        )
        
        logger.info(f"✅ Photo uploaded")
        
        try:
            os.remove(photo_path)
        except:
            pass
        
        return True
        
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return False
    except Exception as e:
        logger.error(f"❌ Photo error: {e}")
        return False


async def upload_document(
    client: Client,
    chat_id: int,
    document_path: str,
    caption: str,
    progress_msg: Message
) -> bool:
    """Upload document with multi-part support"""
    try:
        all_parts = find_all_parts(document_path)
        
        if not all_parts:
            return False
        
        # Single file
        if len(all_parts) == 1:
            file_path = all_parts[0]
            
            if not os.path.exists(file_path):
                return False
            
            tracker = UploadProgressTracker(progress_msg, os.path.basename(file_path))
            
            await client.send_document(
                chat_id=chat_id,
                document=file_path,
                caption=caption,
                progress=tracker.progress_callback
            )
            
            logger.info(f"✅ Document uploaded")
            
            try:
                os.remove(file_path)
            except:
                pass
            
            return True
        
        # Multi-part
        uploaded_count = 0
        
        for i, part_path in enumerate(all_parts, 1):
            if not os.path.exists(part_path):
                continue
            
            part_size = os.path.getsize(part_path) / (1024 * 1024)
            part_caption = f"{caption}\n\n📦 Part {i}/{len(all_parts)} ({part_size:.1f}MB)"
            
            tracker = UploadProgressTracker(progress_msg, os.path.basename(part_path), i, len(all_parts))
            
            retry_count = 0
            upload_success = False
            
            while retry_count < 3 and not upload_success:
                try:
                    await client.send_document(
                        chat_id=chat_id,
                        document=part_path,
                        caption=part_caption,
                        progress=tracker.progress_callback
                    )
                    
                    upload_success = True
                    uploaded_count += 1
                    
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    retry_count += 1
                except Exception as e:
                    logger.error(f"Part {i} error: {e}")
                    retry_count += 1
                    if retry_count < 3:
                        await asyncio.sleep(5)
            
            try:
                if os.path.exists(part_path):
                    os.remove(part_path)
            except:
                pass
            
            await asyncio.sleep(0.5)
        
        return uploaded_count == len(all_parts)
        
    except Exception as e:
        logger.error(f"❌ Document error: {e}")
        return False


async def send_failed_link(
    client: Client,
    chat_id: int,
    title: str,
    url: str,
    serial_num: int,
    reason: str = "Download failed",
    file_type: str = "content"
) -> bool:
    """Send failed link message"""
    try:
        type_emoji = {
            'video': '🎬',
            'image': '🖼️',
            'document': '📄',
            'content': '📦'
        }
        
        emoji = type_emoji.get(file_type, '📦')
        
        message = (
            f"❌ **{reason}**\n\n"
            f"{emoji} **Item #{serial_num}**\n"
            f"📝 Title: `{title}`\n\n"
            f"🔗 **Direct Link:**\n`{url}`\n\n"
            f"💡 **What to do:**\n"
            f"• Copy the link above\n"
            f"• Open in your browser\n"
            f"• Download manually if needed\n\n"
            f"⚠️ This content could not be processed automatically."
        )
        
        await client.send_message(
            chat_id=chat_id,
            text=message,
            disable_web_page_preview=False
        )
        
        logger.info(f"📧 Failed link sent for #{serial_num}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed link error: {e}")
        return False


async def send_to_destination(
    client: Client,
    destination_id: int,
    file_path: str,
    caption: str,
    file_type: str,
    progress_msg: Message,
    thumb_path: Optional[str] = None,
    duration: int = 0,
    width: int = 1280,
    height: int = 720
) -> bool:
    """Send to destination"""
    try:
        if file_type == 'video':
            return await upload_video(
                client, destination_id, file_path, caption,
                progress_msg, thumb_path, duration, width, height
            )
        
        elif file_type == 'image':
            return await upload_photo(
                client, destination_id, file_path, caption, progress_msg
            )
        
        elif file_type == 'document':
            return await upload_document(
                client, destination_id, file_path, caption, progress_msg
            )
        
        return False
        
    except Exception as e:
        logger.error(f"Destination error: {e}")
        return False
