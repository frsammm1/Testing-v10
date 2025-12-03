import os
import asyncio
import logging
import time
from typing import Optional, List
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError
from utils import format_size, format_time, create_progress_bar, split_large_file
from config import (
    UPLOAD_CHUNK_SIZE, TELEGRAM_FILE_LIMIT, 
    SAFE_SPLIT_SIZE, UPLOAD_PROGRESS_INTERVAL
)

logger = logging.getLogger(__name__)


class UploadProgressTracker:
    """Enhanced upload progress tracker with speed monitoring"""
    
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
        """Real-time progress with speed calculation"""
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
                    f"📤 **UPLOADING TO TELEGRAM**\n\n"
                    f"{part_info}"
                    f"{bar}\n\n"
                    f"📦 {format_size(current)} / {format_size(total)}\n"
                    f"🚀 Speed: {format_size(int(avg_speed))}/s\n"
                    f"⏱️ ETA: {format_time(eta)}"
                )
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.debug(f"Progress update error: {e}")


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
    Upload video with SMART 2GB+ handling
    Auto-splits large files perfectly
    """
    try:
        file_size = os.path.getsize(video_path)
        file_size_mb = file_size / (1024 * 1024)
        
        logger.info(f"📹 Video size: {file_size_mb:.2f}MB, Limit: {TELEGRAM_FILE_LIMIT}MB")
        
        # Check if splitting needed
        if file_size_mb > SAFE_SPLIT_SIZE:
            logger.info(f"🔪 Large file detected! Splitting...")
            await progress_msg.edit_text(
                f"📦 **Large File Detected!**\n\n"
                f"Size: {file_size_mb:.1f}MB\n"
                f"Telegram Limit: {TELEGRAM_FILE_LIMIT}MB\n\n"
                f"🔪 Splitting into parts...\n"
                f"Please wait..."
            )
            
            # Split the file
            parts = await split_large_file(video_path, SAFE_SPLIT_SIZE)
            
            if not parts or len(parts) == 0:
                logger.error("❌ File splitting failed!")
                await progress_msg.edit_text(
                    "❌ **Splitting Failed!**\n\n"
                    "Could not split the large file.\n"
                    "This might be due to disk space issues."
                )
                return False
            
            logger.info(f"✅ File split into {len(parts)} parts")
            
            # Upload each part
            for i, part_path in enumerate(parts, 1):
                if not os.path.exists(part_path):
                    logger.error(f"Part {i} not found: {part_path}")
                    continue
                
                part_size = os.path.getsize(part_path) / (1024 * 1024)
                part_caption = f"{caption}\n\n📦 **Part {i}/{len(parts)}** ({part_size:.1f}MB)"
                
                tracker = UploadProgressTracker(progress_msg, os.path.basename(part_path), i, len(parts))
                
                try:
                    await client.send_video(
                        chat_id=chat_id,
                        video=part_path,
                        caption=part_caption,
                        supports_streaming=True,
                        duration=duration if i == 1 else 0,
                        width=width if i == 1 else 0,
                        height=height if i == 1 else 0,
                        thumb=thumb_path if i == 1 and thumb_path else None,
                        progress=tracker.progress_callback
                    )
                    
                    logger.info(f"✅ Part {i}/{len(parts)} uploaded successfully!")
                    
                except FloodWait as e:
                    logger.warning(f"FloodWait {e.value}s, waiting...")
                    await asyncio.sleep(e.value)
                    # Retry
                    await client.send_video(
                        chat_id=chat_id,
                        video=part_path,
                        caption=part_caption,
                        supports_streaming=True,
                        progress=tracker.progress_callback
                    )
                
                except Exception as e:
                    logger.error(f"❌ Part {i} upload failed: {e}")
                    return False
                
                finally:
                    # Cleanup part file
                    try:
                        if os.path.exists(part_path):
                            os.remove(part_path)
                            logger.info(f"🗑️ Cleaned up part {i}")
                    except Exception as e:
                        logger.debug(f"Cleanup error: {e}")
            
            # All parts uploaded successfully
            await progress_msg.edit_text(
                f"✅ **All Parts Uploaded!**\n\n"
                f"📦 Total Parts: {len(parts)}\n"
                f"💾 Total Size: {file_size_mb:.1f}MB\n\n"
                f"🎉 Upload Complete!"
            )
            return True
        
        # Normal upload for files under limit
        logger.info(f"📤 Uploading video normally (under limit)")
        tracker = UploadProgressTracker(progress_msg, os.path.basename(video_path))
        
        await client.send_video(
            chat_id=chat_id,
            video=video_path,
            caption=caption,
            supports_streaming=True,
            duration=duration,
            width=width,
            height=height,
            thumb=thumb_path,
            progress=tracker.progress_callback
        )
        
        logger.info(f"✅ Video uploaded successfully: {video_path}")
        return True
        
    except FloodWait as e:
        logger.warning(f"FloodWait: {e.value}s")
        await asyncio.sleep(e.value)
        return False
    
    except Exception as e:
        logger.error(f"❌ Video upload error: {e}", exc_info=True)
        return False


async def upload_photo(
    client: Client,
    chat_id: int,
    photo_path: str,
    caption: str,
    progress_msg: Message
) -> bool:
    """Upload photo with progress tracking"""
    try:
        tracker = UploadProgressTracker(progress_msg, os.path.basename(photo_path))
        
        await client.send_photo(
            chat_id=chat_id,
            photo=photo_path,
            caption=caption,
            progress=tracker.progress_callback
        )
        
        logger.info(f"✅ Photo uploaded: {photo_path}")
        return True
        
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return False
    
    except Exception as e:
        logger.error(f"❌ Photo upload error: {e}")
        return False


async def upload_document(
    client: Client,
    chat_id: int,
    document_path: str,
    caption: str,
    progress_msg: Message
) -> bool:
    """
    Upload document with auto-splitting for large files
    """
    try:
        file_size = os.path.getsize(document_path)
        file_size_mb = file_size / (1024 * 1024)
        
        logger.info(f"📄 Document size: {file_size_mb:.2f}MB")
        
        # Check if splitting needed
        if file_size_mb > SAFE_SPLIT_SIZE:
            logger.info(f"🔪 Large document! Splitting...")
            await progress_msg.edit_text(
                f"📦 **Large Document Detected!**\n\n"
                f"Size: {file_size_mb:.1f}MB\n"
                f"Splitting into parts..."
            )
            
            parts = await split_large_file(document_path, SAFE_SPLIT_SIZE)
            
            if not parts:
                logger.error("Document splitting failed")
                return False
            
            # Upload each part
            for i, part_path in enumerate(parts, 1):
                if not os.path.exists(part_path):
                    continue
                
                part_size = os.path.getsize(part_path) / (1024 * 1024)
                part_caption = f"{caption}\n\n📦 Part {i}/{len(parts)} ({part_size:.1f}MB)"
                
                tracker = UploadProgressTracker(progress_msg, os.path.basename(part_path), i, len(parts))
                
                try:
                    await client.send_document(
                        chat_id=chat_id,
                        document=part_path,
                        caption=part_caption,
                        progress=tracker.progress_callback
                    )
                    
                    logger.info(f"✅ Document part {i}/{len(parts)} uploaded")
                
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    # Retry
                    await client.send_document(
                        chat_id=chat_id,
                        document=part_path,
                        caption=part_caption,
                        progress=tracker.progress_callback
                    )
                
                except Exception as e:
                    logger.error(f"Part {i} upload failed: {e}")
                    return False
                
                finally:
                    try:
                        if os.path.exists(part_path):
                            os.remove(part_path)
                    except:
                        pass
            
            return True
        
        # Normal upload
        tracker = UploadProgressTracker(progress_msg, os.path.basename(document_path))
        
        await client.send_document(
            chat_id=chat_id,
            document=document_path,
            caption=caption,
            progress=tracker.progress_callback
        )
        
        logger.info(f"✅ Document uploaded: {document_path}")
        return True
        
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return False
    
    except Exception as e:
        logger.error(f"❌ Document upload error: {e}")
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
    """
    UNIVERSAL failed link handler
    Works for ALL file types - videos, images, documents
    """
    try:
        # Emoji based on file type
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
        
        logger.info(f"📧 Failed link sent for item #{serial_num} ({file_type})")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send link info: {e}")
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
    """
    Send file to destination channel/group
    Supports videos, photos, documents
    """
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
        logger.error(f"Destination upload error: {e}")
        return False
