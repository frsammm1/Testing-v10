import os
import asyncio
import logging
import time
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


def find_all_parts(base_path: str) -> List[str]:
    """
    🔍 Find all parts of a split file
    Handles ffmpeg-generated parts: filename_part001_of_003.mp4
    """
    base_file = Path(base_path)
    
    # Check if this is a part file
    if '_part' in base_file.stem and '_of_' in base_file.stem:
        # Extract base name before _part
        parts = base_file.stem.split('_part')
        base_name = parts[0]
        ext = base_file.suffix
        
        # Find all matching parts
        pattern = f"{base_name}_part*_of_*{ext}"
        all_parts = sorted(DOWNLOAD_DIR.glob(pattern))
        
        if len(all_parts) > 1:
            logger.info(f"🔍 Found {len(all_parts)} parts:")
            for p in all_parts:
                logger.info(f"   → {p.name}")
            return [str(p) for p in all_parts]
    
    # Check if parts exist without opening this file
    # Pattern: basename_part001_of_NNN.ext
    base_name = base_file.stem
    ext = base_file.suffix
    pattern = f"{base_name}_part*_of_*{ext}"
    possible_parts = sorted(DOWNLOAD_DIR.glob(pattern))
    
    if possible_parts:
        logger.info(f"🔍 Found {len(possible_parts)} split parts")
        return [str(p) for p in possible_parts]
    
    # Single file - no parts
    if os.path.exists(base_path):
        logger.info(f"📦 Single file: {base_file.name}")
        return [base_path]
    
    logger.warning(f"⚠️ No file found: {base_path}")
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
    🚀 SMART video upload with ffmpeg-split detection
    Automatically detects and uploads all parts
    """
    try:
        # Find all parts (if file was split)
        all_parts = find_all_parts(video_path)
        
        if not all_parts:
            logger.error("❌ No files found to upload!")
            await progress_msg.edit_text("❌ File not found!")
            return False
        
        # Single file upload
        if len(all_parts) == 1:
            file_path = all_parts[0]
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            logger.info(f"📹 Single file: {file_size_mb:.2f}MB")
            
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
                
                logger.info(f"✅ Video uploaded successfully")
                
                # Cleanup
                try:
                    os.remove(file_path)
                    if thumb_path and os.path.exists(thumb_path):
                        os.remove(thumb_path)
                except:
                    pass
                
                return True
                
            except FloodWait as e:
                logger.warning(f"⏳ FloodWait {e.value}s")
                await asyncio.sleep(e.value)
                # Retry once
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
        
        # Multi-part upload
        logger.info(f"📦 Multi-part upload: {len(all_parts)} parts")
        
        # Calculate total size
        total_size = sum(os.path.getsize(p) for p in all_parts if os.path.exists(p))
        total_size_mb = total_size / (1024 * 1024)
        
        await progress_msg.edit_text(
            f"📦 **MULTI-PART UPLOAD**\n\n"
            f"Parts detected: {len(all_parts)}\n"
            f"Total size: {total_size_mb:.1f}MB\n\n"
            f"Uploading each part...\n"
            f"⏳ Please wait..."
        )
        
        # Upload each part
        uploaded_count = 0
        
        for i, part_path in enumerate(all_parts, 1):
            if not os.path.exists(part_path):
                logger.warning(f"⚠️ Part {i} not found: {part_path}")
                continue
            
            part_size = os.path.getsize(part_path) / (1024 * 1024)
            part_name = os.path.basename(part_path)
            
            # Build caption for this part
            part_caption = f"{caption}\n\n📦 **Part {i}/{len(all_parts)}**\n💾 Size: {part_size:.1f}MB"
            
            logger.info(f"📤 Uploading part {i}/{len(all_parts)}: {part_name}")
            
            tracker = UploadProgressTracker(
                progress_msg, 
                part_name,
                i, 
                len(all_parts)
            )
            
            # Upload with retry logic
            retry_count = 0
            max_retries = 3
            upload_success = False
            
            while retry_count < max_retries and not upload_success:
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
                    
                    upload_success = True
                    uploaded_count += 1
                    logger.info(f"✅ Part {i}/{len(all_parts)} uploaded successfully!")
                    
                except FloodWait as e:
                    logger.warning(f"⏳ FloodWait {e.value}s for part {i}")
                    await asyncio.sleep(e.value)
                    retry_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Part {i} upload error (attempt {retry_count+1}): {e}")
                    retry_count += 1
                    
                    if retry_count < max_retries:
                        logger.info(f"🔄 Retrying part {i} in 5s...")
                        await asyncio.sleep(5)
                    else:
                        logger.error(f"❌ Part {i} failed after {max_retries} attempts")
                        # Continue with next part instead of failing completely
                        break
            
            # Cleanup this part after upload
            try:
                if upload_success and os.path.exists(part_path):
                    os.remove(part_path)
                    logger.info(f"🗑️ Cleaned up part {i}")
            except Exception as e:
                logger.debug(f"Cleanup error: {e}")
            
            # Small delay between parts
            await asyncio.sleep(1)
        
        # Cleanup thumbnail
        try:
            if thumb_path and os.path.exists(thumb_path):
                os.remove(thumb_path)
        except:
            pass
        
        # Summary
        if uploaded_count == len(all_parts):
            await progress_msg.edit_text(
                f"✅ **ALL PARTS UPLOADED!**\n\n"
                f"📦 Total Parts: {len(all_parts)}\n"
                f"💾 Total Size: {total_size_mb:.1f}MB\n"
                f"✔️ Uploaded: {uploaded_count}/{len(all_parts)}\n\n"
                f"🎉 Upload Complete!"
            )
            return True
        else:
            await progress_msg.edit_text(
                f"⚠️ **PARTIAL UPLOAD**\n\n"
                f"✔️ Uploaded: {uploaded_count}/{len(all_parts)}\n"
                f"❌ Failed: {len(all_parts) - uploaded_count}\n\n"
                f"Some parts could not be uploaded."
            )
            return False
        
    except Exception as e:
        logger.error(f"❌ Video upload error: {e}", exc_info=True)
        await progress_msg.edit_text(f"❌ Upload failed: {str(e)[:100]}")
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
        if not os.path.exists(photo_path):
            logger.error(f"Photo not found: {photo_path}")
            return False
        
        tracker = UploadProgressTracker(progress_msg, os.path.basename(photo_path))
        
        await client.send_photo(
            chat_id=chat_id,
            photo=photo_path,
            caption=caption,
            progress=tracker.progress_callback
        )
        
        logger.info(f"✅ Photo uploaded: {photo_path}")
        
        # Cleanup
        try:
            os.remove(photo_path)
        except:
            pass
        
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
    📄 Smart document upload with multi-part support
    """
    try:
        # Find all parts
        all_parts = find_all_parts(document_path)
        
        if not all_parts:
            logger.error("❌ Document not found!")
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
            
            # Cleanup
            try:
                os.remove(file_path)
            except:
                pass
            
            return True
        
        # Multi-part document
        logger.info(f"📦 Multi-part document: {len(all_parts)} parts")
        
        uploaded_count = 0
        
        for i, part_path in enumerate(all_parts, 1):
            if not os.path.exists(part_path):
                continue
            
            part_size = os.path.getsize(part_path) / (1024 * 1024)
            part_caption = f"{caption}\n\n📦 Part {i}/{len(all_parts)} ({part_size:.1f}MB)"
            
            tracker = UploadProgressTracker(
                progress_msg,
                os.path.basename(part_path),
                i,
                len(all_parts)
            )
            
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
                    logger.info(f"✅ Document part {i}/{len(all_parts)} uploaded")
                    
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    retry_count += 1
                    
                except Exception as e:
                    logger.error(f"Part {i} error: {e}")
                    retry_count += 1
                    if retry_count < 3:
                        await asyncio.sleep(5)
            
            # Cleanup
            try:
                if upload_success and os.path.exists(part_path):
                    os.remove(part_path)
            except:
                pass
            
            await asyncio.sleep(1)
        
        return uploaded_count == len(all_parts)
        
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
    ❌ UNIVERSAL failed link handler
    Works for ALL file types
    """
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
    🎯 Send file to destination with smart multi-part handling
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
