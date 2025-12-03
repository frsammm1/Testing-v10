import os
import asyncio
import aiofiles
import logging
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from config import DOWNLOAD_DIR, QUALITY_SETTINGS
from utils import (
    parse_content, sanitize_filename, is_youtube_url, 
    is_unsupported_platform, extract_channel_id,
    save_destination_channel, get_destination_channel, 
    clear_destination_channel
)
from video_processor import (
    get_video_info, generate_thumbnail_with_text, 
    validate_video_file, convert_video_quality
)
from downloader import download_video, download_file
from uploader import (
    upload_video, upload_photo, upload_document, 
    send_failed_link, send_to_destination
)

logger = logging.getLogger(__name__)

# Global state
user_data = {}
active_downloads = {}
download_progress = {}


def setup_handlers(app: Client):
    """Setup all bot handlers with HYDROGEN BOMB features"""
    
    @app.on_message(filters.command("start"))
    async def start_cmd(client: Client, message: Message):
        await message.reply_text(
            "🚀 **M3U8 Downloader Bot - HYDROGEN BOMB Edition v10.0**\n\n"
            "⚡ **ULTIMATE FEATURES:**\n"
            "🔥 6-7x Faster Downloads\n"
            "📊 Real-time Progress Tracking\n"
            "📦 Smart 2GB+ File Handling\n"
            "🎯 Destination Channel Support\n"
            "✏️ Custom Caption Feature\n"
            "🎨 Text Watermark on Thumbnails\n"
            "⚙️ Advanced Quality Control\n"
            "❌ Universal Failed Link Handler\n"
            "💪 Dynamic Worker Management\n\n"
            "🎥 **Supported Formats:**\n"
            "• Videos: M3U8, MPD, MP4, MKV, AVI, MOV, etc.\n"
            "• Images: PNG, JPG, GIF, WEBP, etc.\n"
            "• Documents: PDF, DOC, TXT, ZIP, etc.\n\n"
            "📝 **How to Use:**\n"
            "1. Send TXT/HTML file with links\n"
            "2. Set destination (optional)\n"
            "3. Add custom caption (optional)\n"
            "4. Add text watermark (optional)\n"
            "5. Select quality & download!\n\n"
            "🔧 **Commands:**\n"
            "/start - Show this message\n"
            "/cancel - Stop all downloads\n"
            "/destination - Set destination channel\n"
            "/clear - Clear destination\n\n"
            "🚀 **Powered by HYDROGEN BOMB Engine!**"
        )
    
    
    @app.on_message(filters.command("destination"))
    async def destination_cmd(client: Client, message: Message):
        await message.reply_text(
            "🎯 **Set Destination Channel/Group**\n\n"
            "Send me ONE of these:\n\n"
            "1️⃣ **Forward any message** from your channel\n"
            "2️⃣ **Send channel link** (e.g., t.me/yourchannel)\n"
            "3️⃣ **Send channel ID** (e.g., -1001234567890)\n\n"
            "💡 Make sure I'm admin in that channel!"
        )
    
    
    @app.on_message(filters.command("clear"))
    async def clear_cmd(client: Client, message: Message):
        user_id = message.from_user.id
        await clear_destination_channel(user_id)
        await message.reply_text("✅ Destination channel cleared!")
    
    
    @app.on_message(filters.forwarded)
    async def handle_forward(client: Client, message: Message):
        """Handle forwarded messages to set destination"""
        user_id = message.from_user.id
        
        if message.forward_from_chat:
            channel_id = message.forward_from_chat.id
            channel_name = message.forward_from_chat.title or "Unknown"
            
            await save_destination_channel(user_id, channel_id, channel_name)
            
            await message.reply_text(
                f"✅ **Destination Set!**\n\n"
                f"📢 Channel: {channel_name}\n"
                f"🆔 ID: `{channel_id}`\n\n"
                f"All files will be sent here now!"
            )
    
    
    @app.on_message(filters.document)
    async def handle_doc(client: Client, message: Message):
        user_id = message.from_user.id
        file_name = message.document.file_name
        
        if not (file_name.endswith('.txt') or file_name.endswith('.html')):
            await message.reply_text("❌ Please send TXT or HTML file only!")
            return
        
        status = await message.reply_text("📥 Processing your file...")
        
        try:
            file_path = await message.download(file_name=f"{DOWNLOAD_DIR}/{user_id}_{file_name}")
            
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
            
            items = parse_content(content)
            
            if not items:
                await status.edit_text("❌ No supported links found in file!")
                os.remove(file_path)
                return
            
            # Count by type
            type_counts = {}
            for item in items:
                ftype = item['type']
                type_counts[ftype] = type_counts.get(ftype, 0) + 1
            
            user_data[user_id] = {
                'items': items, 
                'file_path': file_path,
                'step': 'select_range'
            }
            
            # Check if destination is set
            destination = await get_destination_channel(user_id)
            dest_info = ""
            if destination:
                dest_info = f"\n🎯 Destination: {destination[1] or destination[0]}\n"
            
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Select Range", callback_data="select_range")],
                [InlineKeyboardButton("⬇️ Download All", callback_data="download_all")]
            ])
            
            type_info = "\n".join([
                f"{'🎬' if t == 'video' else '🖼️' if t == 'image' else '📄'} {t.title()}s: {c}" 
                for t, c in type_counts.items()
            ])
            
            await status.edit_text(
                f"✅ **Content Detected!**\n\n"
                f"{type_info}\n"
                f"📦 Total: {len(items)}{dest_info}\n\n"
                f"🚀 Choose your action:",
                reply_markup=kb
            )
            
        except Exception as e:
            logger.error(f"Document processing error: {e}")
            await status.edit_text(f"❌ Error: {str(e)[:100]}")
    
    
    @app.on_callback_query(filters.regex(r"^(select_range|download_all)$"))
    async def range_select(client: Client, callback: CallbackQuery):
        user_id = callback.from_user.id
        action = callback.data
        
        if user_id not in user_data:
            await callback.answer("❌ Session expired!", show_alert=True)
            return
        
        items = user_data[user_id]['items']
        
        if action == "download_all":
            user_data[user_id]['range'] = (1, len(items))
            user_data[user_id]['step'] = 'ask_caption'
            
            await callback.message.edit_text(
                f"✏️ **Custom Caption (Optional)**\n\n"
                f"📦 Processing {len(items)} items\n\n"
                f"Do you want to add custom text to captions?\n\n"
                f"**Options:**\n"
                f"• Send your custom text (will be added to each caption)\n"
                f"• Send `/skip` to skip this step\n\n"
                f"Example: 'Downloaded by @MyBot'\n"
                f"This will be added after default caption."
            )
        else:
            user_data[user_id]['step'] = 'range_input'
            await callback.message.edit_text(
                f"📊 **Range Selection**\n\n"
                f"Total: {len(items)} items\n\n"
                f"📝 Send range:\n"
                f"• `1-50` → Items 1 to 50\n"
                f"• `10-20` → Items 10 to 20\n"
                f"• `5` → Only item 5\n\n"
                f"⏳ Waiting..."
            )
    
    
    @app.on_message(filters.text & filters.private)
    async def handle_text_input(client: Client, message: Message):
        user_id = message.from_user.id
        text = message.text.strip()
        
        if user_id not in user_data:
            # Check if it's a channel link/ID
            if text.startswith('t.me/') or text.startswith('-') or text.isdigit():
                channel_id = extract_channel_id(text)
                if channel_id:
                    await save_destination_channel(user_id, channel_id, text)
                    await message.reply_text(
                        f"✅ **Destination Set!**\n\n"
                        f"🆔 ID: `{channel_id}`\n"
                        f"Use /clear to remove it."
                    )
            return
        
        step = user_data[user_id].get('step', '')
        items = user_data[user_id]['items']
        
        # Handle range input
        if step == 'range_input':
            try:
                if '-' in text:
                    start, end = map(int, text.split('-'))
                else:
                    start = end = int(text)
                
                if start < 1 or end > len(items) or start > end:
                    await message.reply_text(
                        f"❌ Invalid! Valid: 1-{len(items)}"
                    )
                    return
                
                user_data[user_id]['range'] = (start, end)
                user_data[user_id]['step'] = 'ask_caption'
                
                count = end - start + 1
                await message.reply_text(
                    f"✅ **Range: {start}-{end}** ({count} items)\n\n"
                    f"✏️ **Custom Caption (Optional)**\n\n"
                    f"Send custom text or `/skip` to continue."
                )
                
            except:
                await message.reply_text("❌ Invalid format! Use: `1-10` or `5`")
        
        # Handle custom caption
        elif step == 'ask_caption':
            if text.lower() == '/skip':
                user_data[user_id]['custom_caption'] = ""
                user_data[user_id]['step'] = 'ask_watermark'
                
                await message.reply_text(
                    f"✏️ **Text Watermark (Optional)**\n\n"
                    f"Add text overlay on video thumbnails?\n\n"
                    f"**Options:**\n"
                    f"• Send your watermark text\n"
                    f"• Send `/skip` to skip\n\n"
                    f"Example: '@MyChannel' or 'My Brand'\n"
                    f"Text will appear on thumbnails."
                )
            else:
                user_data[user_id]['custom_caption'] = text
                user_data[user_id]['step'] = 'ask_watermark'
                
                await message.reply_text(
                    f"✅ **Caption Saved!**\n\n"
                    f"Preview: `{text[:50]}...`\n\n"
                    f"✏️ **Text Watermark (Optional)**\n\n"
                    f"Add watermark on thumbnails?\n"
                    f"Send text or `/skip`"
                )
        
        # Handle watermark text
        elif step == 'ask_watermark':
            if text.lower() == '/skip':
                user_data[user_id]['watermark_text'] = ""
            else:
                user_data[user_id]['watermark_text'] = text
            
            user_data[user_id]['step'] = 'select_quality'
            
            kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("360p", callback_data="q_360p"),
                    InlineKeyboardButton("480p", callback_data="q_480p")
                ],
                [
                    InlineKeyboardButton("720p ⭐", callback_data="q_720p"),
                    InlineKeyboardButton("1080p 🔥", callback_data="q_1080p")
                ]
            ])
            
            start, end = user_data[user_id]['range']
            caption_preview = user_data[user_id].get('custom_caption', 'None')
            watermark_preview = user_data[user_id].get('watermark_text', 'None')
            
            await message.reply_text(
                f"⚙️ **Final Configuration**\n\n"
                f"📊 Range: {start}-{end}\n"
                f"✏️ Caption: {caption_preview[:30]}...\n"
                f"🎨 Watermark: {watermark_preview[:20]}...\n\n"
                f"🎬 **Select Video Quality:**",
                reply_markup=kb
            )
    
    
    @app.on_callback_query(filters.regex(r"^q_"))
    async def quality_cb(client: Client, callback: CallbackQuery):
        user_id = callback.from_user.id
        quality = callback.data.split("_")[1]
        
        if user_id not in user_data or 'range' not in user_data[user_id]:
            await callback.answer("❌ Session expired!", show_alert=True)
            return
        
        items = user_data[user_id]['items']
        file_path = user_data[user_id]['file_path']
        start, end = user_data[user_id]['range']
        custom_caption = user_data[user_id].get('custom_caption', '')
        watermark_text = user_data[user_id].get('watermark_text', '')
        
        selected_items = items[start-1:end]
        active_downloads[user_id] = True
        
        # Get destination
        destination = await get_destination_channel(user_id)
        dest_id = destination[0] if destination else callback.message.chat.id
        dest_name = destination[1] if destination and destination[1] else "This Chat"
        
        stop_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("⛔ Stop All", callback_data="stop")
        ]])
        
        await callback.message.edit_text(
            f"🚀 **HYDROGEN BOMB ACTIVATED!**\n\n"
            f"⚡ Quality: {quality}\n"
            f"📊 Range: {start}-{end} ({len(selected_items)} items)\n"
            f"🎯 Destination: {dest_name}\n"
            f"✏️ Custom Caption: {'✅' if custom_caption else '❌'}\n"
            f"🎨 Watermark: {'✅' if watermark_text else '❌'}\n\n"
            f"💪 Processing at maximum speed...",
            reply_markup=stop_kb
        )
        
        # Process batch
        await process_batch(
            client, callback.message, selected_items, 
            quality, start, end, user_id, dest_id,
            custom_caption, watermark_text
        )
        
        # Cleanup
        cleanup_user_data(user_id, file_path)
    
    
    @app.on_callback_query(filters.regex("^stop$"))
    async def stop_cb(client: Client, callback: CallbackQuery):
        user_id = callback.from_user.id
        active_downloads[user_id] = False
        await callback.answer("⛔ Stopping...", show_alert=True)
    
    
    @app.on_message(filters.command("cancel"))
    async def cancel_cmd(client: Client, message: Message):
        user_id = message.from_user.id
        active_downloads[user_id] = False
        await message.reply_text("⛔ All downloads cancelled!")


async def process_batch(
    client: Client,
    message: Message,
    items: list,
    quality: str,
    start: int,
    end: int,
    user_id: int,
    destination_id: int,
    custom_caption: str = "",
    watermark_text: str = ""
):
    """Process batch with COMPLETE error handling"""
    success = 0
    failed = 0
    skipped = 0
    
    for idx, item in enumerate(items, start):
        if not active_downloads.get(user_id, False):
            await message.reply_text("⛔ Stopped by user!")
            break
        
        prog = await message.reply_text(
            f"📦 **Item {idx}/{end}**\n"
            f"📝 {item['title'][:50]}...\n"
            f"🚀 Processing..."
        )
        
        try:
            # Build caption
            base_caption = f"{idx}. {item['title']}"
            if custom_caption:
                base_caption += f"\n\n{custom_caption}"
            
            # Check for unsupported platforms
            if is_youtube_url(item['url']) or is_unsupported_platform(item['url']):
                platform = "YouTube" if is_youtube_url(item['url']) else "Social Media"
                await prog.edit_text(f"🎬 {platform} detected! Sending link...")
                
                await send_failed_link(
                    client, destination_id, item['title'],
                    item['url'], idx, 
                    f"{platform} link - Open manually",
                    item['type']
                )
                skipped += 1
                await prog.delete()
                continue
            
            # Process by type
            if item['type'] == 'video':
                result = await process_video(
                    client, message, item, quality,
                    base_caption, idx, prog, user_id,
                    destination_id, watermark_text
                )
            elif item['type'] == 'image':
                result = await process_image(
                    client, message, item, base_caption,
                    idx, prog, user_id, destination_id
                )
            elif item['type'] == 'document':
                result = await process_document(
                    client, message, item, base_caption,
                    idx, prog, user_id, destination_id
                )
            else:
                result = False
            
            if result == 'FAILED':
                await send_failed_link(
                    client, destination_id, item['title'],
                    item['url'], idx, 
                    "Processing failed - Check link manually",
                    item['type']
                )
                failed += 1
            elif result:
                success += 1
            else:
                failed += 1
        
        except Exception as e:
            logger.error(f"Item {idx} error: {e}", exc_info=True)
            try:
                await prog.delete()
                await send_failed_link(
                    client, destination_id, item['title'],
                    item['url'], idx, f"Error: {str(e)[:50]}",
                    item['type']
                )
            except:
                pass
            failed += 1
        
        await asyncio.sleep(0.3)
    
    # Summary
    await message.reply_text(
        f"✅ **BATCH COMPLETE!**\n\n"
        f"✔️ Success: {success}\n"
        f"❌ Failed: {failed}\n"
        f"⏭️ Skipped: {skipped}\n"
        f"📊 Total: {len(items)}\n"
        f"📍 Range: {start}-{end}\n\n"
        f"🚀 HYDROGEN BOMB Engine delivered!"
    )


async def process_video(
    client: Client, message: Message, item: dict,
    quality: str, caption: str, idx: int,
    prog: Message, user_id: int, dest_id: int,
    watermark: str = ""
) -> bool:
    """Process video with quality conversion & watermark"""
    try:
        safe = sanitize_filename(item['title'])
        fname = f"{safe}_{idx}.mp4"
        
        # Download
        vpath = await download_video(
            item['url'], quality, fname, prog,
            user_id, active_downloads, download_progress
        )
        
        if vpath == 'UNSUPPORTED':
            return 'FAILED'
        
        if not vpath or not validate_video_file(vpath):
            return 'FAILED'
        
        # Get info
        await prog.edit_text("🎬 Analyzing...")
        video_info = get_video_info(vpath)
        
        # Check if quality conversion needed
        quality_height = QUALITY_SETTINGS[quality]['height']
        if video_info['height'] != quality_height:
            await prog.edit_text(f"⚙️ Converting to {quality}...")
            conv_path = str(DOWNLOAD_DIR / f"conv_{fname}")
            
            success = await convert_video_quality(
                vpath, conv_path, quality
            )
            
            if success:
                os.remove(vpath)
                vpath = conv_path
                video_info = get_video_info(vpath)
        
        # Generate thumbnail with watermark
        thumb_path = str(DOWNLOAD_DIR / f"thumb_{user_id}_{idx}.jpg")
        has_thumb = generate_thumbnail_with_text(
            vpath, thumb_path, watermark, video_info['duration']
        )
        
        # Upload
        await prog.edit_text("📤 Uploading...")
        upload_success = await send_to_destination(
            client, dest_id, vpath, caption, 'video',
            prog, thumb_path if has_thumb else None,
            video_info['duration'], video_info['width'], video_info['height']
        )
        
        # Cleanup
        try:
            os.remove(vpath)
            if has_thumb and os.path.exists(thumb_path):
                os.remove(thumb_path)
        except:
            pass
        
        await prog.delete()
        return upload_success
        
    except Exception as e:
        logger.error(f"Video error: {e}")
        return False


async def process_image(
    client: Client, message: Message, item: dict,
    caption: str, idx: int, prog: Message,
    user_id: int, dest_id: int
) -> bool:
    """Process image"""
    try:
        safe = sanitize_filename(item['title'])
        ext = os.path.splitext(item['url'])[1] or '.jpg'
        fname = f"{safe}_{idx}{ext}"
        
        ipath = await download_file(
            item['url'], fname, prog, user_id, active_downloads
        )
        
        if not ipath:
            return 'FAILED'
        
        await prog.edit_text("📤 Uploading...")
        success = await send_to_destination(
            client, dest_id, ipath, caption, 'image', prog
        )
        
        try:
            os.remove(ipath)
        except:
            pass
        
        await prog.delete()
        return success
        
    except Exception as e:
        logger.error(f"Image error: {e}")
        return False


async def process_document(
    client: Client, message: Message, item: dict,
    caption: str, idx: int, prog: Message,
    user_id: int, dest_id: int
) -> bool:
    """Process document"""
    try:
        safe = sanitize_filename(item['title'])
        ext = os.path.splitext(item['url'])[1] or '.pdf'
        fname = f"{safe}_{idx}{ext}"
        
        dpath = await download_file(
            item['url'], fname, prog, user_id, active_downloads
        )
        
        if not dpath:
            return 'FAILED'
        
        await prog.edit_text("📤 Uploading...")
        success = await send_to_destination(
            client, dest_id, dpath, caption, 'document', prog
        )
        
        try:
            os.remove(dpath)
        except:
            pass
        
        await prog.delete()
        return success
        
    except Exception as e:
        logger.error(f"Document error: {e}")
        return False


def cleanup_user_data(user_id: int, file_path: str):
    """Cleanup"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except:
        pass
    
    for pattern in [f"temp_{user_id}_*", f"thumb_{user_id}_*", f"*_part*", f"conv_*"]:
        for tf in DOWNLOAD_DIR.glob(pattern):
            try:
                os.remove(tf)
            except:
                pass
    
    if user_id in user_data:
        del user_data[user_id]
    if user_id in active_downloads:
        del active_downloads[user_id]
    if user_id in download_progress:
        del download_progress[user_id]
