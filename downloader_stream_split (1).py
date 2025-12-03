import os
import ssl
import asyncio
import aiohttp
import aiofiles
import yt_dlp
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List
from pyrogram.types import Message
from config import (
    DOWNLOAD_DIR, CHUNK_SIZE, CONCURRENT_FRAGMENTS, 
    MAX_RETRIES, FRAGMENT_RETRIES, CONNECTION_TIMEOUT,
    HTTP_CHUNK_SIZE, BUFFER_SIZE, DYNAMIC_WORKERS,
    MIN_WORKERS, MAX_WORKERS, WORKER_ADJUST_THRESHOLD,
    CONNECTION_POOL_SIZE, CONNECTION_POOL_PER_HOST, 
    DNS_CACHE_TTL, QUALITY_SETTINGS, ENABLE_STREAM_SPLIT,
    STREAM_SPLIT_THRESHOLD, SPLIT_BUFFER_SIZE, SAFE_SPLIT_SIZE,
    MAX_MEMORY_BUFFER, PART_PROGRESS_UPDATE_INTERVAL
)
from utils import format_size, format_time, create_progress_bar

logger = logging.getLogger(__name__)


class DynamicWorkerManager:
    """Dynamic worker adjustment for optimal speed"""
    
    def __init__(self):
        self.current_workers = CONCURRENT_FRAGMENTS
        self.last_speed = 0
        self.last_adjust = time.time()
        self.speed_history = []
    
    def adjust_workers(self, current_speed: float):
        """Adjust workers based on speed"""
        if not DYNAMIC_WORKERS:
            return self.current_workers
        
        now = time.time()
        if now - self.last_adjust < WORKER_ADJUST_THRESHOLD:
            return self.current_workers
        
        self.speed_history.append(current_speed)
        if len(self.speed_history) > 5:
            self.speed_history.pop(0)
        
        avg_speed = sum(self.speed_history) / len(self.speed_history)
        
        if avg_speed > self.last_speed * 1.2 and self.current_workers < MAX_WORKERS:
            self.current_workers = min(self.current_workers + 4, MAX_WORKERS)
            logger.info(f"📈 Workers → {self.current_workers}")
        elif avg_speed < self.last_speed * 0.8 and self.current_workers > MIN_WORKERS:
            self.current_workers = max(self.current_workers - 2, MIN_WORKERS)
            logger.info(f"📉 Workers → {self.current_workers}")
        
        self.last_speed = avg_speed
        self.last_adjust = now
        return self.current_workers


worker_manager = DynamicWorkerManager()


class StreamingSplitDownloader:
    """
    🚀 ADVANCED: Downloads and splits simultaneously
    Never loads entire file in memory
    """
    
    def __init__(self, url: str, base_filename: str, user_id: int):
        self.url = url
        self.base_filename = base_filename
        self.user_id = user_id
        self.parts = []
        self.current_part = 1
        self.current_part_size = 0
        self.current_file = None
        self.total_downloaded = 0
        self.split_threshold_bytes = STREAM_SPLIT_THRESHOLD * 1024 * 1024
        self.part_max_bytes = SAFE_SPLIT_SIZE * 1024 * 1024
        self.total_size = 0
        self.start_time = time.time()
        
    def get_part_path(self, part_num: int) -> Path:
        """Get path for part file"""
        name, ext = os.path.splitext(self.base_filename)
        return DOWNLOAD_DIR / f"{name}_part{part_num:03d}{ext}"
    
    async def open_new_part(self):
        """Open new part file for writing"""
        if self.current_file:
            await self.current_file.close()
            logger.info(f"✅ Closed part {self.current_part}: {format_size(self.current_part_size)}")
        
        self.current_part += 1
        self.current_part_size = 0
        part_path = self.get_part_path(self.current_part)
        self.parts.append(str(part_path))
        self.current_file = await aiofiles.open(part_path, 'wb', buffering=SPLIT_BUFFER_SIZE)
        logger.info(f"📝 Opened part {self.current_part}: {part_path.name}")
    
    async def write_chunk(self, chunk: bytes):
        """Write chunk to current part, auto-switch if needed"""
        chunk_size = len(chunk)
        
        # Check if we need to start a new part
        if self.current_part_size + chunk_size > self.part_max_bytes and self.current_part_size > 0:
            logger.info(f"🔄 Part {self.current_part} full, switching...")
            await self.open_new_part()
        
        # Write to current part
        await self.current_file.write(chunk)
        self.current_part_size += chunk_size
        self.total_downloaded += chunk_size
    
    async def download_with_split(
        self, 
        progress_msg: Message, 
        active_downloads: Dict[int, bool]
    ) -> List[str]:
        """Main download method with streaming split"""
        try:
            # SSL setup
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')
            
            connector = aiohttp.TCPConnector(
                ssl=ssl_context,
                limit=CONNECTION_POOL_SIZE,
                limit_per_host=CONNECTION_POOL_PER_HOST,
                ttl_dns_cache=DNS_CACHE_TTL,
                force_close=False,
                enable_cleanup_closed=True,
                keepalive_timeout=300,
            )
            
            timeout = aiohttp.ClientTimeout(
                total=CONNECTION_TIMEOUT,
                connect=30,
                sock_read=120  # Increased for large files
            )
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                }
                
                async with session.get(self.url, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"HTTP {response.status}")
                        return []
                    
                    self.total_size = int(response.headers.get('content-length', 0))
                    size_mb = self.total_size / (1024 * 1024)
                    
                    # Check if splitting needed
                    needs_split = size_mb > STREAM_SPLIT_THRESHOLD
                    
                    if needs_split:
                        logger.info(f"🔪 Large file ({size_mb:.1f}MB), will split during download")
                        await progress_msg.edit_text(
                            f"📥 **SMART DOWNLOAD ACTIVE**\n\n"
                            f"Size: {format_size(self.total_size)}\n"
                            f"🔪 Splitting: ON\n"
                            f"📦 Part size: {SAFE_SPLIT_SIZE}MB\n\n"
                            f"Downloading..."
                        )
                    
                    # Open first part
                    part_path = self.get_part_path(self.current_part)
                    self.parts.append(str(part_path))
                    self.current_file = await aiofiles.open(part_path, 'wb', buffering=SPLIT_BUFFER_SIZE)
                    logger.info(f"📝 Started part 1: {part_path.name}")
                    
                    # Download with real-time splitting
                    last_update = time.time()
                    memory_buffer = bytearray()
                    
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        # Check cancellation
                        if not active_downloads.get(self.user_id, False):
                            if self.current_file:
                                await self.current_file.close()
                            # Cleanup all parts
                            for part in self.parts:
                                if os.path.exists(part):
                                    os.remove(part)
                            return []
                        
                        # Add to buffer
                        memory_buffer.extend(chunk)
                        
                        # Write buffer if it's getting large
                        if len(memory_buffer) >= MAX_MEMORY_BUFFER:
                            if needs_split:
                                await self.write_chunk(bytes(memory_buffer))
                            else:
                                await self.current_file.write(bytes(memory_buffer))
                                self.total_downloaded += len(memory_buffer)
                            memory_buffer.clear()
                        
                        # Update progress
                        now = time.time()
                        if now - last_update >= PART_PROGRESS_UPDATE_INTERVAL:
                            last_update = now
                            try:
                                await self._update_progress(progress_msg, needs_split)
                            except:
                                pass
                    
                    # Write remaining buffer
                    if memory_buffer:
                        if needs_split:
                            await self.write_chunk(bytes(memory_buffer))
                        else:
                            await self.current_file.write(bytes(memory_buffer))
                            self.total_downloaded += len(memory_buffer)
                        memory_buffer.clear()
                    
                    # Close final part
                    if self.current_file:
                        await self.current_file.close()
                        logger.info(f"✅ Closed final part {self.current_part}")
                    
                    # Verify all parts
                    verified_parts = []
                    for part in self.parts:
                        if os.path.exists(part) and os.path.getsize(part) > 1024:
                            verified_parts.append(part)
                        else:
                            logger.warning(f"⚠️ Part missing or too small: {part}")
                    
                    if not verified_parts:
                        logger.error("❌ No valid parts created!")
                        return []
                    
                    logger.info(f"🎉 Download complete! {len(verified_parts)} parts created")
                    return verified_parts
                    
        except asyncio.TimeoutError:
            logger.error("❌ Download timeout!")
            return []
        except Exception as e:
            logger.error(f"❌ Download error: {e}", exc_info=True)
            return []
        finally:
            if self.current_file:
                try:
                    await self.current_file.close()
                except:
                    pass
    
    async def _update_progress(self, progress_msg: Message, is_split: bool):
        """Update progress message"""
        percent = (self.total_downloaded / self.total_size * 100) if self.total_size > 0 else 0
        elapsed = time.time() - self.start_time
        speed = self.total_downloaded / elapsed if elapsed > 0 else 0
        eta = int((self.total_size - self.total_downloaded) / speed) if speed > 0 else 0
        bar = create_progress_bar(percent)
        
        if is_split:
            part_mb = self.current_part_size / (1024 * 1024)
            msg = (
                f"⚡ **DOWNLOADING + SPLITTING**\n\n"
                f"📦 Current Part: {self.current_part}\n"
                f"💾 Part Size: {part_mb:.1f}MB / {SAFE_SPLIT_SIZE}MB\n\n"
                f"{bar}\n\n"
                f"📊 Total: {format_size(self.total_downloaded)} / {format_size(self.total_size)}\n"
                f"🚀 Speed: {format_size(int(speed))}/s\n"
                f"⏱️ ETA: {format_time(eta)}"
            )
        else:
            msg = (
                f"⚡ **DOWNLOADING**\n\n"
                f"{bar}\n\n"
                f"📦 {format_size(self.total_downloaded)} / {format_size(self.total_size)}\n"
                f"🚀 {format_size(int(speed))}/s\n"
                f"⏱️ {format_time(eta)}"
            )
        
        await progress_msg.edit_text(msg)


async def download_file(
    url: str, 
    filename: str, 
    progress_msg: Message, 
    user_id: int,
    active_downloads: Dict[int, bool]
) -> Optional[str]:
    """
    ENHANCED file downloader with streaming split support
    """
    try:
        # Check if we should use streaming split
        if ENABLE_STREAM_SPLIT:
            downloader = StreamingSplitDownloader(url, filename, user_id)
            parts = await downloader.download_with_split(progress_msg, active_downloads)
            
            if not parts:
                return None
            
            # If only one part, return it directly
            if len(parts) == 1:
                return parts[0]
            
            # Multiple parts - return first part's path (handler will detect multiple)
            return parts[0]
        
        # Fallback to standard download
        filepath = DOWNLOAD_DIR / filename
        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')
        
        connector = aiohttp.TCPConnector(
            ssl=ssl_context,
            limit=CONNECTION_POOL_SIZE,
            limit_per_host=CONNECTION_POOL_PER_HOST,
            ttl_dns_cache=DNS_CACHE_TTL,
            force_close=False,
            enable_cleanup_closed=True,
            keepalive_timeout=300,
        )
        
        timeout = aiohttp.ClientTimeout(
            total=CONNECTION_TIMEOUT,
            connect=30,
            sock_read=60
        )
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            }
            
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    logger.error(f"HTTP {response.status}")
                    return None
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                start_time = time.time()
                last_update = 0
                update_threshold = 256 * 1024
                
                async with aiofiles.open(filepath, 'wb', buffering=BUFFER_SIZE) as f:
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        if not active_downloads.get(user_id, False):
                            if filepath.exists():
                                os.remove(filepath)
                            return None
                        
                        await f.write(chunk)
                        downloaded += len(chunk)
                        
                        if downloaded - last_update >= update_threshold:
                            last_update = downloaded
                            try:
                                percent = (downloaded / total_size * 100) if total_size > 0 else 0
                                elapsed = time.time() - start_time
                                speed = downloaded / elapsed if elapsed > 0 else 0
                                eta = int((total_size - downloaded) / speed) if speed > 0 else 0
                                bar = create_progress_bar(percent)
                                
                                await progress_msg.edit_text(
                                    f"⚡ **DOWNLOADING**\n\n"
                                    f"{bar}\n\n"
                                    f"📦 {format_size(downloaded)} / {format_size(total_size)}\n"
                                    f"🚀 {format_size(int(speed))}/s\n"
                                    f"⏱️ {format_time(eta)}"
                                )
                            except:
                                pass
                
                if filepath.exists() and filepath.stat().st_size > 1024:
                    return str(filepath)
                return None
                
    except Exception as e:
        logger.error(f"Download error: {e}")
        return None


def download_video_sync(
    url: str, 
    quality: str, 
    output_path: str, 
    user_id: int,
    active_downloads: Dict[int, bool],
    download_progress: Dict[int, dict]
) -> bool:
    """
    Video downloader with quality control
    """
    try:
        def progress_hook(d):
            if not active_downloads.get(user_id, False):
                raise Exception("Cancelled")
            
            if d['status'] == 'downloading':
                try:
                    total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                    downloaded = d.get('downloaded_bytes', 0)
                    speed = d.get('speed', 0) or 0
                    eta = d.get('eta', 0)
                    
                    if total > 0:
                        percent = (downloaded / total) * 100
                        workers = worker_manager.adjust_workers(speed)
                        
                        download_progress[user_id] = {
                            'percent': percent,
                            'downloaded': downloaded,
                            'total': total,
                            'speed': speed,
                            'eta': eta,
                            'workers': workers
                        }
                except:
                    pass
        
        quality_height = QUALITY_SETTINGS.get(quality, {}).get('height', 720)
        current_workers = worker_manager.current_workers
        
        ydl_opts = {
            'format': f'bestvideo[height<={quality_height}]+bestaudio/best[height<={quality_height}]/best',
            'format_sort': [f'res:{quality_height}', 'ext:mp4:m4a'],
            'outtmpl': output_path,
            'merge_output_format': 'mp4',
            'quiet': True,
            'no_warnings': True,
            'nocheckcertificate': True,
            'concurrent_fragment_downloads': current_workers,
            'retries': MAX_RETRIES,
            'fragment_retries': FRAGMENT_RETRIES,
            'skip_unavailable_fragments': True,
            'buffersize': BUFFER_SIZE,
            'http_chunk_size': HTTP_CHUNK_SIZE,
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-us,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
            },
            'postprocessors': [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }, {
                'key': 'FFmpegMetadata',
            }],
            'postprocessor_args': {
                'ffmpeg': [
                    '-c:v', 'libx264',
                    '-vf', f'scale=-2:{quality_height}',
                    '-c:a', 'aac',
                    '-movflags', '+faststart',
                    '-threads', '8',
                    '-preset', 'medium'
                ]
            },
            'progress_hooks': [progress_hook],
            'extractor_retries': MAX_RETRIES,
            'file_access_retries': MAX_RETRIES,
            'socket_timeout': 60,
            'hls_prefer_native': True,
            'external_downloader_args': [
                '-threads', '8',
                '-multiple-connections', str(current_workers)
            ],
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            if not active_downloads.get(user_id, False):
                return False
            
            logger.info(f"🚀 Downloading: {url} at {quality}")
            ydl.download([url])
            logger.info(f"✅ Download complete")
            return True
            
    except Exception as e:
        error_msg = str(e).lower()
        if 'youtube' in error_msg or 'unsupported' in error_msg:
            download_progress[user_id] = {
                'error': 'unsupported',
                'url': url
            }
        logger.error(f"Download error: {e}")
        return False


async def update_video_progress(
    progress_msg: Message, 
    user_id: int,
    download_progress: Dict[int, dict],
    active_downloads: Dict[int, bool]
):
    """Update video download progress"""
    last_percent = -1
    
    while active_downloads.get(user_id, False) and user_id in download_progress:
        try:
            prog = download_progress[user_id]
            
            if 'error' in prog:
                break
            
            percent = prog.get('percent', 0)
            
            if int(percent) - last_percent >= 2:
                last_percent = int(percent)
                
                downloaded = prog.get('downloaded', 0)
                total = prog.get('total', 0)
                speed = prog.get('speed', 0)
                eta = prog.get('eta', 0)
                workers = prog.get('workers', CONCURRENT_FRAGMENTS)
                
                bar = create_progress_bar(percent)
                
                await progress_msg.edit_text(
                    f"🎬 **VIDEO DOWNLOAD**\n\n"
                    f"{bar}\n\n"
                    f"📦 {format_size(downloaded)} / {format_size(total)}\n"
                    f"🚀 {format_size(int(speed))}/s\n"
                    f"⏱️ {format_time(int(eta))}\n"
                    f"💪 Workers: {workers}"
                )
                
        except:
            pass
        
        await asyncio.sleep(1)


async def download_video(
    url: str,
    quality: str,
    filename: str,
    progress_msg: Message,
    user_id: int,
    active_downloads: Dict[int, bool],
    download_progress: Dict[int, dict]
) -> Optional[str]:
    """Download video with quality control"""
    temp_name = f"temp_{user_id}_{filename.replace('.mp4', '')}"
    output_path = str(DOWNLOAD_DIR / temp_name)
    
    try:
        download_progress[user_id] = {'percent': 0}
        
        await progress_msg.edit_text("🚀 Starting download...")
        
        progress_task = asyncio.create_task(
            update_video_progress(progress_msg, user_id, download_progress, active_downloads)
        )
        
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(
            None,
            download_video_sync,
            url, quality, output_path, user_id, active_downloads, download_progress
        )
        
        if user_id in download_progress and 'error' in download_progress[user_id]:
            del download_progress[user_id]
            try:
                progress_task.cancel()
            except:
                pass
            return 'UNSUPPORTED'
        
        if user_id in download_progress:
            del download_progress[user_id]
        
        try:
            progress_task.cancel()
        except:
            pass
        
        if not success or not active_downloads.get(user_id, False):
            return None
        
        await progress_msg.edit_text("✅ Download complete!")
        
        # Find output file
        possible_files = []
        for ext in ['.mp4', '.mkv', '.webm', '.ts']:
            p = Path(output_path + ext)
            if p.exists() and p.stat().st_size > 10240:
                possible_files.append(p)
        
        for file in DOWNLOAD_DIR.glob(f"temp_{user_id}_*"):
            if file.is_file() and file.stat().st_size > 10240:
                possible_files.append(file)
        
        if not possible_files:
            return None
        
        output_file = max(possible_files, key=lambda p: p.stat().st_size)
        final_path = DOWNLOAD_DIR / filename
        
        if output_file != final_path:
            os.rename(output_file, final_path)
        else:
            final_path = output_file
        
        if final_path.exists() and final_path.stat().st_size > 10240:
            logger.info(f"✅ Video ready: {final_path}")
            return str(final_path)
        
        return None
        
    except Exception as e:
        logger.error(f"Video download error: {e}")
        if user_id in download_progress:
            del download_progress[user_id]
        return None
