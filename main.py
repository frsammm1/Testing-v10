import logging
import asyncio
from aiohttp import web
from pyrogram import Client, idle
from config import API_ID, API_HASH, BOT_TOKEN, PORT
from handlers import setup_handlers

# Enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Suppress unnecessary logs
logging.getLogger('pyrogram').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)

# Initialize bot with ULTRA settings
app = Client(
    "m3u8_hydrogen_bomb_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=16,
    sleep_threshold=120,
    max_concurrent_transmissions=10
)

# Web server
web_app = web.Application()

async def health_check(request):
    return web.Response(
        text="✅ OK - HYDROGEN BOMB Edition v10.0 Running!",
        content_type="text/plain"
    )

async def stats(request):
    stats_text = """
🚀 M3U8 Downloader Bot - HYDROGEN BOMB Edition v10.0

⚡ ULTIMATE FEATURES:
✅ 6-7x Faster Downloads
✅ Smart 2GB+ File Handling
✅ Destination Channel Support
✅ Custom Caption Feature
✅ Text Watermark on Thumbnails
✅ Advanced Quality Control
✅ Universal Failed Link Handler
✅ Dynamic Worker Management
✅ Real-time Progress Tracking
✅ Auto File Splitting

💪 STATUS: ACTIVE & READY!
🔥 PERFORMANCE: MAXIMUM
    """
    return web.Response(text=stats_text, content_type="text/plain")

async def root(request):
    return web.Response(
        text="🚀 HYDROGEN BOMB Edition v10.0 is running!",
        content_type="text/plain"
    )

web_app.router.add_get("/", root)
web_app.router.add_get("/health", health_check)
web_app.router.add_get("/stats", stats)


async def main():
    """Main initialization"""
    try:
        # Start web server
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"✅ Web server started on port {PORT}")
        logger.info(f"📊 Health: http://0.0.0.0:{PORT}/health")
        logger.info(f"📈 Stats: http://0.0.0.0:{PORT}/stats")
        
        # Setup handlers
        setup_handlers(app)
        logger.info("✅ Handlers configured")
        
        # Start bot
        await app.start()
        
        me = await app.get_me()
        logger.info("=" * 70)
        logger.info(f"🤖 Bot: @{me.username}")
        logger.info(f"📝 Name: {me.first_name}")
        logger.info(f"🆔 ID: {me.id}")
        logger.info("=" * 70)
        logger.info("🚀 HYDROGEN BOMB EDITION v10.0 - ACTIVATED!")
        logger.info("=" * 70)
        logger.info("⚡ FEATURES ENABLED:")
        logger.info("   ✅ 6-7x Faster Downloads")
        logger.info("   ✅ Smart 2GB+ Handling")
        logger.info("   ✅ Destination Channels")
        logger.info("   ✅ Custom Captions")
        logger.info("   ✅ Text Watermarks")
        logger.info("   ✅ Quality Control")
        logger.info("   ✅ Failed Link Handler")
        logger.info("   ✅ Dynamic Workers (8-32)")
        logger.info("   ✅ Progress Tracking")
        logger.info("   ✅ Auto File Splitting")
        logger.info("=" * 70)
        
        await idle()
        
    except Exception as e:
        logger.error(f"❌ Startup error: {e}", exc_info=True)
        raise
    finally:
        try:
            await app.stop()
            logger.info("🛑 Bot stopped gracefully")
        except:
            pass


if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("🚀 INITIALIZING M3U8 DOWNLOADER BOT")
    logger.info("💣 HYDROGEN BOMB EDITION v10.0")
    logger.info("=" * 70)
    logger.info("")
    logger.info("💪 PERFORMANCE ENHANCEMENTS:")
    logger.info("   ✓ 6-7x Download Speed")
    logger.info("   ✓ Dynamic Worker Adjustment")
    logger.info("   ✓ Smart 2GB+ Handling")
    logger.info("   ✓ Real Quality Control")
    logger.info("")
    logger.info("🎯 NEW FEATURES:")
    logger.info("   ✓ Destination Channels")
    logger.info("   ✓ Custom Captions")
    logger.info("   ✓ Text Watermarks")
    logger.info("   ✓ Universal Failed Links")
    logger.info("   ✓ Advanced Quality Control")
    logger.info("")
    logger.info("=" * 70)
    
    try:
        app.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
    finally:
        logger.info("=" * 70)
        logger.info("👋 Shutdown complete")
        logger.info("=" * 70)
