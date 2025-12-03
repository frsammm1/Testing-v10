# 🚀 M3U8 Downloader Bot - HYDROGEN BOMB Edition v10.0

**The Most Powerful Telegram Bot with Revolutionary Features**

---

## 🔥 WHAT'S NEW IN v10.0

### 💣 HYDROGEN BOMB FEATURES

1. **🎯 Destination Channel Support**
   - Forward message to set destination
   - Send channel link or ID
   - Auto-upload to your channel/group

2. **✏️ Custom Caption Feature**
   - Add custom text to all captions
   - Skip if not needed
   - Flexible formatting

3. **🎨 Text Watermark on Thumbnails**
   - Add watermark text overlay
   - Positioned automatically
   - Customizable appearance

4. **⚙️ Advanced Quality Control**
   - REAL quality conversion (not just resolution)
   - FFmpeg-based re-encoding
   - Proper bitrate control

5. **📦 Smart 2GB+ File Handling**
   - Auto-detects large files
   - Intelligent splitting algorithm
   - Seamless multi-part uploads

6. **❌ Universal Failed Link Handler**
   - Works for ALL file types
   - Sends link + caption
   - User-friendly format

---

## 📊 PERFORMANCE COMPARISON

| Feature | v9.0 | v10.0 |
|---------|------|-------|
| Download Speed | 6-7x | **6-7x** ✅ |
| Quality Control | Resolution only | **Full Re-encoding** ⭐ |
| 2GB+ Files | Basic splitting | **Smart Splitting** ⭐ |
| Failed Links | Video only | **All Types** ⭐ |
| Destination | ❌ | **✅ Channels/Groups** ⭐ |
| Custom Caption | ❌ | **✅ Full Support** ⭐ |
| Watermarks | ❌ | **✅ Thumbnail Text** ⭐ |

---

## 🎯 FEATURES

### 🚀 Ultra-Fast Downloads
- 6-7x speed boost
- Dynamic worker management (8-32)
- Adaptive connection pooling
- Real-time progress tracking

### 📦 Smart File Handling
- Auto-splits files over 2GB
- Intelligent part sizing
- Memory-efficient processing
- Seamless multi-part uploads

### 🎬 Advanced Video Processing
- Real quality conversion with FFmpeg
- Proper bitrate control
- Multiple thumbnail methods
- Text watermark support

### 🎯 Destination Management
- Set destination channel/group
- Forward message to set
- Use channel link or ID
- Persistent settings

### ✏️ Customization
- Add custom captions
- Text watermarks on thumbnails
- Skip options available
- Flexible configuration

### ❌ Error Handling
- Universal failed link handler
- Works for videos, images, documents
- Sends link + explanation
- User-friendly messages

---

## 📝 USAGE GUIDE

### Step 1: Start Bot
```
/start
```

### Step 2: Set Destination (Optional)
**Method 1: Forward Message**
- Forward any message from your channel
- Bot automatically detects destination

**Method 2: Send Link**
```
t.me/yourchannel
```

**Method 3: Send ID**
```
-1001234567890
```

**Clear Destination:**
```
/clear
```

### Step 3: Send TXT/HTML File
Format:
```
Title 1: https://example.com/video.m3u8
Title 2: https://example.com/image.jpg
Title 3: https://example.com/doc.pdf
```

### Step 4: Choose Options

**Range Selection:**
- All items: Click "Download All"
- Specific range: Click "Select Range"
  - Examples: `1-50`, `10-20`, `5`

**Custom Caption:**
- Add custom text: Type your caption
- Skip: Send `/skip`

**Text Watermark:**
- Add watermark: Type your text
- Skip: Send `/skip`

### Step 5: Select Quality
- 360p - Good for mobile
- 480p - Balanced quality
- 720p ⭐ - Recommended
- 1080p 🔥 - Best quality

### Step 6: Watch Magic Happen! ✨

---

## 🔧 INSTALLATION

### Prerequisites
- Python 3.11+
- FFmpeg
- Telegram Bot Token
- API ID & Hash

### Local Setup

```bash
git clone <your-repo>
cd m3u8-hydrogen-bomb
pip install -r requirements.txt

export API_ID="your_api_id"
export API_HASH="your_api_hash"
export BOT_TOKEN="your_bot_token"
export PORT="10000"

python main.py
```

### Docker Deployment

```bash
docker build -t m3u8-hydrogen .
docker run -d \
  -e API_ID="your_api_id" \
  -e API_HASH="your_api_hash" \
  -e BOT_TOKEN="your_bot_token" \
  -e PORT="10000" \
  -p 10000:10000 \
  m3u8-hydrogen
```

### Render Deployment

1. Fork repository
2. Create Web Service on Render
3. Environment: Docker
4. Add environment variables:
   - `API_ID`
   - `API_HASH`
   - `BOT_TOKEN`
   - `PORT` = 10000
5. Deploy!

---

## ⚙️ CONFIGURATION

### Quality Settings (config.py)

```python
QUALITY_SETTINGS = {
    "720p": {
        "height": 720,
        "bitrate": "2500k",
        "audio_bitrate": "192k",
        "preset": "medium"
    }
}
```

### File Splitting

```python
TELEGRAM_FILE_LIMIT = 2000  # MB
SAFE_SPLIT_SIZE = 1950      # MB per part
```

### Watermark Settings

```python
WATERMARK_ENABLED = True
WATERMARK_POSITION = "bottom_right"
WATERMARK_FONT_SIZE = 48
WATERMARK_COLOR = "white"
WATERMARK_OPACITY = 0.8
```

---

## 📊 ADVANCED FEATURES

### Dynamic Workers
- Starts at 8 workers
- Auto-adjusts to 32 based on speed
- Reduces if network slows
- Optimal performance guaranteed

### Smart Quality Control
- Detects original resolution
- Re-encodes if needed
- Proper bitrate control
- FFmpeg-based conversion

### Intelligent File Splitting
- Auto-detects large files
- Calculates optimal parts
- Buffer for safety
- Numbered parts

### Failed Link Handler
- Detects all failures
- YouTube links
- Unsupported platforms
- Network errors
- Sends formatted message

---

## 🎨 CUSTOMIZATION

### Watermark Positions
- `top_left`
- `top_right`
- `bottom_left`
- `bottom_right` (default)
- `center`

### Caption Format
```
{serial}. {title}

{custom_caption}
```

Example:
```
1. My Video

Downloaded by @MyBot
```

---

## 🔍 TROUBLESHOOTING

### Large Files Not Uploading?
✅ Auto-splitting enabled
- Files split at 1.95GB
- Each part uploads separately
- Progress tracked per part

### Quality Not Changing?
✅ Real conversion enabled
- FFmpeg re-encodes video
- Proper bitrate control
- Takes longer but works

### Failed Link Not Sent?
✅ Universal handler active
- Works for all types
- Sends link + caption
- Check destination settings

### Destination Not Working?
✅ Check these:
- Bot is admin in channel
- Channel ID is correct
- Use /clear and try again

---

## 📈 PERFORMANCE TIPS

### Maximum Speed
1. Stable internet connection
2. Process in batches of 50-100
3. Use 720p (optimal balance)
4. Let workers auto-adjust

### Best Quality
1. Use 1080p setting
2. Allow conversion time
3. Check file sizes
4. Verify bitrate

### Large Files
1. Auto-splitting handles it
2. Each part shows progress
3. No manual intervention
4. Works for 10GB+ files

---

## 🏆 KEY IMPROVEMENTS FROM v9.0

✅ **NO FEATURES REMOVED** - All v9.0 features kept
✅ **DESTINATION CHANNELS** - Upload to any channel
✅ **CUSTOM CAPTIONS** - Add your branding
✅ **TEXT WATERMARKS** - Brand your thumbnails
✅ **REAL QUALITY CONTROL** - Actual re-encoding
✅ **SMART 2GB+ HANDLING** - Perfect splitting logic
✅ **UNIVERSAL FAILED LINKS** - All types supported
✅ **BETTER UX** - Step-by-step process

---

## 📝 SUPPORTED FORMATS

### Videos
M3U8, MPD, MP4, MKV, AVI, MOV, FLV, WMV, WEBM, TS

### Images
PNG, JPG, JPEG, GIF, BMP, WEBP, SVG

### Documents
PDF, DOC, DOCX, TXT, ZIP, RAR

---

## 🎯 COMMANDS

- `/start` - Show features
- `/destination` - Set destination channel
- `/clear` - Clear destination
- `/cancel` - Stop downloads

---

## 🔐 SECURITY

- No data logging
- Secure SSL connections
- Temporary file cleanup
- Session isolation
- Safe file handling

---

## 📜 LICENSE

MIT License - Free to use and modify

---

## 🤝 CONTRIBUTING

Contributions welcome!
1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Open Pull Request

---

## 💬 SUPPORT

- Open GitHub issue
- Check troubleshooting
- Review documentation

---

## 🎉 CREDITS

**Made with ⚡ by HYDROGEN BOMB Team**

🚀 Powered by:
- Pyrogram
- FFmpeg
- yt-dlp
- aiohttp

---

**🔥 HYDROGEN BOMB EDITION v10.0 - THE ULTIMATE BOT! 🔥**

*Zero compromises. Maximum power. Pure perfection.*

---

## 📊 CHANGELOG

### v10.0 - HYDROGEN BOMB Edition
- ✅ Destination channel support
- ✅ Custom caption feature
- ✅ Text watermark on thumbnails
- ✅ Advanced quality control
- ✅ Smart 2GB+ handling
- ✅ Universal failed link handler
- ✅ Better UI/UX flow

### v9.0 - ULTRA SPEED Edition
- ✅ 6-7x speed boost
- ✅ Upload progress bars
- ✅ Auto file splitting
- ✅ YouTube link support
- ✅ Enhanced thumbnails

### v8.0 - SUPERCHARGED
- ✅ 3-4x speed boost
- ✅ Modular structure
- ✅ Better error handling

---

**END OF DOCUMENTATION**
