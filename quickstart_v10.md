# 🚀 QUICK START GUIDE - HYDROGEN BOMB Edition v10.0

Get your bot running in 5 minutes!

---

## ⚡ FASTEST WAY - Render (Free)

### Step 1: Get Bot Credentials (2 min)

1. **Get Bot Token:**
   - Open Telegram → Search `@BotFather`
   - Send `/newbot`
   - Follow instructions
   - Copy token: `1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`

2. **Get API Credentials:**
   - Go to https://my.telegram.org
   - Login with your phone
   - Go to "API Development Tools"
   - Create new application
   - Copy `API_ID` and `API_HASH`

### Step 2: Deploy to Render (3 min)

1. **Fork Repository:**
   - Click "Fork" on GitHub
   - Wait for fork to complete

2. **Create Render Account:**
   - Go to https://render.com
   - Sign up (free)

3. **Create New Web Service:**
   - Click "New +"
   - Select "Web Service"
   - Connect your GitHub
   - Select forked repository
   - Choose "Docker" environment

4. **Add Environment Variables:**
   ```
   API_ID = your_api_id
   API_HASH = your_api_hash
   BOT_TOKEN = your_bot_token
   PORT = 10000
   ```

5. **Deploy!**
   - Click "Create Web Service"
   - Wait 5-10 minutes
   - Your bot is LIVE! 🎉

---

## 🐳 DOCKER WAY (Recommended for VPS)

### Prerequisites
- Docker installed
- Git installed

### Steps

```bash
# 1. Clone repository
git clone <your-repo-url>
cd m3u8-hydrogen-bomb

# 2. Build Docker image
docker build -t m3u8-hydrogen .

# 3. Run container
docker run -d \
  --name m3u8-bot \
  -e API_ID="your_api_id" \
  -e API_HASH="your_api_hash" \
  -e BOT_TOKEN="your_bot_token" \
  -e PORT="10000" \
  -p 10000:10000 \
  --restart unless-stopped \
  m3u8-hydrogen

# 4. Check logs
docker logs -f m3u8-bot
```

**Stop bot:**
```bash
docker stop m3u8-bot
```

**Start bot:**
```bash
docker start m3u8-bot
```

**Remove bot:**
```bash
docker rm -f m3u8-bot
```

---

## 💻 LOCAL WAY (For Development)

### Prerequisites
- Python 3.11+
- FFmpeg installed
- Git installed

### Installation

```bash
# 1. Clone repository
git clone <your-repo-url>
cd m3u8-hydrogen-bomb

# 2. Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set environment variables
export API_ID="your_api_id"
export API_HASH="your_api_hash"
export BOT_TOKEN="your_bot_token"
export PORT="10000"

# Windows users:
# set API_ID=your_api_id
# set API_HASH=your_api_hash
# set BOT_TOKEN=your_bot_token
# set PORT=10000

# 5. Run bot
python main.py
```

**Check if running:**
- Open browser: http://localhost:10000/health
- Should see: `✅ OK - HYDROGEN BOMB Edition v10.0 Running!`

---

## 🎯 FIRST TIME USAGE

### 1. Start Bot
Open Telegram → Search your bot → Click `/start`

### 2. Set Destination (Optional)
```
/destination
```
Then forward any message from your channel.

### 3. Prepare Your File
Create `links.txt`:
```
Video 1: https://example.com/video1.m3u8
Image 1: https://example.com/image1.jpg
Doc 1: https://example.com/file1.pdf
```

### 4. Upload File
Send `links.txt` to bot.

### 5. Follow Steps
1. Select range or download all
2. Add custom caption or `/skip`
3. Add watermark or `/skip`
4. Select quality
5. Watch magic! ✨

---

## 🔧 CONFIGURATION (Optional)

Edit `config.py`:

### Change Quality Settings
```python
QUALITY_SETTINGS = {
    "720p": {
        "height": 720,
        "bitrate": "2500k",  # Change this
        "audio_bitrate": "192k",  # Change this
        "preset": "medium"  # fast, medium, slow
    }
}
```

### Change Watermark Position
```python
WATERMARK_POSITION = "bottom_right"
# Options: top_left, top_right, bottom_left, bottom_right, center
```

### Change Watermark Style
```python
WATERMARK_FONT_SIZE = 48  # Bigger = larger text
WATERMARK_COLOR = "white"  # or "red", "blue", etc.
WATERMARK_OPACITY = 0.8  # 0.0 to 1.0
```

### Change File Split Size
```python
SAFE_SPLIT_SIZE = 1950  # MB per part
```

---

## 🐛 TROUBLESHOOTING

### Bot Not Starting?

**Check 1: Environment Variables**
```bash
echo $API_ID
echo $API_HASH
echo $BOT_TOKEN
```
Should not be empty!

**Check 2: Dependencies**
```bash
pip install -r requirements.txt --upgrade
```

**Check 3: FFmpeg**
```bash
ffmpeg -version
```
Should show version. If not:
```bash
# Ubuntu/Debian
sudo apt-get install ffmpeg

# MacOS
brew install ffmpeg

# Windows
Download from: https://ffmpeg.org/download.html
```

### Bot Started But Not Responding?

**Check logs:**
```bash
# Docker
docker logs m3u8-bot

# Local
tail -f bot.log
```

**Common issues:**
- Wrong bot token → Check @BotFather
- Bot not admin in destination channel
- Network firewall blocking

### Download Failing?

**Check:**
1. URL is valid and accessible
2. Format is supported
3. Server is online
4. Internet connection stable

**Fix:**
- Try different quality
- Check if YouTube (will send link)
- Wait and retry

### Upload Failing?

**For large files:**
- Auto-splitting should work
- Check destination channel permissions
- Ensure bot is admin

**For quality issues:**
- Allow more time for conversion
- Check FFmpeg logs
- Verify source quality

### Destination Not Working?

**Fix:**
1. Make bot admin in channel
2. Check channel ID is correct
3. Try `/clear` and set again
4. Forward message instead of link

---

## 📊 VERIFY INSTALLATION

### Health Check
```bash
curl http://localhost:10000/health
```
Should return: `✅ OK - HYDROGEN BOMB Edition v10.0 Running!`

### Stats Check
```bash
curl http://localhost:10000/stats
```
Should show feature list.

### Bot Check
1. Open Telegram
2. Search your bot
3. Send `/start`
4. Should see welcome message

---

## 🎯 PRODUCTION TIPS

### For 24/7 Operation
1. Use Docker with `--restart unless-stopped`
2. Deploy on Render/Railway/Fly.io
3. Set up monitoring
4. Regular backups of destination_channels.json

### For Best Performance
1. Use VPS with good internet
2. Enable dynamic workers
3. Use 720p for balance
4. Process in batches

### For Security
1. Don't share bot token
2. Keep repository private
3. Use environment variables
4. Regular updates

---

## 📝 COMMON COMMANDS

```bash
# Check if bot running
ps aux | grep python

# Kill bot
pkill -f main.py

# Restart bot
python main.py &

# Check disk space
df -h

# Check memory
free -h

# View recent logs
tail -100 bot.log
```

---

## 🆘 GET HELP

1. Check documentation: README.md
2. Review troubleshooting section
3. Check bot.log for errors
4. Open GitHub issue with:
   - Error message
   - Steps to reproduce
   - System info

---

## ✅ CHECKLIST

Before asking for help:

- [ ] Bot credentials correct?
- [ ] FFmpeg installed?
- [ ] Dependencies installed?
- [ ] Environment variables set?
- [ ] Bot started without errors?
- [ ] Health check working?
- [ ] Bot responding in Telegram?
- [ ] Checked logs for errors?

If all ✅, bot should work perfectly!

---

## 🎉 SUCCESS INDICATORS

Your bot is working if:

✅ Health check returns OK
✅ Bot responds to `/start`
✅ Can upload TXT file
✅ Can select range
✅ Can set destination
✅ Downloads progress shown
✅ Uploads progress shown
✅ Files appear in chat/channel

---

## 🚀 WHAT'S NEXT?

1. **Test Basic Features:**
   - Upload small file
   - Download 1-2 items
   - Verify quality

2. **Test Advanced Features:**
   - Set destination
   - Add custom caption
   - Add watermark
   - Download 10+ items

3. **Test Large Files:**
   - Upload 3GB+ file
   - Check splitting works
   - Verify all parts uploaded

4. **Optimize Settings:**
   - Adjust quality settings
   - Change watermark style
   - Tune performance

5. **Go Live:**
   - Deploy to production
   - Monitor performance
   - Enjoy! 🎉

---

**🔥 YOU'RE READY TO BLAST WITH HYDROGEN BOMB! 🔥**

**Need help? Check README.md for detailed documentation!**

---

END OF QUICKSTART GUIDE
