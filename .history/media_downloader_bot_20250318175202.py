#!/usr/bin/env python3
import os
import logging
import time
import math
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import NetworkError, TimedOut
from urllib3.exceptions import ProtocolError
import aiohttp
import asyncio

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Constants
DOWNLOAD_PATH = "/home/kelvitz/Videos/Telegram/Downloads"
MAX_TELEGRAM_FILE_SIZE = 40 * 1024 * 1024  # 40MB - Telegram Bot API limit
CHUNK_SIZE = 1024 * 1024  # 1MB for chunk download

# Ensure download directory exists
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    await update.message.reply_text('Hi! Send me any media file and I will download it.')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    await update.message.reply_text('Send me any media file and I will download it for you.')

async def download_file(url, file_path, file_size, update):
    """Download file with progress tracking, handling network issues and resuming."""
    headers = {}
    temp_path = f"{file_path}.part"
    
    # Check if partial download exists
    if os.path.exists(temp_path):
        existing_size = os.path.getsize(temp_path)
        if existing_size < file_size:
            headers['Range'] = f'bytes={existing_size}-'
            mode = 'ab'  # Append binary mode
            downloaded = existing_size
            status_message = await update.message.reply_text(
                f"Resuming download from {format_size(existing_size)}/{format_size(file_size)}"
            )
        else:
            mode = 'wb'  # Write binary mode
            downloaded = 0
            status_message = await update.message.reply_text("Starting download...")
    else:
        mode = 'wb'  # Write binary mode
        downloaded = 0
        status_message = await update.message.reply_text("Starting download...")
    
    progress_bar = ""
    last_update_time = time.time()
    retry_count = 0
    max_retries = 5
    
    async with aiohttp.ClientSession() as session:
        while retry_count < max_retries:
            try:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if not response.ok:
                        await update.message.reply_text(f"Download failed: HTTP error {response.status}")
                        return False
                    
                    total_size = int(response.headers.get('content-length', 0))
                    if headers.get('Range'):
                        total_size += downloaded
                    
                    with open(temp_path, mode) as f:
                        async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                # Update progress only every 1 second to avoid flooding
                                current_time = time.time()
                                if current_time - last_update_time > 1:
                                    progress = min(downloaded / file_size, 1.0) if file_size > 0 else 0
                                    bar_length = 20
                                    filled_length = int(bar_length * progress)
                                    progress_bar = '█' * filled_length + '░' * (bar_length - filled_length)
                                    
                                    await status_message.edit_text(
                                        f"Downloading: [{progress_bar}] {progress:.1%}\n"
                                        f"{format_size(downloaded)}/{format_size(file_size)}"
                                    )
                                    last_update_time = current_time
                
                # Successful download
                os.rename(temp_path, file_path)
                await status_message.edit_text(f"Download complete: {os.path.basename(file_path)}")
                return True
                
            except (aiohttp.ClientError, asyncio.TimeoutError, ProtocolError) as e:
                retry_count += 1
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.error(f"Download error: {e}. Retrying in {wait_time}s... ({retry_count}/{max_retries})")
                await status_message.edit_text(
                    f"Network error: {str(e)[:50]}...\n"
                    f"Retrying in {wait_time}s... ({retry_count}/{max_retries})\n"
                    f"Last progress: [{progress_bar}] {downloaded}/{file_size} bytes"
                )
                await asyncio.sleep(wait_time)
                
                # Prepare for resuming
                if os.path.exists(temp_path):
                    existing_size = os.path.getsize(temp_path)
                    headers['Range'] = f'bytes={existing_size}-'
                    mode = 'ab'
                    downloaded = existing_size
        
        # Max retries reached
        await status_message.edit_text("Download failed after multiple attempts.")
        return False

def format_size(size_bytes):
    """Format bytes to human-readable size."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

async def handle_large_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle files larger than Telegram's bot API limit using file_id and get_file."""
    file_id = None
    file_name = None
    file_size = 0
    
    # Extract file information based on message type
    if update.message.document:
        file_id = update.message.document.file_id
        file_name = update.message.document.file_name
        file_size = update.message.document.file_size
    elif update.message.video:
        file_id = update.message.video.file_id
        file_name = f"video_{file_id[-10:]}.mp4"
        file_size = update.message.video.file_size
    elif update.message.audio:
        file_id = update.message.audio.file_id
        file_name = update.message.audio.file_name or f"audio_{file_id[-10:]}.mp3"
        file_size = update.message.audio.file_size
    elif update.message.voice:
        file_id = update.message.voice.file_id
        file_name = f"voice_{file_id[-10:]}.ogg"
        file_size = update.message.voice.file_size
    elif update.message.photo:
        # Get the largest photo
        photo = update.message.photo[-1]
        file_id = photo.file_id
        file_name = f"photo_{file_id[-10:]}.jpg"
        file_size = photo.file_size
    elif update.message.animation:
        file_id = update.message.animation.file_id
        file_name = f"animation_{file_id[-10:]}.mp4"
        file_size = update.message.animation.file_size
    elif update.message.video_note:
        file_id = update.message.video_note.file_id
        file_name = f"video_note_{file_id[-10:]}.mp4"
        file_size = update.message.video_note.file_size
    else:
        await update.message.reply_text("Unrecognized media type")
        return
    
    # Sanitize filename
    file_name = "".join(c for c in file_name if c.isalnum() or c in "._- ")
    
    # Get file info
    try:
        file_info = await context.bot.get_file(file_id)
        file_url = file_info.file_path
        
        # Acknowledge receipt
        await update.message.reply_text(
            f"Received file: {file_name}\n"
            f"Size: {format_size(file_size)}\n"
            f"Starting download..."
        )
        
        # Download the file
        file_path = os.path.join(DOWNLOAD_PATH, file_name)
        success = await download_file(file_url, file_path, file_size, update)
        
        if success:
            await update.message.reply_text(
                f"File saved to: {file_path}\n"
                f"Size: {format_size(os.path.getsize(file_path))}"
            )
        else:
            await update.message.reply_text("Download failed. Please try again.")
    
    except NetworkError as e:
        await update.message.reply_text(f"Network error: {str(e)}")
    except TimedOut:
        await update.message.reply_text("Connection timed out. Please try again.")
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        await update.message.reply_text(f"Error: {str(e)}")

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all media messages."""
    try:
        await handle_large_file(update, context)
    except Exception as e:
        logger.error(f"Error handling media: {str(e)}")
        await update.message.reply_text(f"Error processing media: {str(e)}")

def main() -> None:
    """Start the bot."""
    # Create the Application instance
    application = Application.builder().token("YOUR_BOT_TOKEN").build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    
    # Media file handlers
    application.add_handler(MessageHandler(
        filters.PHOTO | filters.VIDEO | filters.DOCUMENT | filters.AUDIO | 
        filters.VOICE | filters.ANIMATION | filters.VIDEO_NOTE,
        handle_media
    ))

    # Start the Bot
    application.run_polling()

if __name__ == '__main__':
    main()