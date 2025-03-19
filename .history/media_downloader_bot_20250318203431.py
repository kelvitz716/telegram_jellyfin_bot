#!/usr/bin/env python3
import os
import logging
import time
import math
import asyncio
from telethon import TelegramClient, events
from telethon.tl import types
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import NetworkError, TimedOut

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Constants
DOWNLOAD_PATH = "/home/kelvitz/Videos/Telegram/Downloads"
BOT_TOKEN = "7717230409:AAEtWjrSMPrQVFQRD9TQj97tjcz0tvJ6R4Q"
API_ID = 29626522  # Your Telegram API ID (get from my.telegram.org)
API_HASH = "df5c5933c2f024278cfa1613661685e7"  # Your Telegram API hash (get from my.telegram.org)

# Ensure download directory exists
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

# Create Telethon client for large downloads
telethon_client = None

def format_size(size_bytes):
    """Format bytes to human-readable size."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    await update.message.reply_text('Hi! Send me any media file and I will download it.')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    await update.message.reply_text('Send me any media file and I will download it for you.')

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle media messages by forwarding to Telethon client."""
    try:
        # Get the message ID and chat ID
        message_id = update.message.message_id
        chat_id = update.effective_chat.id
        
        # Acknowledge receipt
        status_message = await update.message.reply_text(
            "Media received! Starting download with Telethon..."
        )
        
        # Forward to telethon client for processing
        # We need to create a task that will handle the download using Telethon
        asyncio.create_task(
            process_with_telethon(chat_id, message_id, status_message.message_id, context.bot)
        )
        
    except Exception as e:
        logger.error(f"Error handling media: {str(e)}")
        await update.message.reply_text(f"Error processing media: {str(e)}")

async def process_with_telethon(chat_id, message_id, status_message_id, bot):
    """Process the message using Telethon client."""
    global telethon_client
    
    try:
        # Get the message from Telethon
        message = await telethon_client.get_messages(chat_id, ids=message_id)
        
        if not message or not message.media:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message_id,
                text="No media found in the message."
            )
            return
        
        # Determine file name and size
        file_name = ""
        if isinstance(message.media, types.MessageMediaDocument):
            for attr in message.media.document.attributes:
                if isinstance(attr, types.DocumentAttributeFilename):
                    file_name = attr.file_name
                    break
            if not file_name:
                file_name = f"document_{message.id}.{message.media.document.mime_type.split('/')[-1]}"
            file_size = message.media.document.size
        elif isinstance(message.media, types.MessageMediaPhoto):
            file_name = f"photo_{message.id}.jpg"
            file_size = 0  # Size is not directly available for photos
        else:
            file_name = f"media_{message.id}"
            file_size = 0
        
        # Sanitize filename
        file_name = "".join(c for c in file_name if c.isalnum() or c in "._- ")
        file_path = os.path.join(DOWNLOAD_PATH, file_name)
        
        # Update status
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_message_id,
            text=f"Starting download of {file_name}"
        )
        
        # Progress callback
        last_update_time = time.time()
        progress_bar = ""
        
        async def progress_callback(downloaded_bytes, total_bytes):
            nonlocal last_update_time, progress_bar
            
            # Update progress only every 1 second to avoid flooding
            current_time = time.time()
            if current_time - last_update_time > 1:
                progress = min(downloaded_bytes / total_bytes, 1.0) if total_bytes > 0 else 0
                bar_length = 20
                filled_length = int(bar_length * progress)
                progress_bar = '█' * filled_length + '░' * (bar_length - filled_length)
                
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message_id,
                        text=f"Downloading: [{progress_bar}] {progress:.1%}\n"
                            f"{format_size(downloaded_bytes)}/{format_size(total_bytes)}"
                    )
                except Exception as e:
                    logger.error(f"Error updating progress: {str(e)}")
                
                last_update_time = current_time
        
        # Download the file with Telethon
        try:
            downloaded_path = await message.download_media(
                file=file_path,
                progress_callback=progress_callback
            )
            
            if downloaded_path:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_message_id,
                    text=f"Download complete!\nFile: {file_name}\nSize: {format_size(os.path.getsize(downloaded_path))}"
                )
            else:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_message_id,
                    text="Download failed. Please try again."
                )
        except Exception as e:
            logger.error(f"Telethon download error: {str(e)}")
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message_id,
                text=f"Download error: {str(e)}"
            )
    except Exception as e:
        logger.error(f"Error in Telethon processing: {str(e)}")
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message_id,
                text=f"Error: {str(e)}"
            )
        except:
            pass

async def initialize_telethon():
    """Initialize the Telethon client."""
    global telethon_client
    
    # Create the Telethon client
    telethon_client = TelegramClient('media_downloader_session', API_ID, API_HASH)
    
    # Start the client and authorize with bot token
    await telethon_client.start(bot_token=BOT_TOKEN)
    logger.info("Telethon client started")

async def shutdown_telethon():
    """Properly disconnect the Telethon client."""
    global telethon_client
    if telethon_client:
        await telethon_client.disconnect()
        logger.info("Telethon client disconnected")

async def main():
    """Main entry point of the bot."""
    # Initialize the Telethon client
    await initialize_telethon()
    
    # Create the application
    app = Application(
        token=BOT_TOKEN,
        context_type=ContextTypes.DEFAULT_TYPE
    )

    # Register handlers
    # Command handlers
    # app.add_handler(CommandHandler("start", start))  