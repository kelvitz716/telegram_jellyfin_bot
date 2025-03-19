#!/usr/bin/env python3
"""
telethon_auth.py - Helper script to authenticate with Telegram API using Telethon
"""

import os
import json
import logging
import asyncio
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

# Configuration
CONFIG_FILE = "config.json"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("Telethon Auth")

async def authenticate():
    """Authenticate with Telegram API using Telethon."""
    
    # Load config
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"Config file not found: {CONFIG_FILE}")
        return False
    
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    
    # Get credentials
    api_id = config["telegram"]["api_id"]
    api_hash = config["telegram"]["api_hash"]
    
    # Create session file
    session_file = "telegram_downloader_session"
    
    # Create client
    client = TelegramClient(session_file, api_id, api_hash)
    
    # Connect
    await client.connect()
    
    # Check if already authorized
    if await client.is_user_authorized():
        logger.info("Already authorized")
        await client.disconnect()
        return True
    
    # Phone number login
    phone = input("Enter your phone number (with country code): ")
    
    try:
        logger.info("Sending code request...")
        await client.send_code_request(phone)
        
        # Get verification code
        code = input("Enter the verification code: ")
        
        try:
            await client.sign_in(phone, code)
        except SessionPasswordNeededError:
            # 2FA enabled
            password = input("Two-factor authentication is enabled. Please enter your password: ")
            await client.sign_in(password=password)
        
        logger.info("Authentication successful!")
        
        # Save session string
        session_string = client.session.save()
        logger.info(f"Session saved to {session_file}")
        
        await client.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        await client.disconnect()
        return False

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(authenticate())