#services/notification_service.py:

import asyncio
import aiohttp
from typing import Optional, Dict, Any
from dataclasses import dataclass, field

from ..config.configuration import ConfigurationManager
from ..te.rate_limiter import AsyncRateLimiter

@dataclass
class NotificationConfig:
    """Configuration for notification services."""
    telegram_bot_token: str = ''
    telegram_chat_id: str = ''
    pushover_token: str = ''
    pushover_user: str = ''
    email_smtp_host: str = ''
    email_smtp_port: int = 587
    email_sender: str = ''
    email_password: str = ''

class NotificationService:
    """
    Multi-channel notification service with rate limiting.
    """
    def __init__(
        self, 
        config: Optional[NotificationConfig] = None,
        config_manager: Optional[ConfigurationManager] = None
    ):
        """
        Initialize notification service.
        
        Args:
            config: Notification configuration
            config_manager: Configuration manager
        """
        # Prioritize passed config, then config manager, then default
        if config_manager:
            self.config = NotificationConfig(
                telegram_bot_token=config_manager.get('telegram', 'bot_token', ''),
                telegram_chat_id=config_manager.get('telegram', 'chat_id', '')
            )
        else:
            self.config = config or NotificationConfig()
        
        # Rate limiters for different services
        self.rate_limiters = {
            'telegram': AsyncRateLimiter(interval=1.0),
            'pushover': AsyncRateLimiter(interval=1.0),
            'email': AsyncRateLimiter(interval=5.0)
        }
    
    async def send_telegram(
        self, 
        message: str, 
        parse_mode: str = 'HTML',
        disable_notification: bool = False
    ) -> bool:
        """
        Send a Telegram message.
        
        Args:
            message: Message text
            parse_mode: Message parse mode
            disable_notification: Disable notification sound
            
        Returns:
            True if message sent successfully
        """
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            return False
        
        async def _send():
            url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    'chat_id': self.config.telegram_chat_id,
                    'text': message,
                    'parse_mode': parse_mode,
                    'disable_notification': disable_notification
                }) as response:
                    return await response.json()
        
        try:
            return await self.rate_limiters['telegram'].call(_send)
        except Exception as e:
            print(f"Telegram notification error: {e}")
            return False
    
    async def send_pushover(
        self, 
        message: str, 
        title: Optional[str] = None,
        priority: int = 0
    ) -> bool:
        """
        Send a Pushover notification.
        
        Args:
            message: Notification message
            title: Optional notification title
            priority: Notification priority
            
        Returns:
            True if message sent successfully
        """
        if not self.config.pushover_token or not self.config.pushover_user:
            return False
        
        async def _send():
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://api.pushover.net/1/messages.json',
                    data={
                        'token': self.config.pushover_token,
                        'user': self.config.pushover_user,
                        'message': message,
                        'title': title or 'Media Manager',
                        'priority': priority
                    }
                ) as response:
                    return await response.json()
        
        try:
            return await self.rate_limiters['pushover'].call(_send)
        except Exception as e:
            print(f"Pushover notification error: {e}")
            return False
    
    async def send_email(
        self, 
        subject: str, 
        body: str, 
        recipient: Optional[str] = None
    ) -> bool:
        """
        Send an email notification.
        
        Args:
            subject: Email subject
            body: Email body
            recipient: Optional recipient email
            
        Returns:
            True if email sent successfully
        """
        # Placeholder for email implementation
        # Would require adding SMTP library and configuration
        return False
    
    async def notify(
        self, 
        message: str, 
        channels: Optional[list] = None
    ) -> Dict[str, bool]:
        """
        Send notifications across multiple channels.
        
        Args:
            message: Notification message
            channels: List of channels to send to
            
        Returns:
            Dictionary of channel send statuses
        """
        channels = channels or ['telegram']
        results = {}
        
        for channel in channels:
            if channel == 'telegram':
                results['telegram'] = await self.send_telegram(message)
            elif channel == 'pushover':
                results['pushover'] = await self.send_pushover(message)
            elif channel == 'email':
                results['email'] = await self.send_email('Media Manager Notification', message)
        
        return results

# Convenience function for quick notifications
async def send_notification(
    message: str, 
    channels: Optional[list] = None,
    config_manager: Optional[ConfigurationManager] = None
) -> Dict[str, bool]:
    """
    Quick notification send utility.
    
    Args:
        message: Notification message
        channels: Notification channels
        config_manager: Optional configuration manager
        
    Returns:
        Dictionary of channel send statuses
    """
    service = NotificationService(config_manager=config_manager)
    return await service.notify(message, channels)