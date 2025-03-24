@self.bot.message_handler(content_types=['document', 'video', 'audio'])
async def handle_media(message):
    # Check for media group (multiple files)
    media_group_id = message.media_group_id if hasattr(message, 'media_group_id') else None
    
    if media_group_id:
        # Handle media group...
        # Creates a batch tracker and queues all files
