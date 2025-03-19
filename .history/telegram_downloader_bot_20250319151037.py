# First try Telethon for larger files if available
use_telethon = (self.client is not None and 
    self.client.is_connected() and 
    file_size > 20 * 1024 * 1024 and 
    original_message)
