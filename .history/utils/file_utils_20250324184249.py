#utils/file_utils.py:

import os
import hashlib
from typing import Optional

class FileUtils:
    """
    Utility class for file-related operations.
    """
    @staticmethod
    def generate_unique_filename(base_path: str) -> str:
        """
        Generate a unique filename by appending a counter if the file exists.
        
        Args:
            base_path: Original file path
            
        Returns:
            Unique file path
        """
        if not os.path.exists(base_path):
            return base_path

        base, ext = os.path.splitext(base_path)
        counter = 1
        while os.path.exists(f"{base}_{counter}{ext}"):
            counter += 1
        
        return f"{base}_{counter}{ext}"

    @staticmethod
    def calculate_file_hash(filepath: str, algorithm: str = 'sha256', chunk_size: int = 8192) -> Optional[str]:
        """
        Calculate file hash for integrity verification.
        
        Args:
            filepath: Path to the file
            algorithm: Hash algorithm (default: sha256)
            chunk_size: Size of chunks to read
            
        Returns:
            Hex digest of file hash, or None if file cannot be read
        """
        try:
            hash_obj = hashlib.new(algorithm)
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(chunk_size), b''):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except (IOError, OSError):
            return None

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """
        Sanitize filename by removing or replacing invalid characters.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename
        """
        # Replace or remove potentially problematic characters
        invalid_chars = r'<>:"/\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Trim leading/trailing spaces and periods
        return filename.strip('. ')