#file_processing/categorizer.py:


import os
import shutil
from typing import Dict, Any, Optional
from pathlib import Path

from utils.media_identifier import MediaFilenameParser
fr

class MediaCategorizer:
    """
    Advanced media file categorization and organization utility.
    """
    def __init__(
        self, 
        base_dir: str,
        movie_dir: Optional[str] = None,
        tv_dir: Optional[str] = None,
        unmatched_dir: Optional[str] = None
    ):
        """
        Initialize media categorizer.
        
        Args:
            base_dir: Base directory for media organization
            movie_dir: Directory for movies
            tv_dir: Directory for TV shows
            unmatched_dir: Directory for unidentified media
        """
        self.base_dir = Path(base_dir)
        self.movie_dir = Path(movie_dir or os.path.join(base_dir, 'Movies'))
        self.tv_dir = Path(tv_dir or os.path.join(base_dir, 'TV Shows'))
        self.unmatched_dir = Path(unmatched_dir or os.path.join(base_dir, 'Unmatched'))
        
        # Create directories if they don't exist
        for directory in [self.movie_dir, self.tv_dir, self.unmatched_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def categorize(self, filepath):
        """
        Categorize a media file.
        
        Args:
            filepath: Path to the file (string or Path object)
        
        Returns:
            Categorized file path
        """
        # Convert to Path if it's a string
        filepath = Path(filepath)
        
        # Your existing categorization logic
        # For example:
        if not filepath.is_file():
            raise ValueError(f"Not a valid file: {filepath}")
        
        # Implement your categorization logic here
        # This is just a placeholder - replace with your actual logic
        filename = filepath.name
        
        # Example categorization (you'll want to implement more sophisticated logic)
        if any(pattern in filename.lower() for pattern in ['s01', 's02', 'season']):
            # TV Show
            category_dir = Path(self.base_dir) / 'TV Shows'
        else:
            # Movie
            category_dir = Path(self.base_dir) / 'Movies'
        
        # Create category directory if it doesn't exist
        category_dir.mkdir(parents=True, exist_ok=True)
        
        # Move or copy file
        new_path = category_dir / filename
        filepath.rename(new_path)
        
        return str(new_path)
    
    def _categorize_movie(
        self, 
        file_path: Path, 
        metadata: Dict[str, Any]
    ) -> Path:
        """
        Categorize movie files.
        
        Args:
            file_path: Original file path
            metadata: Movie metadata
            
        Returns:
            Destination file path
        """
        title = metadata.get('title', 'Unknown')
        year = metadata.get('year', 'Unknown')
        
        # Create year-based subdirectory
        year_dir = self.movie_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate new filename
        new_filename = f"{title} ({year}){file_path.suffix}"
        destination = year_dir / self._sanitize_filename(new_filename)
        
        # Move file
        shutil.move(str(file_path), str(destination))
        return destination
    
    def _categorize_tv_show(
        self, 
        file_path: Path, 
        metadata: Dict[str, Any]
    ) -> Path:
        """
        Categorize TV show files.
        
        Args:
            file_path: Original file path
            metadata: TV show metadata
            
        Returns:
            Destination file path
        """
        show_name = metadata.get('show_name', 'Unknown')
        season = metadata.get('season', 0)
        episode = metadata.get('episode', 0)
        
        # Create show and season subdirectories
        show_dir = self.tv_dir / self._sanitize_filename(show_name)
        season_dir = show_dir / f"Season {season}"
        show_dir.mkdir(parents=True, exist_ok=True)
        season_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate new filename
        new_filename = f"{show_name} - S{season:02d}E{episode:02d}{file_path.suffix}"
        destination = season_dir / self._sanitize_filename(new_filename)
        
        # Move file
        shutil.move(str(file_path), str(destination))
        return destination
    
    def _categorize_unmatched(
        self, 
        file_path: Path
    ) -> Path:
        """
        Categorize unidentified media files.
        
        Args:
            file_path: Original file path
            
        Returns:
            Destination file path
        """
        destination = self.unmatched_dir / file_path.name
        shutil.move(str(file_path), str(destination))
        return destination
    
    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """
        Sanitize filename by removing invalid characters.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename
        """
        import re
        
        # Remove invalid filename characters
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        
        # Replace multiple spaces with single space
        filename = re.sub(r'\s+', ' ', filename).strip()
        
        return filename