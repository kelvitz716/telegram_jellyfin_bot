#services/api_clients.py:

import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from ..config.configuration import ConfigurationManager
from ..utils.rate_limiter import AsyncRateLimiter

@dataclass
class MediaApiConfig:
    """Configuration for media API clients."""
    tmdb_api_key: str = ''
    omdb_api_key: str = ''
    tvdb_api_key: str = ''

class BaseMediaApiClient:
    """
    Base class for media metadata API clients.
    Provides common functionality and rate limiting.
    """
    def __init__(
        self, 
        config: Optional[MediaApiConfig] = None,
        config_manager: Optional[ConfigurationManager] = None
    ):
        """
        Initialize media API client.
        
        Args:
            config: API configuration
            config_manager: Configuration manager
        """
        if config_manager:
            self.config = MediaApiConfig(
                tmdb_api_key=config_manager.get('tmdb', 'api_key', ''),
                omdb_api_key=config_manager.get('omdb', 'api_key', ''),
                tvdb_api_key=config_manager.get('tvdb', 'api_key', '')
            )
        else:
            self.config = config or MediaApiConfig()
        
        # Rate limiter for API calls
        self.rate_limiter = AsyncRateLimiter(interval=0.5)  # 2 requests per second
    
    async def _make_request(
        self, 
        url: str, 
        params: Optional[Dict[str, Any]] = None, 
        method: str = 'GET'
    ) -> Optional[Dict[str, Any]]:
        """
        Generic async HTTP request method with rate limiting.
        
        Args:
            url: Request URL
            params: Query parameters
            method: HTTP method
            
        Returns:
            JSON response or None
        """
        async def _execute_request():
            async with aiohttp.ClientSession() as session:
                try:
                    if method.upper() == 'GET':
                        async with session.get(url, params=params) as response:
                            return await response.json()
                    elif method.upper() == 'POST':
                        async with session.post(url, json=params) as response:
                            return await response.json()
                except Exception as e:
                    print(f"API Request Error: {e}")
                    return None
        
        return await self.rate_limiter.call(_execute_request)

class TMDbClient(BaseMediaApiClient):
    """
    Client for The Movie Database (TMDb) API.
    """
    BASE_URL = 'https://api.themoviedb.org/3'
    
    async def search_movie(
        self, 
        query: str, 
        year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for a movie.
        
        Args:
            query: Movie title
            year: Release year
            
        Returns:
            List of matching movies
        """
        if not self.config.tmdb_api_key:
            return []
        
        url = f"{self.BASE_URL}/search/movie"
        params = {
            'api_key': self.config.tmdb_api_key,
            'query': query,
            'year': year
        }
        
        response = await self._make_request(url, params)
        return response.get('results', []) if response else []
    
    async def search_tv_show(
        self, 
        query: str, 
        year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for a TV show.
        
        Args:
            query: TV show title
            year: First air year
            
        Returns:
            List of matching TV shows
        """
        if not self.config.tmdb_api_key:
            return []
        
        url = f"{self.BASE_URL}/search/tv"
        params = {
            'api_key': self.config.tmdb_api_key,
            'query': query,
            'first_air_date_year': year
        }
        
        response = await self._make_request(url, params)
        return response.get('results', []) if response else []

class OMDbClient(BaseMediaApiClient):
    """
    Client for Open Movie Database (OMDb) API.
    """
    BASE_URL = 'http://www.omdbapi.com/'
    
    async def search_movie(
        self, 
        query: str, 
        year: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Search for a movie using OMDb.
        
        Args:
            query: Movie title
            year: Release year
            
        Returns:
            Movie details or None
        """
        if not self.config.omdb_api_key:
            return None
        
        params = {
            'apikey': self.config.omdb_api_key,
            't': query,
            'y': year,
            'type': 'movie'
        }
        
        return await self._make_request(self.BASE_URL, params)

class MediaIdentifier:
    """
    Centralized media identification service.
    Combines multiple API clients for comprehensive metadata retrieval.
    """
    def __init__(
        self, 
        config_manager: Optional[ConfigurationManager] = None
    ):
        """
        Initialize media identifier with multiple API clients.
        
        Args:
            config_manager: Configuration manager
        """
        self.tmdb_client = TMDbClient(config_manager=config_manager)
        self.omdb_client = OMDbClient(config_manager=config_manager)
    
    async def identify_media(
        self, 
        filename: str
    ) -> Dict[str, Any]:
        """
        Attempt to identify media from filename.
        
        Args:
            filename: Media filename
            
        Returns:
            Media metadata dictionary
        """
        # Extract potential title and year from filename
        title, year = self._parse_filename(filename)
        
        # Try movie identification
        movie_results = await self.tmdb_client.search_movie(title, year)
        if movie_results:
            return self._format_movie_result(movie_results[0])
        
        # Try TV show identification
        tv_results = await self.tmdb_client.search_tv_show(title, year)
        if tv_results:
            return self._format_tv_result(tv_results[0])
        
        return {}
    
    def _parse_filename(self, filename: str) -> tuple:
        """
        Parse filename to extract title and year.
        
        Args:
            filename: Media filename
            
        Returns:
            (title, year) tuple
        """
        # Implement sophisticated filename parsing logic
        # This is a simplified example
        import re
        
        # Remove file extension
        filename = filename.rsplit('.', 1)[0]
        
        # Try to extract year
        year_match = re.search(r'\((\d{4})\)', filename)
        year = int(year_match.group(1)) if year_match else None
        
        # Remove year and clean title
        title = re.sub(r'\(.*?\)', '', filename).strip()
        
        return title, year
    
    def _format_movie_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Format TMDb movie result."""
        return {
            'type': 'movie',
            'title': result.get('title', ''),
            'year': result.get('release_date', '')[:4] if result.get('release_date') else None,
            'overview': result.get('overview', ''),
            'poster_path': f"https://image.tmdb.org/t/p/w500{result.get('poster_path')}" if result.get('poster_path') else None
        }
    
    def _format_tv_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Format TMDb TV show result."""
        return {
            'type': 'tv',
            'title': result.get('name', ''),
            'year': result.get('first_air_date', '')[:4] if result.get('first_air_date') else None,
            'overview': result.get('overview', ''),
            'poster_path': f"https://image.tmdb.org/t/p/w500{result.get('poster_path')}" if result.get('poster_path') else None
        }

# Example usage
async def main():
    identifier = MediaIdentifier()
    result = await identifier.identify_media("The Matrix (1999).mkv")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())