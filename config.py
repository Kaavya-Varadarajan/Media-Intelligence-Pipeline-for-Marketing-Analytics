import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

class Config:
    # NewsAPI Configuration
    NEWS_API_KEY = os.getenv('NEWS_API_KEY', '4df13233289147e9a0095da1238f3e70')
    NEWS_API_BASE_URL = "https://newsapi.org/v2"
    NEWS_API_ENDPOINTS = {
        'everything': f"{NEWS_API_BASE_URL}/everything",
        'top_headlines': f"{NEWS_API_BASE_URL}/top-headlines",
        'sources': f"{NEWS_API_BASE_URL}/sources"
    }
    
    # API Parameters
    DEFAULT_PARAMS = {
        'pageSize': 100,  # Max for free tier
        'language': 'en',
        'sortBy': 'publishedAt'
    }
    
    # Database Configuration
    DATABASE_PATH = "database/news_analytics.db"
    DATABASE_URL = f"sqlite:///{DATABASE_PATH}"
    
    # Data Paths
    RAW_DATA_PATH = "data/raw/"
    PROCESSED_DATA_PATH = "data/processed/"
    
    # Extraction Settings
    MAX_PAGES = 5