import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from config import Config

class NewsAPIExtractor:
    def __init__(self):
        self.config = Config()
        self.session = requests.Session()
    
    def make_api_request(self, url, params=None):
        """Make API request with error handling"""
        try:
            print(f"Making request to: {url}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Request failed: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Status code: {e.response.status_code}")
                print(f"Response: {e.response.text}")
            return None
    
    def get_top_headlines(self, category='general', country='us', pages=3):
        """
        Get top headlines from NewsAPI
        """
        all_articles = []
        
        for page in range(1, pages + 1):
            print(f"Fetching page {page} of {pages} for {category} headlines")
            
            params = {
                'apiKey': self.config.NEWS_API_KEY,
                'category': category,
                'country': country,
                'pageSize': self.config.DEFAULT_PARAMS['pageSize'],
                'page': page
            }
            
            data = self.make_api_request(self.config.NEWS_API_ENDPOINTS['top_headlines'], params)
            
            if not data or 'articles' not in data:
                print(f"No data received for page {page}")
                break
            
            articles = data['articles']
            if not articles:
                print(f"No more articles found on page {page}")
                break
            
            all_articles.extend(articles)
            
            # Respect rate limits
            time.sleep(1)
            
            # Stop if we've got all available articles
            if len(articles) < self.config.DEFAULT_PARAMS['pageSize']:
                break
        
        print(f"Retrieved {len(all_articles)} total articles for {category}")
        return all_articles
    
    def get_news_sources(self):
        """Get available news sources"""
        print("Fetching available news sources...")
        params = {
            'apiKey': self.config.NEWS_API_KEY,
            'language': 'en'
        }
        
        data = self.make_api_request(self.config.NEWS_API_ENDPOINTS['sources'], params)
        return data.get('sources', []) if data else []
    
    def get_everything(self, query='technology', days=7, pages=2):
        """
        Get everything from NewsAPI with a specific query
        """
        all_articles = []
        from_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        for page in range(1, pages + 1):
            print(f"Fetching page {page} of {pages} for query: {query}")
            
            params = {
                'apiKey': self.config.NEWS_API_KEY,
                'q': query,
                'from': from_date,
                'pageSize': self.config.DEFAULT_PARAMS['pageSize'],
                'page': page,
                'sortBy': 'publishedAt',
                'language': 'en'
            }
            
            data = self.make_api_request(self.config.NEWS_API_ENDPOINTS['everything'], params)
            
            if not data or 'articles' not in data:
                print(f"No data received for page {page}")
                break
            
            articles = data['articles']
            if not articles:
                print(f"No more articles found on page {page}")
                break
            
            all_articles.extend(articles)
            time.sleep(1)
            
            if len(articles) < self.config.DEFAULT_PARAMS['pageSize']:
                break
        
        print(f"Retrieved {len(all_articles)} total articles for query: {query}")
        return all_articles
    
    def flatten_news_data(self, raw_articles, article_type='headlines'):
        """
        Convert nested NewsAPI articles into flat pandas DataFrame
        """
        flattened_records = []
        
        for article in raw_articles:
            try:
                # Extract basic article information
                record = {
                    'article_id': hash(article.get('url', '') + article.get('publishedAt', '')),  # Create unique ID
                    'source_id': article.get('source', {}).get('id', ''),
                    'source_name': article.get('source', {}).get('name', ''),
                    'author': article.get('author', ''),
                    'title': article.get('title', ''),
                    'description': article.get('description', ''),
                    'url': article.get('url', ''),
                    'url_to_image': article.get('urlToImage', ''),
                    'published_at': article.get('publishedAt', ''),
                    'content': article.get('content', ''),
                    'article_type': article_type
                }
                
                # Generate derived metrics for analysis
                record['title_length'] = len(record['title']) if record['title'] else 0
                record['description_length'] = len(record['description']) if record['description'] else 0
                record['content_length'] = len(record['content']) if record['content'] else 0
                record['has_image'] = bool(record['url_to_image'])
                record['has_author'] = bool(record['author'])
                
                # Parse dates for better analysis
                if record['published_at']:
                    try:
                        pub_date = datetime.fromisoformat(record['published_at'].replace('Z', '+00:00'))
                        record['published_at_parsed'] = pub_date
                        record['published_year'] = pub_date.year
                        record['published_month'] = pub_date.month
                        record['published_day'] = pub_date.day
                        record['published_hour'] = pub_date.hour
                        record['published_day_of_week'] = pub_date.strftime('%A')
                        record['published_week'] = pub_date.isocalendar()[1]
                    except (ValueError, AttributeError) as e:
                        print(f"Date parsing error: {e}")
                        record['published_at_parsed'] = None
                
                # Content analysis
                record['estimated_word_count'] = len(record['content'].split()) if record['content'] else 0
                
                flattened_records.append(record)
                
            except Exception as e:
                print(f"Error processing article: {e}")
                continue
        
        return pd.DataFrame(flattened_records)
    
    def save_raw_data(self, data, filename):
        """Save raw JSON data for backup"""
        import os
        os.makedirs(self.config.RAW_DATA_PATH, exist_ok=True)
        filepath = os.path.join(self.config.RAW_DATA_PATH, f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Raw data saved to: {filepath}")
        return filepath
    
    def save_to_parquet(self, df, filename):
        """Save DataFrame to Parquet format"""
        import os
        os.makedirs(self.config.PROCESSED_DATA_PATH, exist_ok=True)
        filepath = os.path.join(self.config.PROCESSED_DATA_PATH, f"{filename}.parquet")
        df.to_parquet(filepath, index=False)
        print(f"Data saved to Parquet: {filepath}")
        return filepath

def main():
    extractor = NewsAPIExtractor()
    
    print("🚀 Starting NewsAPI Data Extraction Pipeline")
    print("=" * 50)
    
    # Step 1: Get available sources
    print("\n📋 Step 1: Fetching available news sources...")
    sources = extractor.get_news_sources()
    if sources:
        source_names = [s['name'] for s in sources[:5]]  # Show first 5
        print(f"Sample sources: {', '.join(source_names)}...")
    
    # Step 2: Extract data from multiple categories
    categories = ['technology', 'business', 'sports', 'entertainment']
    all_articles = []
    
    for category in categories:
        print(f"\n📥 Step 2: Extracting '{category}' headlines...")
        category_articles = extractor.get_top_headlines(category=category, pages=2)
        all_articles.extend(category_articles)
        
        # Add category tag to each article
        for article in category_articles:
            article['category'] = category
        
        time.sleep(2)
    
    # Step 3: Get additional technology articles with more historical data
    print(f"\n🔍 Step 3: Searching for technology articles...")
    tech_articles = extractor.get_everything(query='technology', days=30, pages=3)  # Get 30 days of data
    for article in tech_articles:
        article['category'] = 'technology'
    all_articles.extend(tech_articles)
    
    # Step 4: Get business articles with more historical data
    print(f"\n🔍 Step 4: Searching for business articles...")
    business_articles = extractor.get_everything(query='business', days=30, pages=3)  # Get 30 days of data
    for article in business_articles:
        article['category'] = 'business'
    all_articles.extend(business_articles)
    
    if not all_articles:
        print("❌ No data retrieved. Please check your API key and internet connection.")
        return None
    
    print(f"✅ Total articles collected: {len(all_articles)}")
    
    # Step 4: Save raw JSON data
    print("\n💾 Step 4: Saving raw data...")
    raw_file = extractor.save_raw_data(all_articles, "newsapi_raw")
    
    # Step 5: Flatten and transform data
    print("\n🔄 Step 5: Flattening JSON data...")
    flattened_df = extractor.flatten_news_data(all_articles)
    
    print(f"✅ Processed {len(flattened_df)} records")
    print("\n📊 Data Overview:")
    print(f"   - Columns: {len(flattened_df.columns)}")
    print(f"   - Sources: {flattened_df['source_name'].nunique()}")
    print(f"   - Date range: {flattened_df['published_at_parsed'].min()} to {flattened_df['published_at_parsed'].max()}")
    
    # Step 6: Data cleaning
    print("\n🧹 Step 6: Cleaning data...")
    # Remove duplicates based on URL
    initial_count = len(flattened_df)
    flattened_df = flattened_df.drop_duplicates(subset=['url'])
    print(f"   - Removed {initial_count - len(flattened_df)} duplicates")
    
    # Fill missing values
    numeric_columns = ['title_length', 'description_length', 'content_length', 'estimated_word_count']
    for col in numeric_columns:
        flattened_df[col] = flattened_df[col].fillna(0)
    
    # Step 7: Save to Parquet
    print("\n💾 Step 7: Saving processed data...")
    parquet_file = extractor.save_to_parquet(flattened_df, "newsapi_processed")
    
    print("\n🎉 Extraction completed successfully!")
    print(f"📈 Sample of the data:")
    print(flattened_df[['source_name', 'title', 'published_at', 'estimated_word_count']].head())
    
    return flattened_df

if __name__ == "__main__":
    df = main()