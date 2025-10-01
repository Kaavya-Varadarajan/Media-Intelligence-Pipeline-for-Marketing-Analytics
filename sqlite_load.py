import sqlite3
import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from config import Config

class SQLiteLoader:
    def __init__(self):
        self.config = Config()
        self.db_path = self.config.DATABASE_PATH
        self.engine = None
        self.setup_database()
    
    def setup_database(self):
        """Setup database and create necessary directories"""
        # Create database directory if it doesn't exist
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Create SQLAlchemy engine
        self.engine = create_engine(self.config.DATABASE_URL, echo=False)
        
        # Create tables
        self.create_tables()
        
        print(f"✅ Database setup complete: {self.db_path}")
    
    def create_tables(self):
        """Create necessary tables in the database"""
        
        # Articles table schema
        articles_schema = """
        CREATE TABLE IF NOT EXISTS articles (
            article_id INTEGER PRIMARY KEY,
            source_id TEXT,
            source_name TEXT NOT NULL,
            author TEXT,
            title TEXT NOT NULL,
            description TEXT,
            url TEXT UNIQUE NOT NULL,
            url_to_image TEXT,
            published_at TEXT NOT NULL,
            published_at_parsed DATETIME,
            content TEXT,
            article_type TEXT,
            category TEXT,
            title_length INTEGER,
            description_length INTEGER,
            content_length INTEGER,
            estimated_word_count INTEGER,
            has_image BOOLEAN,
            has_author BOOLEAN,
            published_year INTEGER,
            published_month INTEGER,
            published_day INTEGER,
            published_hour INTEGER,
            published_day_of_week TEXT,
            published_week INTEGER,
            published_date DATE,
            title_word_count INTEGER,
            description_word_count INTEGER,
            content_category TEXT,
            engagement_score INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Sources table schema
        sources_schema = """
        CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            description TEXT,
            url TEXT,
            category TEXT,
            language TEXT,
            country TEXT,
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
            total_articles INTEGER DEFAULT 0
        );
        """
        
        # Analytics summary table
        analytics_schema = """
        CREATE TABLE IF NOT EXISTS analytics_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date DATE,
            total_articles INTEGER,
            unique_sources INTEGER,
            avg_engagement REAL,
            top_source TEXT,
            peak_hour INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_name);",
            "CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at_parsed);",
            "CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category);",
            "CREATE INDEX IF NOT EXISTS idx_articles_engagement ON articles(engagement_score);"
        ]
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(articles_schema))
                conn.execute(text(sources_schema))
                conn.execute(text(analytics_schema))
                
                for index in indexes:
                    conn.execute(text(index))
                
                conn.commit()
            
            print("✅ Database tables created successfully")
            
        except Exception as e:
            print(f"❌ Error creating tables: {e}")
    
    def load_articles(self, df):
        """Load articles data into the database"""
        try:
            print(f"📥 Loading {len(df)} articles into database...")
            
            # Prepare data for insertion
            df_to_load = df.copy()
            
            # Ensure proper data types
            if 'published_at_parsed' in df_to_load.columns:
                df_to_load['published_at_parsed'] = pd.to_datetime(df_to_load['published_at_parsed'])
            
            # Handle boolean columns
            bool_columns = ['has_image', 'has_author']
            for col in bool_columns:
                if col in df_to_load.columns:
                    df_to_load[col] = df_to_load[col].astype(int)
            
            # Add timestamps
            df_to_load['created_at'] = datetime.now()
            df_to_load['updated_at'] = datetime.now()
            
            # Load data using pandas to_sql with upsert logic
            df_to_load.to_sql(
                'articles', 
                self.engine, 
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print(f"✅ Successfully loaded {len(df_to_load)} articles")
            
            # Update source statistics
            self.update_source_stats()
            
            return True
            
        except Exception as e:
            print(f"❌ Error loading articles: {e}")
            return False
    
    def update_source_stats(self):
        """Update source statistics table"""
        try:
            source_stats_query = """
            INSERT OR REPLACE INTO sources (source_id, source_name, total_articles, last_updated)
            SELECT 
                source_id,
                source_name,
                COUNT(*) as total_articles,
                datetime('now') as last_updated
            FROM articles 
            GROUP BY source_id, source_name
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(source_stats_query))
                conn.commit()
            
            print("✅ Source statistics updated")
            
        except Exception as e:
            print(f"❌ Error updating source stats: {e}")
    
    def create_analytics_summary(self):
        """Create daily analytics summary"""
        try:
            analytics_query = """
            INSERT OR REPLACE INTO analytics_summary 
            (report_date, total_articles, unique_sources, avg_engagement, top_source, peak_hour)
            SELECT 
                DATE('now') as report_date,
                COUNT(*) as total_articles,
                COUNT(DISTINCT source_name) as unique_sources,
                AVG(engagement_score) as avg_engagement,
                (SELECT source_name FROM articles GROUP BY source_name ORDER BY COUNT(*) DESC LIMIT 1) as top_source,
                (SELECT published_hour FROM articles GROUP BY published_hour ORDER BY COUNT(*) DESC LIMIT 1) as peak_hour
            FROM articles 
            WHERE DATE(published_at_parsed) = DATE('now')
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(analytics_query))
                conn.commit()
            
            print("✅ Analytics summary created")
            
        except Exception as e:
            print(f"❌ Error creating analytics summary: {e}")
    
    def get_database_stats(self):
        """Get current database statistics"""
        try:
            with self.engine.connect() as conn:
                # Total articles
                total_articles = conn.execute(text("SELECT COUNT(*) FROM articles")).scalar()
                
                # Unique sources
                unique_sources = conn.execute(text("SELECT COUNT(DISTINCT source_name) FROM articles")).scalar()
                
                # Date range
                date_range = conn.execute(text("""
                    SELECT 
                        MIN(published_at_parsed) as min_date,
                        MAX(published_at_parsed) as max_date
                    FROM articles
                """)).fetchone()
                
                # Recent articles
                recent_articles = conn.execute(text("""
                    SELECT COUNT(*) FROM articles 
                    WHERE published_at_parsed >= datetime('now', '-7 days')
                """)).scalar()
                
                stats = {
                    'total_articles': total_articles,
                    'unique_sources': unique_sources,
                    'date_range': {
                        'min': date_range[0] if date_range else None,
                        'max': date_range[1] if date_range else None
                    },
                    'recent_articles': recent_articles
                }
                
                return stats
                
        except Exception as e:
            print(f"❌ Error getting database stats: {e}")
            return None
    
    def export_to_csv(self, output_dir="data/exports/"):
        """Export database tables to CSV for Power BI"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Export articles
            articles_df = pd.read_sql("SELECT * FROM articles", self.engine)
            articles_path = os.path.join(output_dir, "articles.csv")
            articles_df.to_csv(articles_path, index=False)
            
            # Export sources
            sources_df = pd.read_sql("SELECT * FROM sources", self.engine)
            sources_path = os.path.join(output_dir, "sources.csv")
            sources_df.to_csv(sources_path, index=False)
            
            # Export analytics summary
            analytics_df = pd.read_sql("SELECT * FROM analytics_summary", self.engine)
            analytics_path = os.path.join(output_dir, "analytics_summary.csv")
            analytics_df.to_csv(analytics_path, index=False)
            
            print(f"✅ Data exported to CSV files in {output_dir}")
            print(f"   - Articles: {articles_path}")
            print(f"   - Sources: {sources_path}")
            print(f"   - Analytics: {analytics_path}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error exporting to CSV: {e}")
            return False

def main():
    """Main function for loading processed data"""
    print("💾 Starting Database Loading Process...")
    
    # Initialize loader
    loader = SQLiteLoader()
    
    # Check for processed data file
    processed_file = "data/processed/final_cleaned_data.parquet"
    
    if os.path.exists(processed_file):
        print(f"📂 Loading data from: {processed_file}")
        df = pd.read_parquet(processed_file)
        
        # Load into database
        success = loader.load_articles(df)
        
        if success:
            # Create analytics summary
            loader.create_analytics_summary()
            
            # Export to CSV for Power BI
            loader.export_to_csv()
            
            # Show database stats
            stats = loader.get_database_stats()
            if stats:
                print(f"\n📊 DATABASE STATISTICS:")
                print(f"   Total Articles: {stats['total_articles']:,}")
                print(f"   Unique Sources: {stats['unique_sources']}")
                print(f"   Date Range: {stats['date_range']['min']} to {stats['date_range']['max']}")
                print(f"   Recent Articles (7 days): {stats['recent_articles']}")
            
            print("\n🎉 Database loading completed successfully!")
            return True
        else:
            print("\n❌ Database loading failed!")
            return False
    
    else:
        print(f"❌ Processed data file not found: {processed_file}")
        print("   Please run the data extraction and cleaning pipeline first.")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)