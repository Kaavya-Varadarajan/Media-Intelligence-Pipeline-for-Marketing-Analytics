import pandas as pd
import re
from datetime import datetime
import numpy as np

class DataCleaner:
    def __init__(self, df):
        self.df = df
        self.cleaning_log = []
    
    def clean_data(self):
        """Execute complete data cleaning pipeline"""
        print("🧹 Starting Data Cleaning Process...")
        
        original_count = len(self.df)
        
        # Execute cleaning steps
        self._remove_duplicates()
        self._handle_missing_values()
        self._standardize_text_fields()
        self._clean_and_parse_dates()
        self._validate_and_fix_urls()
        self._create_derived_features()
        self._filter_invalid_records()
        
        final_count = len(self.df)
        removed_count = original_count - final_count
        
        print(f"✅ Cleaning complete. Removed {removed_count} invalid records.")
        print(f"📊 Final dataset: {final_count} records")
        
        self._generate_cleaning_report()
        return self.df
    
    def _remove_duplicates(self):
        """Remove duplicate records"""
        initial_count = len(self.df)
        
        # Remove exact duplicates
        self.df = self.df.drop_duplicates()
        
        # Remove duplicates based on URL (primary key)
        if 'url' in self.df.columns:
            self.df = self.df.drop_duplicates(subset=['url'])
        
        removed = initial_count - len(self.df)
        self.cleaning_log.append(f"Removed {removed} duplicate records")
    
    def _handle_missing_values(self):
        """Handle missing values appropriately"""
        # For critical fields, we might remove records
        critical_columns = ['title', 'source_name', 'published_at']
        for col in critical_columns:
            if col in self.df.columns:
                before = len(self.df)
                self.df = self.df.dropna(subset=[col])
                after = len(self.df)
                removed = before - after
                if removed > 0:
                    self.cleaning_log.append(f"Removed {removed} records with missing {col}")
        
        # For less critical fields, impute or fill
        if 'author' in self.df.columns:
            self.df['author'] = self.df['author'].fillna('Unknown')
            self.cleaning_log.append("Filled missing authors with 'Unknown'")
        
        if 'description' in self.df.columns:
            self.df['description'] = self.df['description'].fillna('')
    
    def _standardize_text_fields(self):
        """Clean and standardize text fields"""
        text_columns = ['title', 'description', 'source_name', 'author']
        
        for col in text_columns:
            if col in self.df.columns:
                # Remove extra whitespace
                self.df[col] = self.df[col].astype(str).str.strip()
                
                # Replace multiple spaces with single space
                self.df[col] = self.df[col].str.replace(r'\s+', ' ', regex=True)
                
                # Handle "NaN" strings from pandas
                self.df[col] = self.df[col].replace('nan', '', regex=False)
        
        self.cleaning_log.append("Standardized text fields")
    
    def _clean_and_parse_dates(self):
        """Ensure proper date parsing and handling"""
        if 'published_at' in self.df.columns:
            # Convert to datetime, coerce errors to NaT
            self.df['published_at_parsed'] = pd.to_datetime(
                self.df['published_at'], errors='coerce', utc=True
            )
            
            # Remove records with invalid dates
            invalid_dates = self.df['published_at_parsed'].isna().sum()
            if invalid_dates > 0:
                self.df = self.df.dropna(subset=['published_at_parsed'])
                self.cleaning_log.append(f"Removed {invalid_dates} records with invalid dates")
            
            # Create date-based features
            self.df['published_date'] = self.df['published_at_parsed'].dt.date
            self.df['published_year'] = self.df['published_at_parsed'].dt.year
            self.df['published_month'] = self.df['published_at_parsed'].dt.month
            self.df['published_day'] = self.df['published_at_parsed'].dt.day
            self.df['published_day_of_week'] = self.df['published_at_parsed'].dt.day_name()
            self.df['published_hour'] = self.df['published_at_parsed'].dt.hour
            
            self.cleaning_log.append("Parsed dates and created date-based features")
    
    def _validate_and_fix_urls(self):
        """Validate and clean URL fields"""
        if 'url' in self.df.columns:
            # Remove records with completely invalid URLs
            url_pattern = r'^https?://.+'
            valid_urls = self.df['url'].str.match(url_pattern, na=False)
            invalid_url_count = (~valid_urls).sum()
            
            if invalid_url_count > 0:
                self.df = self.df[valid_urls]
                self.cleaning_log.append(f"Removed {invalid_url_count} records with invalid URLs")
    
    def _create_derived_features(self):
        """Create additional analytical features"""
        # Content length features
        if 'title' in self.df.columns:
            self.df['title_length'] = self.df['title'].str.len()
            self.df['title_word_count'] = self.df['title'].str.split().str.len()
        
        if 'description' in self.df.columns:
            self.df['description_length'] = self.df['description'].str.len()
            self.df['description_word_count'] = self.df['description'].str.split().str.len()
        
        # Content type categorization
        if 'title' in self.df.columns:
            # Simple keyword-based categorization
            def categorize_content(title):
                title_lower = str(title).lower()
                if any(word in title_lower for word in ['ai', 'artificial intelligence', 'machine learning']):
                    return 'AI'
                elif any(word in title_lower for word in ['stock', 'market', 'economy', 'financial']):
                    return 'Finance'
                elif any(word in title_lower for word in ['sport', 'game', 'match', 'player']):
                    return 'Sports'
                elif any(word in title_lower for word in ['movie', 'film', 'celebrity', 'entertainment']):
                    return 'Entertainment'
                else:
                    return 'General'
            
            self.df['content_category'] = self.df['title'].apply(categorize_content)
        
        # Engagement score (simulated - in real scenario, this would come from API)
        np.random.seed(42)  # For reproducible results
        self.df['engagement_score'] = np.random.randint(1, 1000, size=len(self.df))
        
        self.cleaning_log.append("Created derived features and engagement metrics")
    
    def _filter_invalid_records(self):
        """Remove records that don't meet quality thresholds"""
        initial_count = len(self.df)
        
        # Remove articles with very short titles (likely errors)
        if 'title_length' in self.df.columns:
            self.df = self.df[self.df['title_length'] >= 10]
        
        # Remove articles from unknown sources
        if 'source_name' in self.df.columns:
            self.df = self.df[self.df['source_name'] != '']
            self.df = self.df[self.df['source_name'].notna()]
        
        final_count = len(self.df)
        removed = initial_count - final_count
        if removed > 0:
            self.cleaning_log.append(f"Removed {removed} records failing quality checks")
    
    def _generate_cleaning_report(self):
        """Generate cleaning summary report"""
        print("\n" + "="*50)
        print("🧹 DATA CLEANING REPORT")
        print("="*50)
        
        for log_entry in self.cleaning_log:
            print(f"  • {log_entry}")
        
        print(f"\n📊 Final Dataset Stats:")
        print(f"  • Total records: {len(self.df)}")
        print(f"  • Total columns: {len(self.df.columns)}")
        print(f"  • Date range: {self.df['published_at_parsed'].min()} to {self.df['published_at_parsed'].max()}")
        print(f"  • Unique sources: {self.df['source_name'].nunique()}")

# Usage
def clean_retrieved_data(df):
    cleaner = DataCleaner(df)
    cleaned_df = cleaner.clean_data()
    return cleaned_df