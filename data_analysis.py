import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import json
import os

class NewsDataAnalyzer:
    def __init__(self, df):
        self.df = df
        self.analysis_results = {}
    
    def perform_comprehensive_analysis(self):
        """Execute complete data analysis pipeline"""
        print("📊 Starting Comprehensive Data Analysis...")
        
        # Run all analysis components
        self.analysis_results = {
            'basic_stats': self._basic_statistics(),
            'temporal_analysis': self._temporal_analysis(),
            'source_analysis': self._source_analysis(),
            'content_analysis': self._content_analysis(),
            'engagement_analysis': self._engagement_analysis(),
            'trend_analysis': self._trend_analysis(),
            'business_insights': self._business_insights()
        }
        
        self._generate_analysis_report()
        self._save_analysis_results()
        
        return self.analysis_results
    
    def _basic_statistics(self):
        """Basic statistical overview"""
        stats = {
            'total_articles': len(self.df),
            'unique_sources': self.df['source_name'].nunique(),
            'date_range': {
                'start': self.df['published_at_parsed'].min(),
                'end': self.df['published_at_parsed'].max(),
                'span_days': (self.df['published_at_parsed'].max() - self.df['published_at_parsed'].min()).days
            },
            'content_stats': {
                'avg_title_length': self.df['title_length'].mean(),
                'avg_description_length': self.df['description_length'].mean(),
                'avg_word_count': self.df['estimated_word_count'].mean(),
                'articles_with_images': self.df['has_image'].sum(),
                'articles_with_authors': self.df['has_author'].sum()
            }
        }
        return stats
    
    def _temporal_analysis(self):
        """Analyze publishing patterns over time"""
        temporal_stats = {}
        
        # Daily publication patterns
        daily_counts = self.df.groupby('published_date').size()
        temporal_stats['daily_publication'] = {
            'avg_per_day': daily_counts.mean(),
            'max_day': daily_counts.max(),
            'min_day': daily_counts.min(),
            'most_active_date': daily_counts.idxmax(),
            'least_active_date': daily_counts.idxmin()
        }
        
        # Hourly patterns
        if 'published_hour' in self.df.columns:
            hourly_counts = self.df.groupby('published_hour').size()
            temporal_stats['hourly_patterns'] = {
                'peak_hour': hourly_counts.idxmax(),
                'quiet_hour': hourly_counts.idxmin(),
                'peak_count': hourly_counts.max(),
                'distribution': hourly_counts.to_dict()
            }
        
        # Day of week patterns
        if 'published_day_of_week' in self.df.columns:
            dow_counts = self.df.groupby('published_day_of_week').size()
            temporal_stats['day_of_week'] = {
                'most_active_day': dow_counts.idxmax(),
                'least_active_day': dow_counts.idxmin(),
                'distribution': dow_counts.to_dict()
            }
        
        return temporal_stats
    
    def _source_analysis(self):
        """Analyze news sources performance"""
        source_stats = {}
        
        # Top sources by volume
        source_counts = self.df['source_name'].value_counts()
        source_stats['top_sources'] = {
            'by_volume': source_counts.head(10).to_dict(),
            'total_sources': len(source_counts),
            'avg_articles_per_source': source_counts.mean()
        }
        
        # Source content quality metrics
        source_quality = self.df.groupby('source_name').agg({
            'title_length': 'mean',
            'description_length': 'mean',
            'estimated_word_count': 'mean',
            'has_image': 'mean',
            'has_author': 'mean',
            'engagement_score': 'mean'
        }).round(2)
        
        source_stats['quality_metrics'] = source_quality.head(10).to_dict()
        
        return source_stats
    
    def _content_analysis(self):
        """Analyze content characteristics"""
        content_stats = {}
        
        # Content categories if available
        if 'content_category' in self.df.columns:
            category_dist = self.df['content_category'].value_counts()
            content_stats['categories'] = category_dist.to_dict()
        
        # Title analysis
        content_stats['title_analysis'] = {
            'avg_length': self.df['title_length'].mean(),
            'length_distribution': {
                'short': len(self.df[self.df['title_length'] < 50]),
                'medium': len(self.df[(self.df['title_length'] >= 50) & (self.df['title_length'] < 100)]),
                'long': len(self.df[self.df['title_length'] >= 100])
            }
        }
        
        # Word count analysis
        content_stats['word_count_analysis'] = {
            'avg_words': self.df['estimated_word_count'].mean(),
            'word_distribution': {
                'short': len(self.df[self.df['estimated_word_count'] < 100]),
                'medium': len(self.df[(self.df['estimated_word_count'] >= 100) & (self.df['estimated_word_count'] < 500)]),
                'long': len(self.df[self.df['estimated_word_count'] >= 500])
            }
        }
        
        return content_stats
    
    def _engagement_analysis(self):
        """Analyze engagement patterns"""
        engagement_stats = {}
        
        if 'engagement_score' in self.df.columns:
            engagement_stats['overall'] = {
                'avg_engagement': self.df['engagement_score'].mean(),
                'median_engagement': self.df['engagement_score'].median(),
                'max_engagement': self.df['engagement_score'].max(),
                'min_engagement': self.df['engagement_score'].min()
            }
            
            # Engagement by source
            source_engagement = self.df.groupby('source_name')['engagement_score'].mean().sort_values(ascending=False)
            engagement_stats['by_source'] = source_engagement.head(10).to_dict()
            
            # Engagement by time
            if 'published_hour' in self.df.columns:
                hourly_engagement = self.df.groupby('published_hour')['engagement_score'].mean()
                engagement_stats['by_hour'] = hourly_engagement.to_dict()
        
        return engagement_stats
    
    def _trend_analysis(self):
        """Identify trends and patterns"""
        trends = {}
        
        # Publication volume trends
        daily_volume = self.df.groupby('published_date').size()
        if len(daily_volume) > 1:
            trends['volume_trend'] = {
                'slope': np.polyfit(range(len(daily_volume)), daily_volume.values, 1)[0],
                'is_increasing': np.polyfit(range(len(daily_volume)), daily_volume.values, 1)[0] > 0
            }
        
        # Content length trends
        if 'published_date' in self.df.columns:
            daily_avg_length = self.df.groupby('published_date')['title_length'].mean()
            if len(daily_avg_length) > 1:
                trends['title_length_trend'] = {
                    'slope': np.polyfit(range(len(daily_avg_length)), daily_avg_length.values, 1)[0],
                    'is_increasing': np.polyfit(range(len(daily_avg_length)), daily_avg_length.values, 1)[0] > 0
                }
        
        return trends
    
    def _business_insights(self):
        """Generate actionable business insights"""
        insights = {}
        
        # Peak performance insights
        insights['optimal_timing'] = {
            'best_publication_hour': self.df.groupby('published_hour')['engagement_score'].mean().idxmax() if 'engagement_score' in self.df.columns else None,
            'best_day_of_week': self.df.groupby('published_day_of_week')['engagement_score'].mean().idxmax() if 'engagement_score' in self.df.columns else None
        }
        
        # Content recommendations
        high_engagement = self.df[self.df['engagement_score'] > self.df['engagement_score'].quantile(0.8)] if 'engagement_score' in self.df.columns else pd.DataFrame()
        
        if not high_engagement.empty:
            insights['high_performing_content'] = {
                'avg_title_length': high_engagement['title_length'].mean(),
                'avg_word_count': high_engagement['estimated_word_count'].mean(),
                'image_usage_rate': high_engagement['has_image'].mean(),
                'top_sources': high_engagement['source_name'].value_counts().head(5).to_dict()
            }
        
        # Market gaps
        insights['content_gaps'] = {
            'underrepresented_hours': self.df.groupby('published_hour').size().nsmallest(3).index.tolist() if 'published_hour' in self.df.columns else [],
            'low_volume_sources': self.df['source_name'].value_counts().tail(5).to_dict()
        }
        
        return insights
    
    def _generate_analysis_report(self):
        """Generate comprehensive analysis report"""
        print("\n" + "="*60)
        print("📊 COMPREHENSIVE DATA ANALYSIS REPORT")
        print("="*60)
        
        # Basic Statistics
        basic = self.analysis_results['basic_stats']
        print(f"\n📈 BASIC STATISTICS:")
        print(f"   Total Articles: {basic['total_articles']:,}")
        print(f"   Unique Sources: {basic['unique_sources']}")
        print(f"   Date Range: {basic['date_range']['span_days']} days")
        print(f"   Avg Title Length: {basic['content_stats']['avg_title_length']:.1f} characters")
        print(f"   Articles with Images: {basic['content_stats']['articles_with_images']:,} ({basic['content_stats']['articles_with_images']/basic['total_articles']*100:.1f}%)")
        
        # Top Sources
        source = self.analysis_results['source_analysis']
        print(f"\n🏢 TOP NEWS SOURCES:")
        for source_name, count in list(source['top_sources']['by_volume'].items())[:5]:
            print(f"   {source_name}: {count} articles")
        
        # Temporal Patterns
        temporal = self.analysis_results['temporal_analysis']
        print(f"\n⏰ TEMPORAL PATTERNS:")
        print(f"   Peak Hour: {temporal['hourly_patterns']['peak_hour']}:00")
        print(f"   Most Active Day: {temporal['day_of_week']['most_active_day']}")
        print(f"   Avg Articles/Day: {temporal['daily_publication']['avg_per_day']:.1f}")
        
        # Business Insights
        insights = self.analysis_results['business_insights']
        print(f"\n💡 KEY INSIGHTS:")
        if insights['optimal_timing']['best_publication_hour']:
            print(f"   Optimal Publishing Hour: {insights['optimal_timing']['best_publication_hour']}:00")
        if insights['optimal_timing']['best_day_of_week']:
            print(f"   Best Day for Engagement: {insights['optimal_timing']['best_day_of_week']}")
    
    def _save_analysis_results(self):
        """Save analysis results to file"""
        report_path = "data/analysis_reports/"
        os.makedirs(report_path, exist_ok=True)
        
        filename = f"{report_path}analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Convert non-serializable objects to strings
        serializable_results = {}
        for key, value in self.analysis_results.items():
            serializable_results[key] = self._make_serializable(value)
        
        with open(filename, 'w') as f:
            json.dump(serializable_results, f, indent=2)
        
        print(f"\n📄 Full analysis report saved to: {filename}")
    
    def _make_serializable(self, obj):
        """Convert pandas/numpy objects to JSON serializable format"""
        import numpy as np
        from datetime import date, datetime
        
        if isinstance(obj, (pd.Timestamp, datetime, date)):
            return str(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, tuple):
            return [self._make_serializable(v) for v in obj]
        elif hasattr(obj, 'item'):  # Handle numpy scalars
            return obj.item()
        elif pd.isna(obj):  # Handle pandas NaN values
            return None
        else:
            return obj

# Main function for pipeline integration
def analyze_data(df):
    """Main function to perform data analysis"""
    analyzer = NewsDataAnalyzer(df)
    analysis_results = analyzer.perform_comprehensive_analysis()
    return analysis_results

if __name__ == "__main__":
    # For testing purposes
    print("Data Analysis module loaded successfully!")