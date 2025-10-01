import pandas as pd
import numpy as np
from datetime import datetime
import json

class DataValidator:
    def __init__(self, df):
        self.df = df
        self.validation_report = {}
    
    def validate_data_quality(self):
        """Comprehensive data quality validation"""
        print("🔍 Starting Data Quality Validation...")
        
        validation_checks = {
            'total_records': len(self.df),
            'total_columns': len(self.df.columns),
            'null_records': self._check_nulls(),
            'duplicate_records': self._check_duplicates(),
            'data_types': self._validate_data_types(),
            'value_ranges': self._check_value_ranges(),
            'date_integrity': self._check_date_integrity(),
            'business_rules': self._check_business_rules()
        }
        
        self.validation_report = validation_checks
        self._generate_validation_report()
        return validation_checks
    
    def _check_nulls(self):
        """Check for null values in critical columns"""
        null_report = {}
        critical_columns = ['title', 'source_name', 'published_at', 'url']
        
        for col in critical_columns:
            if col in self.df.columns:
                null_count = self.df[col].isnull().sum()
                null_percentage = (null_count / len(self.df)) * 100
                null_report[col] = {
                    'null_count': null_count,
                    'null_percentage': round(null_percentage, 2)
                }
        
        return null_report
    
    def _check_duplicates(self):
        """Check for duplicate records"""
        duplicate_report = {}
        
        # Check duplicate URLs (should be unique)
        if 'url' in self.df.columns:
            url_duplicates = self.df['url'].duplicated().sum()
            duplicate_report['url_duplicates'] = url_duplicates
        
        # Check duplicate titles from same source on same day
        if all(col in self.df.columns for col in ['title', 'source_name', 'published_at']):
            title_duplicates = self.df.duplicated(
                subset=['title', 'source_name', 'published_at']
            ).sum()
            duplicate_report['content_duplicates'] = title_duplicates
        
        return duplicate_report
    
    def _validate_data_types(self):
        """Validate data types are as expected"""
        type_report = {}
        
        expected_types = {
            'title': 'object',
            'source_name': 'object', 
            'published_at': 'datetime64[ns]',
            'estimated_word_count': 'int64',
            'title_length': 'int64'
        }
        
        for col, expected_type in expected_types.items():
            if col in self.df.columns:
                actual_type = str(self.df[col].dtype)
                type_report[col] = {
                    'expected': expected_type,
                    'actual': actual_type,
                    'valid': expected_type in actual_type
                }
        
        return type_report
    
    def _check_value_ranges(self):
        """Check for reasonable value ranges"""
        range_report = {}
        
        if 'estimated_word_count' in self.df.columns:
            word_stats = {
                'min': self.df['estimated_word_count'].min(),
                'max': self.df['estimated_word_count'].max(),
                'mean': self.df['estimated_word_count'].mean(),
                'outliers': len(self.df[self.df['estimated_word_count'] > 10000])  # Unreasonable word count
            }
            range_report['word_count'] = word_stats
        
        if 'title_length' in self.df.columns:
            title_stats = {
                'min': self.df['title_length'].min(),
                'max': self.df['title_length'].max(),
                'mean': self.df['title_length'].mean()
            }
            range_report['title_length'] = title_stats
        
        return range_report
    
    def _check_date_integrity(self):
        """Validate date fields make sense"""
        date_report = {}
        
        if 'published_at_parsed' in self.df.columns:
            # Use timezone-aware datetime for comparison
            now_utc = pd.Timestamp.now(tz='UTC')
            old_date_utc = pd.Timestamp('2000-01-01', tz='UTC')
            
            future_dates = self.df[self.df['published_at_parsed'] > now_utc]
            very_old_dates = self.df[self.df['published_at_parsed'] < old_date_utc]
            
            date_report = {
                'future_dates': len(future_dates),
                'very_old_dates': len(very_old_dates),
                'date_range': {
                    'min': self.df['published_at_parsed'].min(),
                    'max': self.df['published_at_parsed'].max()
                }
            }
        
        return date_report
    
    def _check_business_rules(self):
        """Validate business logic rules"""
        business_rules = {}
        
        # Rule: Articles should have either title or description
        if 'title' in self.df.columns and 'description' in self.df.columns:
            missing_both = self.df[self.df['title'].isna() & self.df['description'].isna()]
            business_rules['missing_both_title_description'] = len(missing_both)
        
        # Rule: URLs should be valid format
        if 'url' in self.df.columns:
            invalid_urls = self.df[~self.df['url'].str.startswith('http', na=False)]
            business_rules['invalid_urls'] = len(invalid_urls)
        
        return business_rules
    
    def _generate_validation_report(self):
        """Generate comprehensive validation report"""
        print("\n" + "="*60)
        print("📊 DATA VALIDATION REPORT")
        print("="*60)
        
        print(f"Total Records: {self.validation_report['total_records']}")
        print(f"Total Columns: {self.validation_report['total_columns']}")
        
        print("\n❌ NULL VALUES CHECK:")
        for col, stats in self.validation_report['null_records'].items():
            print(f"   {col}: {stats['null_count']} nulls ({stats['null_percentage']}%)")
        
        print("\n🔍 DUPLICATES CHECK:")
        for dup_type, count in self.validation_report['duplicate_records'].items():
            print(f"   {dup_type}: {count}")
        
        print("\n✅ DATA QUALITY SCORE:")
        quality_score = self._calculate_quality_score()
        print(f"   Overall Data Quality: {quality_score}%")
        
        # Save detailed report
        self._save_validation_report()
    
    def _calculate_quality_score(self):
        """Calculate overall data quality score"""
        total_checks = 0
        passed_checks = 0
        
        # Null checks
        for col, stats in self.validation_report['null_records'].items():
            total_checks += 1
            if stats['null_percentage'] < 10:  # Less than 10% nulls
                passed_checks += 1
        
        # Duplicate checks
        for dup_type, count in self.validation_report['duplicate_records'].items():
            total_checks += 1
            if count == 0:
                passed_checks += 1
        
        return round((passed_checks / total_checks) * 100, 2) if total_checks > 0 else 0
    
    def _save_validation_report(self):
        """Save validation report to file"""
        report_path = "data/validation_reports/"
        import os
        os.makedirs(report_path, exist_ok=True)
        
        filename = f"{report_path}validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            # Convert non-serializable objects to strings
            serializable_report = self._make_json_serializable(self.validation_report)
            json.dump(serializable_report, f, indent=2)
        
        print(f"📄 Full validation report saved to: {filename}")
    
    def _make_json_serializable(self, obj):
        """Convert all non-JSON serializable objects to serializable format"""
        import numpy as np
        
        if isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(v) for v in obj]
        elif isinstance(obj, tuple):
            return [self._make_json_serializable(v) for v in obj]
        elif hasattr(obj, 'item'):  # Handle numpy scalars
            return obj.item()
        else:
            return obj

# Usage in your main pipeline
def validate_retrieved_data(df):
    validator = DataValidator(df)
    validation_results = validator.validate_data_quality()
    return validation_results