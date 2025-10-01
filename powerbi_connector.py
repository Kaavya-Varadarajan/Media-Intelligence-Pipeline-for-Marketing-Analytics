import pandas as pd
import sqlite3
from datetime import datetime
import os

class PowerBIConnector:
    def __init__(self, db_path='database/news_analytics.db'):
        self.db_path = db_path
        self.export_dir = 'powerbi_exports'
        os.makedirs(self.export_dir, exist_ok=True)
    
    def create_powerbi_datasets(self):
        """Create CSV datasets for Power BI"""
        print(" Creating Power BI CSV datasets...")
        
        # Connect to SQLite database
        conn = sqlite3.connect(self.db_path)
        
        # Export all tables and views to CSV
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        # Get all views
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view';")
        views = cursor.fetchall()
        
        all_objects = tables + views
        
        exported_files = []
        
        for obj in all_objects:
            table_name = obj[0]
            try:
                # Read data into pandas
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                
                # Clean column names for Power BI
                df.columns = [col.replace(' ', '_').lower() for col in df.columns]
                
                # Export to CSV
                csv_path = f'{self.export_dir}/{table_name}.csv'
                df.to_csv(csv_path, index=False, encoding='utf-8')
                
                exported_files.append((table_name, len(df)))
                print(f"   Exported {table_name}: {len(df)} rows")
                
            except Exception as e:
                print(f"    Could not export {table_name}: {e}")
        
        conn.close()
        
        # Create a combined dataset for easier analysis
        self._create_combined_dataset()
        
        # Create connection instructions
        self._create_connection_guide(exported_files)
        
        print(f"\n Power BI CSV files ready in '{self.export_dir}/'")
        return exported_files
    
    def _create_combined_dataset(self):
        """Create a comprehensive combined dataset for Power BI"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Create a denormalized fact table for easier Power BI analysis
            combined_query = """
            SELECT 
                a.*,
                s.total_articles as source_total_articles
            FROM articles a
            LEFT JOIN sources s ON a.source_name = s.source_name
            """
            
            combined_df = pd.read_sql_query(combined_query, conn)
            
            # Clean column names for Power BI
            combined_df.columns = [col.replace(' ', '_').lower() for col in combined_df.columns]
            
            combined_df.to_csv(f'{self.export_dir}/combined_news_analysis.csv', index=False, encoding='utf-8')
            print("   Created combined_news_analysis.csv")
            
        except Exception as e:
            print(f"    Could not create combined dataset: {e}")
            # Fallback: just export articles table
            try:
                articles_df = pd.read_sql_query("SELECT * FROM articles", conn)
                articles_df.columns = [col.replace(' ', '_').lower() for col in articles_df.columns]
                articles_df.to_csv(f'{self.export_dir}/articles_main.csv', index=False, encoding='utf-8')
                print("   Created articles_main.csv as fallback")
            except Exception as e2:
                print(f"   Could not create fallback dataset: {e2}")
        
        conn.close()
    
    def _create_connection_guide(self, exported_files):
        """Create a comprehensive Power BI connection guide"""
        guide_content = f"""
#  Power BI Connection Guide for News Analytics
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

##  Available Data Files:
"""
        for table_name, row_count in exported_files:
            guide_content += f"- `{table_name}.csv` ({row_count:,} rows)\n"

        guide_content += """

## 🔗 How to Connect to Power BI:


"""
        
        with open(f'{self.export_dir}/POWER_BI_SETUP_GUIDE.md', 'w', encoding='utf-8') as f:
            f.write(guide_content)
        
        print("  Created comprehensive POWER_BI_SETUP_GUIDE.md")

# Usage
def prepare_for_powerbi():
    connector = PowerBIConnector()
    files = connector.create_powerbi_datasets()
    return files