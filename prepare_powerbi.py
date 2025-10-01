#!/usr/bin/env python3
"""
Prepare News Analytics Data for Power BI
This script exports all data from the SQLite database to CSV files for Power BI import
"""

from powerbi_connector import PowerBIConnector
import os

def main():
    print(" PREPARING DATA FOR POWER BI")
    print("=" * 50)
    
    # Check if database exists
    db_path = 'database/news_analytics.db'
    if not os.path.exists(db_path):
        print(" Database not found! Please run the complete pipeline first:")
        print("   python run_complete_pipeline.py")
        return False
    
    # Initialize connector
    connector = PowerBIConnector(db_path)
    
    # Create Power BI datasets
    exported_files = connector.create_powerbi_datasets()
    
    if exported_files:
        print(f"\n SUCCESS! {len(exported_files)} datasets prepared for Power BI")
        print("\n Files created in 'powerbi_exports/' folder:")
        for table_name, row_count in exported_files:
            print(f"    {table_name}.csv ({row_count:,} rows)")
        
        print(f"\n Check 'powerbi_exports/POWER_BI_SETUP_GUIDE.md' for connection instructions")
        print(f"\n To refresh data: Re-run this script after updating the database")
        return True
    else:
        print(" No data exported. Check database structure.")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)