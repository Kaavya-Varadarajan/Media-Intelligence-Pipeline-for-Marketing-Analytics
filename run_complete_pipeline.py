#!/usr/bin/env python3
"""
Complete Data Pipeline: Extraction → Validation → Cleaning → Analysis → Loading
"""

from newsapi_extract import main as extract_main
from data_validation import validate_retrieved_data
from data_cleaning import clean_retrieved_data
from data_analysis import analyze_data
from sqlite_load import main as load_main
import time
import pandas as pd

def run_complete_pipeline():
    print(" STARTING COMPLETE DATA PIPELINE")
    print("=" * 70)
    
    # Step 1: Data Extraction
    print("\n STEP 1: Data Extraction from NewsAPI")
    start_time = time.time()
    raw_df = extract_main()
    
    if raw_df is None or raw_df.empty:
        print(" Pipeline failed at extraction stage")
        return False
    
    extraction_time = time.time() - start_time
    print(f" Extraction completed in {extraction_time:.2f} seconds")
    
    # Step 2: Data Validation
    print("\n STEP 2: Data Quality Validation")
    start_time = time.time()
    validation_results = validate_retrieved_data(raw_df)
    validation_time = time.time() - start_time
    print(f" Validation completed in {validation_time:.2f} seconds")
    
    # Step 3: Data Cleaning
    print("\n STEP 3: Data Cleaning & Transformation")
    start_time = time.time()
    cleaned_df = clean_retrieved_data(raw_df)
    cleaning_time = time.time() - start_time
    print(f" Cleaning completed in {cleaning_time:.2f} seconds")
    
    # Step 4: Data Analysis
    print("\n STEP 4: Data Analysis & Business Insights")
    start_time = time.time()
    analysis_results = analyze_data(cleaned_df)
    analysis_time = time.time() - start_time
    print(f" Analysis completed in {analysis_time:.2f} seconds")
    
    # Step 5: Database Loading
    print("\n STEP 5: Database Loading")
    start_time = time.time()
    
    # Save cleaned data for loading
    import os
    os.makedirs("data/processed/", exist_ok=True)
    cleaned_df.to_parquet("data/processed/final_cleaned_data.parquet")
    
    load_main()  # This will load the cleaned data
    
    loading_time = time.time() - start_time
    print(f" Database loading completed in {loading_time:.2f} seconds")
    
    # Pipeline Summary
    total_time = extraction_time + validation_time + cleaning_time + analysis_time + loading_time
    
    print("\n PIPELINE EXECUTION SUMMARY")
    print("=" * 50)
    print(f" Extraction:    {extraction_time:.2f}s")
    print(f" Validation:    {validation_time:.2f}s") 
    print(f" Cleaning:      {cleaning_time:.2f}s")
    print(f" Analysis:      {analysis_time:.2f}s")
    print(f" Loading:       {loading_time:.2f}s")
    print(f"  Total Time:    {total_time:.2f}s")
    print(f" Final Records: {len(cleaned_df)}")
    print(f" Data Sources:  {cleaned_df['source_name'].nunique()}")
    
    return True

if __name__ == "__main__":
    success = run_complete_pipeline()
    if success:
        print("\n Pipeline completed successfully! Data is ready for Power BI analysis.")
    else:
        print("\n Pipeline failed. Check the logs above for issues.")