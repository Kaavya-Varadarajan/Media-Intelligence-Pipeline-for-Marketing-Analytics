# 📰 News Analytics Pipeline for Power BI

A complete data pipeline that extracts news data from NewsAPI, processes it, and exports it for Power BI analysis.

## 🚀 Quick Start

### 1. Run Complete Pipeline
```powershell
python run_complete_pipeline.py
```

### 2. Prepare Power BI Data
```powershell
python prepare_powerbi.py
```

### 3. Import to Power BI
- Open Power BI Desktop
- Get Data → Text/CSV
- Import files from `powerbi_exports/` folder

## 📁 Project Structure

```
├── 📄 Core Scripts
│   ├── run_complete_pipeline.py    # Main pipeline runner
│   ├── newsapi_extract.py          # Data extraction from NewsAPI
│   ├── data_validation.py          # Data quality validation
│   ├── data_cleaning.py            # Data cleaning & transformation
│   ├── data_analysis.py            # Data analysis & insights
│   └── sqlite_load.py              # Database loading
│
├── 🔧 Configuration
│   ├── config.py                   # Configuration settings
│   ├── .env.txt                    # API keys (rename to .env)
│   └── requirements.txt            # Python dependencies
│
├── 💾 Data Storage
│   ├── data/
│   │   ├── raw/                    # Raw JSON data from API
│   │   ├── processed/              # Cleaned parquet files
│   │   ├── analysis_reports/       # Analysis summaries
│   │   └── validation_reports/     # Data quality reports
│   └── database/
│       └── news_analytics.db       # SQLite database
│
├── 📊 Power BI Integration
│   ├── powerbi_connector.py        # Power BI export utilities
│   ├── prepare_powerbi.py          # CSV export for Power BI
│   ├── powerbi_exports/            # CSV files for Power BI
│   └── NewAPI Dashboard.pbix       # Power BI dashboard file
```

## 🔄 Data Flow

1. **Extract** → NewsAPI → Raw JSON
2. **Validate** → Data quality checks
3. **Clean** → Transform & enrich data
4. **Analyze** → Generate insights
5. **Load** → SQLite database
6. **Export** → CSV files for Power BI

## 📊 Power BI Dashboard

The pipeline creates several CSV files for Power BI:
- `articles.csv` - Main articles data (272 records)
- `sources.csv` - News source information (149 sources)
- `combined_news_analysis.csv` - Complete dataset for analysis

### Dashboard Pages:
1. **Executive Overview** - Key metrics and trends
2. **Content Analysis** - Article quality and patterns
3. **Temporal Insights** - Time-based analysis
4. **Source Performance** - Source analytics

## 🛠️ Requirements

- Python 3.8+
- NewsAPI key (free tier available)
- Power BI Desktop

## 📈 Data Metrics

- **Articles**: 272 news articles
- **Sources**: 149 unique news sources
- **Date Range**: Last 3 days of news data
- **Categories**: Technology, Business, Sports, Entertainment
- **Metrics**: Title length, word count, engagement scores

## 🔧 Configuration

1. Rename `.env.txt` to `.env`
2. Add your NewsAPI key
3. Run the pipeline

## 📞 Support

Check `powerbi_exports/POWER_BI_SETUP_GUIDE.md` for detailed Power BI setup instructions.