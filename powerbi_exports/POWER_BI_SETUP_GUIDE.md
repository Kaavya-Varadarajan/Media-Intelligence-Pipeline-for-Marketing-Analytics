
# 📊 Power BI Connection Guide for News Analytics
Generated: 2025-10-01 13:23:43

## 📁 Available Data Files:
- `articles.csv` (272 rows)
- `sources.csv` (149 rows)
- `analytics_summary.csv` (1 rows)
- `sqlite_sequence.csv` (1 rows)


## 🔗 How to Connect to Power BI:

### Method 1: CSV Import (Recommended) ⭐
1. **Open Power BI Desktop**
2. **Get Data** → **Text/CSV**
3. Navigate to the `powerbi_exports` folder
4. Select `combined_news_analysis.csv` (main dataset)
5. Click **Load**
6. Repeat for other CSV files if needed

### Method 2: Folder Import (All Files at Once)
1. **Get Data** → **Folder**
2. Navigate to `powerbi_exports` folder
3. **Combine & Transform** files as needed
4. Click **Load**

### Method 3: SQLite Database Direct Connection
1. **Get Data** → **Database** → **SQLite database**
2. Navigate to `database/news_analytics.db`
3. Select tables: `articles`, `sources`, `analytics_summary`
4. Click **Load**

### Method 4: Excel Workbook
1. Open CSV files in Excel and save as `.xlsx`
2. **Get Data** → **Excel workbook**
3. Import the Excel file

## 📈 Recommended Dashboard Structure:

### Page 1: Executive Overview
```
📊 Key Metrics Cards:
- Total Articles
- Unique Sources  
- Date Range
- Average Engagement

📈 Charts:
- Daily Article Volume (Line Chart)
- Top Sources (Bar Chart)
- Articles by Category (Pie Chart)
```

### Page 2: Content Analysis
```
📊 Content Metrics:
- Average Title Length
- Average Word Count
- Articles with Images %

📈 Charts:
- Title Length Distribution (Histogram)
- Word Count vs Engagement (Scatter)
- Content Category Performance (Bar Chart)
```

### Page 3: Temporal Insights
```
📈 Time-based Analysis:
- Articles by Hour (Heatmap)
- Day of Week Trends (Column Chart)
- Publishing Calendar (Matrix)
- Peak Hours Analysis
```

### Page 4: Source Performance
```
📊 Source Analytics:
- Source Productivity (Table)
- Source Engagement Comparison
- Geographic Distribution (if available)
- Source Category Performance
```

## 🔄 Data Refresh Process:

### Manual Refresh:
1. Run: `python run_complete_pipeline.py` (updates database)
2. Run: `python prepare_powerbi.py` (exports to CSV)
3. In Power BI: **Home** → **Refresh**

### Automatic Refresh (Power BI Service):
1. Publish report to Power BI Service
2. Set up scheduled refresh
3. Configure data source credentials

## 📋 Sample DAX Measures:

```dax
// Total Articles
Total Articles = COUNTROWS(articles)

// Average Engagement
Avg Engagement = AVERAGE(articles[engagement_score])

// Articles This Week
Articles This Week = 
CALCULATE(
    COUNTROWS(articles),
    articles[published_at_parsed] >= TODAY() - 7
)

// Top Source
Top Source = 
CALCULATE(
    VALUES(articles[source_name]),
    TOPN(1, articles, articles[engagement_score])
)

// Engagement Trend
Engagement Trend = 
VAR CurrentEngagement = [Avg Engagement]
VAR PreviousEngagement = 
    CALCULATE(
        [Avg Engagement],
        DATEADD(articles[published_at_parsed], -7, DAY)
    )
RETURN
    DIVIDE(CurrentEngagement - PreviousEngagement, PreviousEngagement)
```

## 🎨 Visualization Recommendations:

1. **Line Chart**: Daily article volume over time
2. **Bar Chart**: Top 10 sources by article count
3. **Heatmap**: Articles published by hour and day of week
4. **Scatter Plot**: Word count vs engagement score
5. **Treemap**: Article distribution by category
6. **Card Visuals**: Key metrics (total articles, avg engagement)
7. **Table**: Detailed article listings with filters

## 🔧 Data Model Relationships:

```
articles (Fact Table)
├── source_name → sources[source_name] (Many-to-One)
├── published_date → Date Table (Many-to-One)
└── category → Categories Table (Many-to-One)
```

## 📱 Mobile Dashboard Tips:

1. Use large, clear visuals
2. Limit to 3-4 key metrics per page
3. Ensure touch-friendly interactions
4. Test on mobile before publishing

## ⚠️ Troubleshooting:

**Issue**: CSV files not loading
- **Solution**: Check file encoding (should be UTF-8)

**Issue**: Date columns not recognized
- **Solution**: Format dates as YYYY-MM-DD in CSV

**Issue**: Large file size
- **Solution**: Filter data by date range before export

**Issue**: Refresh fails
- **Solution**: Ensure file paths are accessible

## 📞 Support:
For issues with data preparation, run: `python prepare_powerbi.py --help`
