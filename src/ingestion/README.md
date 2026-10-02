# YouTube Data Ingestion
This folder contains scripts and modules responsible for collecting YouTube trending content data for the project.
## Responsibilities
- Collect video metadata from YouTube
- Retrieve relevant information such as video title, channel, category, views, likes, comments, and publication time when available
- Store collected raw data for further processing
- Handle API requests and basic data collection errors
## Planned Components
- `crawler.py` - Main crawler/data ingestion script
- Additional ingestion modules may be added as the project develops.
## Data Flow
YouTube Data API
→ Python Crawler
→ Raw Data
→ Data Processing
