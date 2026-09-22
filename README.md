# ADY201m-Group5-Trending-Content-Analysis
## GROUP MEMBER
- Nguyễn Thành Gia Bảo - QE210225 - Leader
- Lê Hoàng Linh - QE210139
- Phan Thành Đạt - QE200132
  
-----------------------------

## PROJECT OVERVIEW
* **Course:** ADY201m - AI20D, Data Science with Python & SQL
* **Institution:** FPT University Campus Quy Nhon
* **Group:** Group 5
* **Project Name:** `ADY201m-Group5-Trending-Content-Analysis`

### Description
This project develops an automated end-to-end data pipeline deployed on Docker infrastructure to collect, store, process, and analyze video data from **YouTube Top Trending** in Vietnam.

Using data retrieved directly from the **YouTube Data API v3**, the project conducts Exploratory Data Analysis (EDA) and tests statistical hypotheses regarding:
- Optimal publishing time slots.
- Impact of negative engagement (Dislike rates & Negative Comments).
- Sensational headline/title structures (Clickbait).

Subsequently, the project trains Machine Learning models to predict viral potential and provides data-driven recommendations for content creators to optimize their publishing strategies.

-----------------------------

## RESEARCH HYPOTHESIS
### RQ1: Timing (Publishing Time)
* **Research Question:** Does publishing a video during peak hours (18:00 - 22:00) significantly impact the speed at which it appears on the Trending tab compared to other time slots?
  * **$H_0$:** Null Hypothesis: The time of posting a video (prime hours 6 PM-10 PM compared to other times) does not significantly affect how quickly the video appears on the Trending tab.
  * **$H_1$:** Alternative Hypothesis: Posting a video during prime hours (6 PM-10 PM) has a positive effect, helping the video appear on the Trending tab faster than posting at other times of the day. 
 
### RQ2: Engagement & Sentiment (Negative Interactions)
* **Research Question:** Do negative comments and high dislike rates reduce a video's virality and shorten its retention time on the Trending tab?
  * **$H_0$:** Null Hypothesis: The number of negative comments and the dislike ratio do not have a negative effect or reduce the time a video stays on the Trending tab.
  * **$H_1$:** Alternative Hypothesis: The number of negative comments and dislike ratio have a negative impact, reducing a video's virality and significantly shortening the time it remains on the Trending tab.

### RQ3: Content & Title (Clickbait Titles)
* **Research Question:** Does using sensational or clickbait titles (all caps, strong keywords, short length) increase average view counts in the first 48 hours compared to standard titles?
  * **$H_0$:** Null hypothesis: Using curiosity-inducing (clickbait) titles does not create a statistically significant difference in the average views during the first 48 hours compared to videos with regular titles.
  * **$H_1$:**  Alternative hypothesis: Using curiosity-inducing (clickbait) titles creates a noticeable difference (specifically, a significantly higher average view count) in the first 48 hours compared to videos with regular titles.
  
-----------------------------

## SYSTEM ARCHITECTURE 
### Docker Architecture Overview
The system is designed as a containerized data pipeline for collecting, storing, processing, and analyzing YouTube Trending Content.

The overall data flow follows:

YouTube Data API → Python Crawler → MinIO → Database → App / Workstation

YouTube Data API is an external data source, while the main system components are deployed as independent Docker containers. Each container is responsible for a specific stage of the data pipeline, allowing the system to maintain clear separation of responsibilities and a consistent development environment across team members.

### System Components
- YouTube Data API (External Service):	Provides YouTube video and trending-content data.
- Python Crawler	(Docker Container):	Collects data from the YouTube Data API.
- MinIO	(Docker Container):	Stores raw data as the Raw Data Lake.
- Database	(Docker Container):	Stores processed and structured data.
- App / Workstation	(Docker Container):	Performs data processing, analysis, and visualization.

### Data Flow
The data flows through the system in the following stages:
1. Data Source — YouTube Data API provides video-related metadata such as video ID, channel ID, title, published time, view count, like count, and comment count.
2. Data Ingestion — The Python Crawler sends API requests to YouTube Data API, receives the data, and transfers the collected raw data to MinIO.
3. Raw Data Storage — MinIO acts as the Raw Data Lake and preserves the original collected data before further processing.
4. Data Processing & Structured Storage — Raw data from MinIO is processed through the data processing/ETL stage and stored in the Database as structured data.
5. Analysis — The App / Workstation accesses structured data from the Database for further processing, analysis, and visualization.

### Technologies
- Python:	Data collection, data processing, and analysis
- YouTube Data API:	Provides YouTube video and trending-content data
- Docker:	Containerizes the system components and provides a consistent development environment
- MinIO:	Provides object storage and serves as the Raw Data Lake
- Database:	Stores processed and structured data for querying and analysis
- Jupyter Notebook:	Data exploration, processing, and analysis
- RStudio:	Statistical analysis and data distribution visualization
- Matplotlib:	Data visualization and chart generation
- Seaborn:	Statistical data visualization and distribution analysis
- Git & GitHub:	Version control and collaboration among team members

### Architecture Rationale

The architecture separates data ingestion, raw data storage, structured data storage, and analysis into independent components. This separation makes the system easier to maintain and allows individual components to be modified or extended without redesigning the entire pipeline.

Using MinIO as the Raw Data Lake preserves the original collected data, while the Database provides structured data for efficient querying and analysis. Docker ensures that the main services can run in consistent and reproducible environments across different machines.
