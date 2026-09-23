# YouTube Data Ingestion

This folder contains scripts and modules for collecting YouTube trending content data.

## Responsibilities

- Collect video metadata from YouTube Data API
- Retrieve video information such as title, channel, views, likes, comments, and publication time
- Handle API requests and basic errors
- Store raw collected data for further processing

## Components

- `crawler.py` - Main YouTube data collection script

## Data Flow

YouTube Data API
→ Python Crawler
→ Raw Data
→ Data Processing
