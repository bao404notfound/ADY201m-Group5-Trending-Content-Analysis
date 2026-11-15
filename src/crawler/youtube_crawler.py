import os

#thư viện dùng đọc file .env
from dotenv import load_dotenv

#dùng thu viện gọi API của Google
from googleapiclient.discovery import build 

#đọc nội dung file .env
load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")

youtube = build(
    "youtube",
    "v3",
    developerKey=API_KEY
)

print("YouTube API client initialized successfully!")
