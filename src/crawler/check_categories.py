import os

from dotenv import load_dotenv
from googleapiclient.discovery import build


load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")

youtube = build(
    "youtube",
    "v3",
    developerKey=API_KEY
)


response = youtube.videoCategories().list(
    part="snippet",
    regionCode="VN"
).execute()


for item in response.get("items", []):

    print(
        item["id"],
        "|",
        item["snippet"]["title"],
        "| assignable:",
        item["snippet"]["assignable"]
    )
