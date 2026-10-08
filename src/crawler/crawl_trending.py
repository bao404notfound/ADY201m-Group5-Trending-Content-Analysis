import os
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ============================================================
# CONFIG
# ============================================================

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")

if not API_KEY:
    raise ValueError("YOUTUBE_API_KEY not found in .env")

REGIONS = ["VN", "US", "KR", "JP", "GB"]

CATEGORIES = {
    "10": "Music",
    "1": "Film & Animation",
    "28": "Science & Technology",
    "17": "Sports",
    "20": "Gaming",
    "25": "News & Politics",
}

MAX_RESULTS = 50
OUTPUT_DIR = Path("data/raw/youtube/trending")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# YOUTUBE CLIENT
# ============================================================

youtube = build("youtube", "v3", developerKey=API_KEY)


# ============================================================
# CRAWL
# ============================================================

def safe_int(value, default=0):
    """Ép kiểu int an toàn, tránh lỗi khi giá trị là None hoặc chuỗi rỗng."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def crawl_trending(crawled_at_str):
    rows = []

    for region in REGIONS:
        for category_id, category_name in CATEGORIES.items():
            print(f"\n[TRENDING] Region={region} | Category={category_name}")
            page_token = None

            while True:
                try:
                    request = youtube.videos().list(
                        part="snippet,statistics,contentDetails",
                        chart="mostPopular",
                        regionCode=region,
                        videoCategoryId=category_id,
                        maxResults=MAX_RESULTS,
                        pageToken=page_token,
                    )
                    response = request.execute()

                except HttpError as e:
                    print(f"[ERROR API] {e}")
                    break

                items = response.get("items", [])
                if not items:
                    break

                for item in items:
                    snippet = item.get("snippet", {})
                    statistics = item.get("statistics", {})
                    content = item.get("contentDetails", {})

                    rows.append(
                        {
                            "video_id": item.get("id"),
                            "title": snippet.get("title"),
                            "description": snippet.get("description"),
                            "published_at": snippet.get("publishedAt"),
                            "channel_id": snippet.get("channelId"),
                            "channel_title": snippet.get("channelTitle"),
                            "category_id": category_id,
                            "category_name": category_name,
                            "duration": content.get("duration"),
                            "view_count": safe_int(statistics.get("viewCount")),
                            "like_count": safe_int(statistics.get("likeCount")),
                            "comment_count": safe_int(statistics.get("commentCount")),
                            "region": region,
                            "is_trending": 1,
                            "source": "youtube_mostPopular",
                            "crawled_at": crawled_at_str,
                        }
                    )

                print(f"    Collected total: {len(rows)} items")

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

                time.sleep(0.5)

    return rows


# ============================================================
# SAVE
# ============================================================

def main():
    print("=" * 60)
    print("YOUTUBE TRENDING CRAWLER")
    print("=" * 60)

    crawled_at = datetime.now(timezone.utc)
    crawled_at_str = crawled_at.isoformat()
    timestamp = crawled_at.strftime("%Y%m%d_%H%M")

    print(f"Crawl time (UTC): {crawled_at_str}")

    rows = crawl_trending(crawled_at_str)

    if not rows:
        print("No data collected.")
        return

    df = pl.DataFrame(rows)

    print(f"\nBefore dedup: {df.height}")
    df = df.unique(subset=["video_id", "region", "category_id"])
    print(f"After dedup: {df.height}")

    # Chuyển đổi kiểu dữ liệu cột crawled_at và published_at sang Datetime chuẩn
    df = df.with_columns(
        [
            pl.col("crawled_at").str.to_datetime(),
            pl.col("published_at").str.to_datetime(strict=False),
        ]
    )

    output = OUTPUT_DIR / f"trending_{timestamp}.parquet"
    df.write_parquet(output, compression="zstd")

    print(f"\nSaved: {output}")
    print(f"Rows: {df.height}")
    print(f"Unique videos: {df.select(pl.col('video_id')).unique().height}")
    print(f"Crawl timestamp: {crawled_at_str}")


if __name__ == "__main__":
    main()