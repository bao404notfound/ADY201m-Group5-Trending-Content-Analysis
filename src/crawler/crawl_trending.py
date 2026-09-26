import os
import time
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")

if not API_KEY:
    raise ValueError(
        "YOUTUBE_API_KEY not found in .env"
    )


REGIONS = [
    "VN",
    "US",
    "KR",
    "JP",
    "GB"
]


CATEGORIES = {
    "10": "Music",
    "1": "Film & Animation",
    "28": "Science & Technology",
    "17": "Sports",
    "20": "Gaming",
    "25": "News & Politics"
}


MAX_RESULTS = 50

OUTPUT_DIR = Path(
    "data/raw/youtube/trending"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# YOUTUBE CLIENT
# ============================================================

youtube = build(
    "youtube",
    "v3",
    developerKey=API_KEY
)


# ============================================================
# CRAWL
# ============================================================

def crawl_trending():

    rows = []

    for region in REGIONS:

        for category_id, category_name in CATEGORIES.items():

            print()
            print(
                f"[TRENDING] "
                f"Region={region} "
                f"Category={category_name}"
            )

            page_token = None

            while True:

                try:

                    request = youtube.videos().list(
                        part=(
                            "snippet,"
                            "statistics,"
                            "contentDetails"
                        ),

                        chart="mostPopular",

                        regionCode=region,

                        videoCategoryId=category_id,

                        maxResults=MAX_RESULTS,

                        pageToken=page_token
                    )

                    response = request.execute()

                except HttpError as e:

                    print(
                        f"[ERROR] {e}"
                    )

                    break

                items = response.get(
                    "items",
                    []
                )

                if not items:
                    break

                for item in items:

                    snippet = item.get(
                        "snippet",
                        {}
                    )

                    statistics = item.get(
                        "statistics",
                        {}
                    )

                    content = item.get(
                        "contentDetails",
                        {}
                    )

                    rows.append({

                        "video_id":
                            item["id"],

                        "title":
                            snippet.get(
                                "title"
                            ),

                        "description":
                            snippet.get(
                                "description"
                            ),

                        "published_at":
                            snippet.get(
                                "publishedAt"
                            ),

                        "channel_id":
                            snippet.get(
                                "channelId"
                            ),

                        "channel_title":
                            snippet.get(
                                "channelTitle"
                            ),

                        "category_id":
                            category_id,

                        "category_name":
                            category_name,

                        "duration":
                            content.get(
                                "duration"
                            ),

                        "view_count":
                            int(
                                statistics.get(
                                    "viewCount",
                                    0
                                )
                            ),

                        "like_count":
                            int(
                                statistics.get(
                                    "likeCount",
                                    0
                                )
                            ),

                        "comment_count":
                            int(
                                statistics.get(
                                    "commentCount",
                                    0
                                )
                            ),

                        "region":
                            region,

                        "is_trending": 1,

                        "source":
                            "youtube_mostPopular"
                    })

                print(
                    f"    Collected: "
                    f"{len(rows)}"
                )

                page_token = response.get(
                    "nextPageToken"
                )

                if not page_token:
                    break

                time.sleep(0.2)

    return rows


# ============================================================
# SAVE
# ============================================================

def main():

    print(
        "=" * 60
    )

    print(
        "YOUTUBE TRENDING CRAWLER"
    )

    print(
        "=" * 60
    )

    rows = crawl_trending()

    if not rows:

        print(
            "No data collected."
        )

        return

    df = pl.DataFrame(rows)

    print()
    print(
        "Before dedup:",
        df.height
    )

    df = df.unique(
        subset=[
            "video_id",
            "region"
        ]
    )

    print(
        "After dedup:",
        df.height
    )

    output = (
        OUTPUT_DIR /
        "trending_raw.parquet"
    )

    df.write_parquet(
        output,
        compression="zstd"
    )

    print()
    print(
        f"Saved: {output}"
    )

    print(
        f"Rows: {df.height}"
    )

    print(
        "Unique videos:",
        df.select(
            pl.col("video_id")
        ).unique().height
    )


if __name__ == "__main__":
    main()
