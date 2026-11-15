import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm


# ============================================================
# CONFIG
# ============================================================

INPUT_FILE = (
    "data/raw/youtube/non_trending/"
    "non_trending_ids.parquet"
)

OUTPUT_FILE = (
    "data/raw/youtube/non_trending/"
    "non_trending_raw.parquet"
)


# ============================================================
# LOAD API KEY
# ============================================================

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")

if not API_KEY:
    raise ValueError(
        "YOUTUBE_API_KEY not found in .env"
    )


youtube = build(
    "youtube",
    "v3",
    developerKey=API_KEY
)


# ============================================================
# HELPERS
# ============================================================

def chunks(items, size=50):

    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_batch(video_ids):

    response = (
        youtube.videos()
        .list(
            part=(
                "snippet,"
                "statistics,"
                "contentDetails"
            ),
            id=",".join(video_ids)
        )
        .execute()
    )

    return response.get("items", [])


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("NON-TRENDING METADATA CRAWLER")
    print("=" * 60)

    # --------------------------------------------------------
    # Read IDs
    # --------------------------------------------------------

    df_ids = pl.read_parquet(
        INPUT_FILE
    )

    video_ids = (
        df_ids
        .select("video_id")
        .unique()
        ["video_id"]
        .to_list()
    )

    print(
        f"Video IDs: {len(video_ids)}"
    )

    # --------------------------------------------------------
    # Fetch metadata
    # --------------------------------------------------------

    rows = []

    batches = list(
        chunks(
            video_ids,
            50
        )
    )

    print(
        f"API batches: {len(batches)}"
    )

    for batch in tqdm(
        batches,
        desc="Fetching metadata"
    ):

        try:

            items = fetch_batch(
                batch
            )

        except HttpError as e:

            print(
                "\nAPI ERROR:"
            )

            print(e)

            continue

        # ----------------------------------------------------
        # Parse response
        # ----------------------------------------------------

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

                # ----------------------------
                # ID
                # ----------------------------

                "video_id":
                    item["id"],

                # ----------------------------
                # Snippet
                # ----------------------------

                "title":
                    snippet.get("title"),

                "description":
                    snippet.get("description"),

                "published_at":
                    snippet.get("publishedAt"),

                "channel_id":
                    snippet.get("channelId"),

                "channel_title":
                    snippet.get("channelTitle"),

                "category_id":
                    snippet.get("categoryId"),

                # ----------------------------
                # Content details
                # ----------------------------

                "duration":
                    content.get("duration"),

                "definition":
                    content.get("definition"),

                "caption":
                    content.get("caption"),

                # ----------------------------
                # Statistics
                # ----------------------------

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

                # ----------------------------
                # Dataset labels
                # ----------------------------

                "is_trending": 0,

                "source":
                    "yt_dlp_youtube_search",
            })

    # --------------------------------------------------------
    # Check result
    # --------------------------------------------------------

    if not rows:

        print(
            "No metadata collected."
        )

        return

    # --------------------------------------------------------
    # DataFrame
    # --------------------------------------------------------

    df = (
        pl.DataFrame(rows)
        .unique(
            subset=["video_id"]
        )
    )

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    output = Path(
        OUTPUT_FILE
    )

    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.write_parquet(
        output,
        compression="zstd"
    )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print("\n" + "=" * 60)
    print("NON-TRENDING METADATA")
    print("=" * 60)

    print(
        "Rows:",
        df.height
    )

    print(
        "Unique videos:",
        df.select(
            "video_id"
        ).unique().height
    )

    print(
        "Saved:",
        output
    )


if __name__ == "__main__":
    main()
