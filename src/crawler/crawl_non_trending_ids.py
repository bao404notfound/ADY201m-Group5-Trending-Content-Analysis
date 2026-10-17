import subprocess
from pathlib import Path

import polars as pl
import time

# ============================================================
# CONFIG
# ============================================================

SEARCH_QUERIES = [
    # Music
    "V-pop",
    "J-pop",
    "US-UK music",
    "K-pop",
    "piano cover",
    "C-pop",
    
    # Entertainment
    "horror movie",
    "new film review",
    "Disney movie",

    # Technology
    "new technology",
    "AI news",
    "tech review",

    # Gaming
    "game",
    "Candy Crush Saga",
    "Dragon Ball Z Dokkan Battle",
    "One Piece Bounty Rush",
    "Sky: Children of the Light",
    "Play Together",
    "One Piece game",

    # Sports
    "sports news",
    "football",
    "climbing mountain",
    "racing",

    # Education
    "tutorial",
    "how to",
    "Japanese learning",
    "language learning",
    "study tips",

    # Lifestyle
    "vlog",
    "travel",
    "cooking",
    "fitness",
    "fashion",
    "beauty",
    "lifestyle",

    # News / information
    "news",
    "war news",
    "politics",
    "economics",
    "weather",
    "breaking news",

    # Other
    "audio book",
    "audio drama",
    "Vietnam history documentary",
    "new anime",
    "cartoon network",
    "new manga",
    "hot manhwa",
    "hot manhua",
]

TARGET_PER_QUERY = 600

OUTPUT_FILE = (
    "data/raw/youtube/non_trending/"
    "candidate_ids.parquet"
)


# ============================================================
# SEARCH
# ============================================================

def search_youtube(query, limit):

    # Append 2026 to query to bias towards recent videos
    actual_query = f"{query} 2026"
    print(f"\n[SEARCH] {actual_query}")

    command = [
        "yt-dlp",
        f"ytsearch{limit}:{actual_query}",
        "--flat-playlist",
        "--print",
        "%(id)s",
        "--skip-download",
        "--no-warnings",
        "--quiet",
    ]

    try:

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300,
        )

    except subprocess.TimeoutExpired:

        print("[TIMEOUT]")
        return []

    if result.returncode != 0:

        print("[YT-DLP ERROR]")
        print(f"Query failed: {query}")
        print(result.stderr[:1500])

        return []

    video_ids = []

    for line in result.stdout.splitlines():

        video_id = line.strip()

        if not video_id:
            continue

        if len(video_id) != 11:
            continue

        video_ids.append(video_id)

    # Deduplicate inside query
    video_ids = list(
        dict.fromkeys(video_ids)
    )

    print(
        f"Collected: {len(video_ids)}"
    )

    return video_ids


# ============================================================
# MAIN
# ============================================================

def main():

    output = Path(
        OUTPUT_FILE
    )

    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Load existing candidate IDs
    # --------------------------------------------------------

    if output.exists():

        print(
            "Existing candidate file found."
        )

        existing = (
            pl.read_parquet(output)
            .unique(
                subset=["video_id"]
            )
        )

        print(
            "Existing unique videos:",
            existing.height
        )

    else:

        existing = pl.DataFrame(
            schema={
                "video_id": pl.String,
                "search_query": pl.String,
                "is_trending": pl.Int8,
                "source": pl.String,
            }
        )

        print(
            "No existing candidate file."
        )

    existing_ids = set(
        existing["video_id"].to_list()
    )

    # --------------------------------------------------------
    # Search
    # --------------------------------------------------------

    new_rows = []

    for query in SEARCH_QUERIES:

        video_ids = search_youtube(
            query,
            TARGET_PER_QUERY
        )

        new_count = 0

        for video_id in video_ids:

            if video_id in existing_ids:

                continue

            existing_ids.add(
                video_id
            )

            new_rows.append({

                "video_id":
                    video_id,

                "search_query":
                    query,

                "is_trending":
                    0,

                "source":
                    "yt_dlp_youtube_search",
            })

            new_count += 1

        print(
            f"New unique videos: {new_count}"
        )

        print(
            f"Total unique videos: "
            f"{len(existing_ids)}"
        )

        # ----------------------------------------------------
        # CHECKPOINT
        # ----------------------------------------------------

        if new_rows:

            new_df = pl.DataFrame(
                new_rows
            )

            existing = pl.concat(
                [
                    existing,
                    new_df
                ],
                how="vertical"
            )

            existing.write_parquet(
                output,
                compression="zstd"
            )

            new_rows = []

            print(
                f"[CHECKPOINT SAVED] "
                f"{output}"
            )
            time.sleep(5)

    # --------------------------------------------------------
    # Final
    # --------------------------------------------------------

    print("\n" + "=" * 60)
    print("CRAWL FINISHED")
    print("=" * 60)

    print(
        "Total unique candidates:",
        len(existing_ids)
    )

    print(
        "Saved:",
        output
    )


if __name__ == "__main__":
    main()
