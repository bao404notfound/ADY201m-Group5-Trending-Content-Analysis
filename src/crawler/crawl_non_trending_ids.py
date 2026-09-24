import subprocess
from pathlib import Path

import polars as pl
import time

# ============================================================
# CONFIG
# ============================================================

SEARCH_QUERIES = [
    # Music
    "music",
    "chill music",
    "music video",
    "live music",
    "song",
    "concert",
    "karaoke",
    "cover song",
    "instrumental",
    "remix music",
    "rock music",

    # Entertainment
    "movie",
    "film",
    "trailer",
    "tv show",
    "celebrity",
    "comedy",
    "game show",
    "reality show",

    # Technology
    "technology",
    "tech",
    "AI",
    "artificial intelligence",
    "programming",
    "Python",
    "computer",
    "smartphone",
    "laptop",
    "automobile",
    "robotics",
    "drone",

    # Gaming
    "gaming",
    "game",
    "Free Fire",
    "Mobile Legends",
    "MLBB",
    "Delta Force",
    "esports",
    "FC Mobile",
    "PUBG",
    "Area of Valor",
    "Mini World",
    "Dota 2",
    "Black Myth Wukong",
    "Genshin Impact",

    # Sports
    "sports",
    "football",
    "soccer",
    "basketball",
    "tennis",
    "baseball",
    "UFC",
    "boxing",
    "cricket",
    "Olympics",
    "athletics",
    "swimming",
    "cycling",
    "gymnastics",
    "volleyball",
    "World Cup",

    # Education
    "education",
    "tutorial",
    "how to",
    "course",
    "lecture",
    "study",
    "science",
    "math",
    "history",
    "language",
    "psychology",
    "philosophy",
    "economics",
    "sociology",
    "biology",
    "chemistry",

    # Lifestyle
    "vlog",
    "daily vlog",
    "travel",
    "food",
    "cooking",
    "fitness",
    "workout",
    "fashion",
    "beauty",
    "lifestyle",
    "health",
    "wellness",

    # News / information
    "news",
    "world news",
    "politics",
    "business",
    "finance",
    "economics",
    "weather",
    "breaking news",
    "current events",
    "investigation",

    # Other
    "podcast",
    "interview",
    "review",
    "documentary",
    "anime",
    "cartoon",
    "DIY",
    "car",
    "motorcycle",
    "manga",
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

    print(f"\n[SEARCH] {query}")

    command = [
        "yt-dlp",
        f"ytsearch{limit}:{query}",
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
