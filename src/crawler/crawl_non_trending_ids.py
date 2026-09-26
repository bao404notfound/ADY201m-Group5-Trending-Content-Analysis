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
    "lofi music",
    "hip hop music",
    "pop music",
    "classical music",
    "jazz music",
    "electronic music",
    "v-pop music",
    "music video",

    # Entertainment
    "movie",
    "tv series",
    "drama",
    "animation",
    "comedy",
    "documentary",
    "short film",
    "AI generated content",
    "film",
    "romance",
    "anime",

    # Technology
    "technology",
    "tech",
    "self-driving car",
    "smart television",
    "virtual reality",
    "smart home",
    "AI",
    "artificial intelligence",
    "programming",

    # Gaming
    "gaming",
    "game",
    "live stream",
    "video game",
    "horror game",
    "cyberpunk game",
    "CYB3RPUNK 2077",
    "The sims 4",
    "Horizon Forbidden West",
    "dragon dogma 2",
    "Minecraft",
    "GTA V",

    # Sports
    "sports",
    "e-sports",
    "football",
    "Olympics",
    "martial arts",
    "marathon",
    "vovinam",
    "kickboxing",
    "judo",
    "karate",
    "sumo",
    "taekwondo",
    "wu shu",
    "racing",

    # Education
    "education",
    "tutorial",
    "how to",
    "course",
    "lecture",
    "study",
    "civic education",
    "psychology",
    "political science",
    "media studies",
    "architecture",

    # Lifestyle
    "vlog",
    "Active lifestyle",
    "Avoid processed foods",
    "Healthy eating",
    "healthy lifestyle",
    "work-life balance",
    "alcohol-free lifestyle",
    "caffeine-free lifestyle",
    "excessive screen time",
    "fast food consumption",
    "adrenaline junkie lifestyle",
    "close-knit community lifestyle",
    "frequent traveler lifestyle",
    "hungry for love lifestyle",
    "indoorsy lifestyle",
    "people personality lifestyle",
    "single lifestyle",
    "sedentary lifestyle",
    "workaholic lifestyle",
    "lifestyle",
    

    # News / information
    "news",
    "BBC news",
    "CNN news",
    "breaking news",
    "Vnexpress news",
    "VTV news",
    "24h news",

    # Other
    "podcast",
    "fitness and yoga",
    "cooking and recipes",
    "personal finance and investing",
    "history and culture",
    "reviews and unboxing",
    "mukbang and food challenges",
]

TARGET_PER_QUERY = 600

OUTPUT_FILE = (
    "data/raw/youtube/non_trending/"
    "candidate_ids_parallel.parquet"
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
                "is_trending": pl.Int64,
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
                new_rows,
                schema={
                    "video_id": pl.String,
                    "search_query": pl.String,
                    "is_trending": pl.Int64,
                    "source": pl.String,
                },
                orient="row",
            )

            existing = existing.with_columns(
                pl.col("is_trending").cast(pl.Int64)
            )

            new_df = new_df.with_columns(
                pl.col("is_trending").cast(pl.Int64)
            )

            existing = pl.concat(
                [existing, new_df],
                how="vertical",
            )

            existing.write_parquet(
                output,
                compression="zstd",
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
