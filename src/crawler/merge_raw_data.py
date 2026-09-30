from pathlib import Path
import polars as pl

# ============================================================
# CONFIG
# ============================================================

TRENDING_FILE = Path(
    "data/raw/youtube/trending/trending_raw.parquet"
)

NON_TRENDING_FILE = Path(
    "data/raw/youtube/non_trending/non_trending_raw.parquet"
)

DISLIKES_FILE = Path(
    "data/raw/youtube/dislikes/dislikes_raw.parquet"
)

OUTPUT_FILE = Path(
    "data/raw/youtube/youtube_raw_merged.parquet"
)


# ============================================================
# CATEGORY MAPPING
# ============================================================

CATEGORY_MAP = {
    1: "Film & Animation",
    2: "Autos & Vehicles",
    10: "Music",
    15: "Pets & Animals",
    17: "Sports",
    18: "Short Movies",
    19: "Travel & Events",
    20: "Gaming",
    21: "Videoblogging",
    22: "People & Blogs",
    23: "Comedy",
    24: "Entertainment",
    25: "News & Politics",
    26: "Howto & Style",
    27: "Education",
    28: "Science & Technology",
    29: "Nonprofits & Activism",
    30: "Movies",
    31: "Anime/Animation",
    32: "Action/Adventure",
    33: "Classics",
    34: "Comedy",
    35: "Documentary",
    36: "Drama",
    37: "Family",
    38: "Foreign",
    39: "Horror",
    40: "Sci-Fi/Fantasy",
    41: "Thriller",
    42: "Shorts",
    43: "Shows",
    44: "Trailers",
}


# ============================================================
# HELPERS
# ============================================================

def print_section(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print_section("MERGE YOUTUBE RAW DATA + DISLIKES")

    # --------------------------------------------------------
    # 1. Check input files
    # --------------------------------------------------------

    print_section("CHECK INPUT FILES")

    input_files = [
        TRENDING_FILE,
        NON_TRENDING_FILE,
        DISLIKES_FILE,
    ]

    for file in input_files:
        if not file.exists():
            raise FileNotFoundError(
                f"Input file not found: {file}"
            )

        print(f"OK: {file}")

    # --------------------------------------------------------
    # 2. Read Parquet files
    # --------------------------------------------------------

    print_section("READ PARQUET FILES")

    trending = pl.read_parquet(TRENDING_FILE)
    non_trending = pl.read_parquet(NON_TRENDING_FILE)
    dislikes = pl.read_parquet(DISLIKES_FILE)

    print(f"Trending rows     : {trending.height:,}")
    print(f"Non-trending rows : {non_trending.height:,}")
    print(f"Dislikes rows     : {dislikes.height:,}")

    # --------------------------------------------------------
    # 3. Validate required columns
    # --------------------------------------------------------

    print_section("VALIDATE SCHEMAS")

    trending_required = [
        "video_id",
        "title",
        "description",
        "published_at",
        "channel_id",
        "channel_title",
        "category_id",
        "category_name",
        "duration",
        "view_count",
        "like_count",
        "comment_count",
        "region",
        "is_trending",
        "source",
    ]

    non_trending_required = [
        "video_id",
        "title",
        "description",
        "published_at",
        "channel_id",
        "channel_title",
        "category_id",
        "duration",
        "definition",
        "caption",
        "view_count",
        "like_count",
        "comment_count",
        "is_trending",
        "source",
    ]

    dislikes_required = [
        "video_id",
        "likes",
        "dislikes",
        "raw_likes",
        "raw_dislikes",
        "rating",
        "view_count",
        "date_created",
        "deleted",
        "source",
    ]

    for column in trending_required:
        if column not in trending.columns:
            raise ValueError(
                f"Trending is missing column: {column}"
            )

    for column in non_trending_required:
        if column not in non_trending.columns:
            raise ValueError(
                f"Non-trending is missing column: {column}"
            )

    for column in dislikes_required:
        if column not in dislikes.columns:
            raise ValueError(
                f"Dislikes is missing column: {column}"
            )

    print("Trending schema    : OK")
    print("Non-trending schema: OK")
    print("Dislikes schema    : OK")

    # --------------------------------------------------------
    # 4. Add category_name to non-trending
    # --------------------------------------------------------

    print_section("ADD CATEGORY NAME")

    non_trending = non_trending.with_columns(
        pl.col("category_id")
        .replace(CATEGORY_MAP, default=None)
        .alias("category_name")
    )

    unknown_categories = (
        non_trending
        .filter(pl.col("category_name").is_null())
        .select("category_id")
        .unique()
        .sort("category_id")
    )

    if unknown_categories.height > 0:
        print("WARNING: Unknown category IDs found:")
        print(unknown_categories)
    else:
        print("All non-trending category IDs mapped successfully.")

    # --------------------------------------------------------
    # 5. Select common video columns
    # --------------------------------------------------------
    #
    # NOTE:
    # - region only exists in trending
    # - definition/caption only exist in non-trending
    #
    # We keep the common schema for the merged raw dataset.
    # --------------------------------------------------------

    video_columns = [
        "video_id",
        "title",
        "description",
        "published_at",
        "channel_id",
        "channel_title",
        "category_id",
        "category_name",
        "duration",
        "view_count",
        "like_count",
        "comment_count",
        "is_trending",
        "source",
    ]

    trending = trending.select(video_columns)
    non_trending = non_trending.select(video_columns)

    # --------------------------------------------------------
    # 6. Combine trending + non-trending
    # --------------------------------------------------------

    print_section("COMBINE TRENDING + NON-TRENDING")

    videos = pl.concat(
        [
            trending,
            non_trending,
        ],
        how="vertical_relaxed",
    )

    print(f"Combined rows: {videos.height:,}")

    # --------------------------------------------------------
    # 7. Count duplicate video IDs
    # --------------------------------------------------------

    duplicate_count = (
        videos.height
        - videos.select("video_id").unique().height
    )

    print(f"Duplicate video IDs: {duplicate_count:,}")

    # --------------------------------------------------------
    # 8. Deduplicate
    # --------------------------------------------------------
    #
    # If a video exists in both datasets, keep the first row.
    # Trending is intentionally placed first above.
    # --------------------------------------------------------

    videos = videos.unique(
        subset=["video_id"],
        keep="first",
        maintain_order=True,
    )

    print(f"Unique videos: {videos.height:,}")

    # --------------------------------------------------------
    # 9. Prepare dislikes
    # --------------------------------------------------------

    print_section("PREPARE DISLIKES")

    dislikes = dislikes.select(
        [
            "video_id",
            "dislikes",
            "rating",
            "deleted",
            "source",
        ]
    )

    # Rename source so it does not conflict with YouTube source.
    dislikes = dislikes.rename(
        {
            "source": "dislike_source",
        }
    )

    # Ensure one row per video_id.
    dislikes_before_dedup = dislikes.height

    dislikes = dislikes.unique(
        subset=["video_id"],
        keep="first",
    )

    dislikes_duplicates = (
        dislikes_before_dedup - dislikes.height
    )

    print(
        f"Dislikes duplicate rows removed: "
        f"{dislikes_duplicates:,}"
    )

    print(
        f"Unique dislikes records: "
        f"{dislikes.height:,}"
    )

    # --------------------------------------------------------
    # 10. Join video metadata + dislikes
    # --------------------------------------------------------

    print_section("JOIN VIDEO DATA + DISLIKES")

    merged = videos.join(
        dislikes,
        on="video_id",
        how="left",
    )

    print(f"Merged rows: {merged.height:,}")

    # --------------------------------------------------------
    # 11. Validate row count
    # --------------------------------------------------------

    if merged.height != videos.height:
        raise RuntimeError(
            "ERROR: Row count changed after dislikes join. "
            "This indicates duplicate video_id values "
            "may exist in the dislikes dataset."
        )

    print("Row count validation: OK")

    # --------------------------------------------------------
    # 12. Check dislikes matching
    # --------------------------------------------------------

    matched = merged.filter(
        pl.col("dislikes").is_not_null()
    ).height

    unmatched = merged.height - matched

    match_rate = (
        matched / merged.height * 100
        if merged.height > 0
        else 0
    )

    print(f"Dislikes matched : {matched:,}")
    print(f"Dislikes missing : {unmatched:,}")
    print(f"Match rate       : {match_rate:.2f}%")

    # --------------------------------------------------------
    # 13. Final column order
    # --------------------------------------------------------

    final_columns = [
        "video_id",
        "title",
        "description",
        "published_at",
        "channel_id",
        "channel_title",
        "category_id",
        "category_name",
        "duration",
        "view_count",
        "like_count",
        "comment_count",
        "is_trending",
        "dislikes",
        "rating",
        "deleted",
        "source",
        "dislike_source",
    ]

    merged = merged.select(final_columns)

    # --------------------------------------------------------
    # 14. Save merged Parquet
    # --------------------------------------------------------

    print_section("SAVE MERGED DATA")

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    merged.write_parquet(OUTPUT_FILE)

    print(f"Output: {OUTPUT_FILE}")
    print(f"Rows  : {merged.height:,}")

    # --------------------------------------------------------
    # 15. Final schema
    # --------------------------------------------------------

    print_section("FINAL SCHEMA")

    for index, column in enumerate(merged.columns, start=1):
        print(f"{index:2}. {column}")

    # --------------------------------------------------------
    # 16. Final validation
    # --------------------------------------------------------

    print_section("FINAL VALIDATION")

    unique_video_ids = merged.select(
        "video_id"
    ).unique().height

    if unique_video_ids != merged.height:
        raise RuntimeError(
            "ERROR: Duplicate video_id detected "
            "in final dataset."
        )

    print("Unique video_id: OK")
    print("Final row count : OK")
    print("Schema          : OK")
    print()
    print("MERGE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
