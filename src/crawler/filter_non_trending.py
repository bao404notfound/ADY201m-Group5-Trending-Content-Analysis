import polars as pl
from pathlib import Path


TRENDING_FILE = (
    "data/raw/youtube/trending/"
    "trending_raw.parquet"
)

CANDIDATE_FILE = (
    "data/raw/youtube/non_trending/"
    "candidate_ids.parquet"
)

OUTPUT_FILE = (
    "data/raw/youtube/non_trending/"
    "non_trending_ids.parquet"
)


def main():

    trending = (
        pl.read_parquet(TRENDING_FILE)
        .select("video_id")
        .unique()
    )

    candidates = (
        pl.read_parquet(CANDIDATE_FILE)
        .unique(subset=["video_id"])
    )

    print("Trending:", trending.height)
    print("Candidates:", candidates.height)

    non_trending = candidates.filter(
        ~pl.col("video_id").is_in(
            trending["video_id"].implode()
        )
    )

    non_trending = non_trending.unique(
        subset=["video_id"]
    )

    output = Path(OUTPUT_FILE)

    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    non_trending.write_parquet(
        output,
        compression="zstd"
    )

    print("\n" + "=" * 60)
    print("NON-TRENDING IDS")
    print("=" * 60)

    print(
        "Non-trending:",
        non_trending.height
    )

    print("Saved:", output)


if __name__ == "__main__":
    main()
