#!/usr/bin/env python3
"""
backfill_trending_raw.py
Backfills the legacy trending_raw.parquet (missing `crawled_at` column only;
`region` already exists in this file).

Saves a new snapshot file compatible with partition_trending.py.
The original trending_raw.parquet is NOT modified or deleted.

Usage (from repo root):
    python src/ingestion/backfill_trending_raw.py
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

PROJECT_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = PROJECT_DIR / "data/raw/youtube/trending/trending_raw.parquet"
OUTPUT_DIR = PROJECT_DIR / "data/raw/youtube/trending"

CATEGORIES = {
    "10": "Music",
    "1": "Film & Animation",
    "28": "Science & Technology",
    "17": "Sports",
    "20": "Gaming",
    "25": "News & Politics",
}


def main() -> None:
    if not INPUT_FILE.exists():
        print(f"[ERROR] Not found: {INPUT_FILE}")
        return

    df = pl.read_parquet(INPUT_FILE)
    print(f"Loaded {df.height} rows from {INPUT_FILE.name}")
    print(f"Columns: {df.columns}")

    # --- crawled_at: use file modification time ---
    file_mtime = INPUT_FILE.stat().st_mtime
    crawled_at = datetime.fromtimestamp(file_mtime, tz=timezone.utc)
    crawled_at_str = crawled_at.isoformat()
    timestamp = crawled_at.strftime("%Y%m%d_%H%M")
    print(f"Using crawled_at (file mtime): {crawled_at_str}")

    # --- Add crawled_at column ---
    df = df.with_columns(
        pl.lit(crawled_at_str)
        .str.to_datetime(time_zone="UTC")
        .alias("crawled_at")
    )

    # --- Normalise published_at to timezone-aware Datetime ---
    df = df.with_columns(
        pl.col("published_at")
        .cast(pl.String)
        .str.to_datetime(strict=False, time_zone="UTC")
        .alias("published_at")
    )

    # --- Ensure category_name exists ---
    if "category_name" not in df.columns:
        df = df.with_columns(
            pl.col("category_id")
            .map_elements(
                lambda cid: CATEGORIES.get(str(cid), "Unknown"),
                return_dtype=pl.String,
            )
            .alias("category_name")
        )

    # --- Ensure source exists ---
    if "source" not in df.columns:
        df = df.with_columns(pl.lit("youtube_mostPopular_legacy").alias("source"))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / f"trending_backfill_{timestamp}.parquet"
    df.write_parquet(out_file, compression="zstd")

    print(f"\n[OK] Saved {df.height} rows -> {out_file}")
    print("Region distribution:")
    print(df.group_by("region").len().sort("len", descending=True))
    print("\nSchema:")
    print(df.schema)


if __name__ == "__main__":
    main()
