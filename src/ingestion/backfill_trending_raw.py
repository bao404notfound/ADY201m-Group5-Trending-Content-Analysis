#!/usr/bin/env python3
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
import polars as pl

PROJECT_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = PROJECT_DIR / "data/raw/youtube/trending/trending_raw.parquet"
OUTPUT_DIR = PROJECT_DIR / "data/raw/youtube/trending"

CATEGORIES = {
    "10": "Music", "1": "Film & Animation", "28": "Science & Technology",
    "17": "Sports", "20": "Gaming", "25": "News & Politics",
}

def main() -> None:
    if not INPUT_FILE.exists():
        print(f"[ERROR] Không tìm thấy: {INPUT_FILE}")
        return

    df = pl.read_parquet(INPUT_FILE)
    print(f"Đã load {df.height} dòng từ {INPUT_FILE.name}")

    file_mtime = INPUT_FILE.stat().st_mtime
    crawled_at = datetime.fromtimestamp(file_mtime, tz=timezone.utc)
    crawled_at_str = crawled_at.isoformat()
    timestamp = crawled_at.strftime("%Y%m%d_%H%M")

    df = df.with_columns(
        pl.lit(crawled_at_str).str.to_datetime(time_zone="UTC").alias("crawled_at")
    )
    df = df.with_columns(
        pl.col("published_at").cast(pl.String).str.to_datetime(strict=False, time_zone="UTC").alias("published_at")
    )

    if "category_name" not in df.columns:
        df = df.with_columns(
            pl.col("category_id").map_elements(lambda cid: CATEGORIES.get(str(cid), "Unknown"), return_dtype=pl.String).alias("category_name")
        )

    if "source" not in df.columns:
        df = df.with_columns(pl.lit("youtube_mostPopular_legacy").alias("source"))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / f"trending_backfill_{timestamp}.parquet"
    df.write_parquet(out_file, compression="zstd")

    print(f"\n[OK] Đã lưu {df.height} dòng -> {out_file}")

if __name__ == "__main__":
    main()