#!/usr/bin/env python3
"""Partition unprocessed Trending snapshots by category, Vietnam publish date, and time block."""
from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

PROJECT_DIR = Path(__file__).resolve().parents[2]
INPUT_DIR = PROJECT_DIR / "data/raw/youtube/trending"
OUTPUT_DIR = PROJECT_DIR / "data/processed/trending_partitions"
ARCHIVE_DIR = INPUT_DIR / "archive"
TIMEZONE = "Asia/Ho_Chi_Minh"

def safe_part(value: object, default: str = "Unknown") -> str:
    if value is None:
        return default
    value = str(value).strip()
    if not value or value.lower() in {"none", "null", "nan"}:
        return default
    value = re.sub(r"[^A-Za-z0-9._ -]+", "_", value)
    return value.replace(" ", "_")

def local_datetime_expr(column: str) -> pl.Expr:
    # Handles timezone-aware strings (Z/offset) and naive datetimes. Naive YouTube
    # publishedAt values should not occur; treating naive values as UTC is safest.
    parsed = pl.col(column).cast(pl.String).str.to_datetime(strict=False, time_zone="UTC")
    return parsed.dt.convert_time_zone(TIMEZONE)

def main() -> int:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    inputs = sorted(p for p in INPUT_DIR.glob("trending_*.parquet") if p.is_file())
    if not inputs:
        print(f"No unprocessed snapshots found in {INPUT_DIR}")
        return 0

    failures = 0
    for source in inputs:
        try:
            df = pl.read_parquet(source)
            if df.is_empty():
                print(f"[SKIP] Empty file: {source.name}")
                continue
            if "published_at" not in df.columns:
                raise ValueError("Required column 'published_at' is missing")

            df = df.with_columns(local_datetime_expr("published_at").alias("_published_local"))
            df = df.with_columns([
                pl.col("_published_local").dt.strftime("%Y-%m-%d").alias("publish_date"),
                pl.when(
                    (pl.col("_published_local").dt.weekday() <= 5)
                    & (pl.col("_published_local").dt.hour() >= 8)
                    & (pl.col("_published_local").dt.hour() < 17)
                )
                .then(pl.lit("Office-hours"))
                .otherwise(pl.lit("Off-hours"))
                .alias("time_block"),
            ])

            if "category_name" in df.columns:
                category_expr = pl.col("category_name").map_elements(
                    lambda x: safe_part(x), return_dtype=pl.String
                )
            elif "category_id" in df.columns:
                category_expr = pl.col("category_id").map_elements(
                    lambda x: safe_part(x), return_dtype=pl.String
                )
            else:
                category_expr = pl.lit("Unknown")
            df = df.with_columns(category_expr.alias("_category"))

            snapshot = source.stem.removeprefix("trending_")
            output_count = 0
            for keys, group in df.partition_by(
                ["_category", "publish_date", "time_block"], as_dict=True, maintain_order=True
            ).items():
                category, publish_date, time_block = keys
                out_dir = (
                    OUTPUT_DIR
                    / f"category={safe_part(category)}"
                    / f"publish_date={safe_part(publish_date)}"
                    / f"time_block={safe_part(time_block)}"
                )
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"trending_{snapshot}.parquet"
                group.drop(["_published_local", "_category"]).write_parquet(out_file, compression="zstd")
                output_count += 1

            print(f"[OK] {source.name}: {df.height} rows -> {output_count} partition file(s)")
        except Exception as exc:
            failures += 1
            print(f"[ERROR] {source.name}: {exc}", file=sys.stderr)

    return 1 if failures else 0

if __name__ == "__main__":
    raise SystemExit(main())
