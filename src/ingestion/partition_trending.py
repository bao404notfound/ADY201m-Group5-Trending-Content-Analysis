#!/usr/bin/env python3
from __future__ import annotations
import re
import sys
from pathlib import Path
import polars as pl

PROJECT_DIR = Path(__file__).resolve().parents[2]
INPUT_DIR = PROJECT_DIR / "data/raw/youtube/trending"
OUTPUT_DIR = PROJECT_DIR / "data/processed/member_linh/trending"
ARCHIVE_DIR = INPUT_DIR / "archive"
TIMEZONE = "Asia/Ho_Chi_Minh"
TARGET_REGIONS = ["US", "GB", "DE", "BR"]

def safe_part(value: object, default: str = "Unknown") -> str:
    if value is None: return default
    value = str(value).strip()
    if not value or value.lower() in {"none", "null", "nan"}: return default
    return re.sub(r"[^A-Za-z0-9._ -]+", "_", value).replace(" ", "_")

def local_datetime_expr(column: str) -> pl.Expr:
    parsed = pl.col(column).cast(pl.String).str.to_datetime(strict=False, time_zone="UTC")
    return parsed.dt.convert_time_zone(TIMEZONE)

def main() -> int:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    inputs = sorted(p for p in INPUT_DIR.glob("trending_*.parquet") if p.is_file())
    if not inputs:
        print(f"Không có file raw nào mới trong {INPUT_DIR}")
        return 0

    failures = 0
    for source in inputs:
        try:
            df = pl.read_parquet(source)
            if df.is_empty(): continue
            if "published_at" not in df.columns: raise ValueError("Thiếu cột published_at")

            df = df.with_columns(local_datetime_expr("published_at").alias("_published_local"))
            df = df.with_columns([
                pl.col("_published_local").dt.strftime("%Y-%m-%d").alias("publish_date"),
                pl.when(
                    (pl.col("_published_local").dt.weekday() <= 5) & 
                    (pl.col("_published_local").dt.hour() >= 8) & 
                    (pl.col("_published_local").dt.hour() < 17)
                ).then(pl.lit("Office-hours")).otherwise(pl.lit("Off-hours")).alias("time_block"),
            ])

            if "category_name" in df.columns:
                category_expr = pl.col("category_name").map_elements(safe_part, return_dtype=pl.String)
            elif "category_id" in df.columns:
                category_expr = pl.col("category_id").map_elements(safe_part, return_dtype=pl.String)
            else:
                category_expr = pl.lit("Unknown")
            df = df.with_columns(category_expr.alias("_category"))

            if "region" not in df.columns:
                df = df.with_columns(pl.lit("Unknown").alias("region"))
            else:
                df = df.with_columns(pl.col("region").cast(pl.String).fill_null("Unknown"))

            # Lọc khắt khe chỉ lấy đúng 4 quốc gia yêu cầu
            df = df.filter(pl.col("region").is_in(TARGET_REGIONS))

            snapshot = source.stem.removeprefix("trending_")
            output_count = 0
            for keys, group in df.partition_by(["region", "_category", "publish_date", "time_block"], as_dict=True, maintain_order=True).items():
                region, category, publish_date, time_block = keys
                out_dir = (
                    OUTPUT_DIR 
                    / f"region={safe_part(region)}" 
                    / f"category={safe_part(category)}" 
                    / f"publish_date={safe_part(publish_date)}" 
                    / f"time_block={safe_part(time_block)}"
                )
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"trending_{snapshot}.parquet"
                
                group.drop(["_published_local", "_category"]).write_parquet(out_file, compression="zstd")
                output_count += 1

            print(f"[OK] {source.name}: {df.height} rows -> {output_count} partitions (Lọc theo {TARGET_REGIONS})")
        except Exception as exc:
            failures += 1
            print(f"[ERROR] Lỗi tại file {source.name}: {exc}", file=sys.stderr)

    return 1 if failures else 0

if __name__ == "__main__":
    raise SystemExit(main())