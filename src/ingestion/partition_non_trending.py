#!/usr/bin/env python3
from __future__ import annotations
import re
import sys
import shutil
from pathlib import Path
import polars as pl

PROJECT_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = PROJECT_DIR / "data/raw/youtube/non_trending/non_trending_raw.parquet"
OUTPUT_DIR = PROJECT_DIR / "data/processed/member_linh/non_trending"
TIMEZONE = "Asia/Ho_Chi_Minh"

CATEGORY_MAPPING = {
    "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music", 
    "15": "Pets & Animals", "17": "Sports", "19": "Travel & Events", 
    "20": "Gaming", "22": "People & Blogs", "23": "Comedy", 
    "24": "Entertainment", "25": "News & Politics", "26": "Howto & Style", 
    "27": "Education", "28": "Science & Technology", "29": "Nonprofits & Activism"
}

def safe_part(value: object, default: str = "Unknown") -> str:
    if value is None: return default
    value = str(value).strip()
    if not value or value.lower() in {"none", "null", "nan"}: return default
    return re.sub(r"[^A-Za-z0-9._ -]+", "_", value).replace(" ", "_")

def main() -> int:
    if not INPUT_FILE.exists():
        print(f"[ERROR] Không tìm thấy file: {INPUT_FILE}", file=sys.stderr)
        return 1

    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    df = pl.read_parquet(INPUT_FILE)
    if df.is_empty(): return 1

    pub_col = next((c for c in ("published_at", "publishedAt", "publish_time") if c in df.columns), None)
    if pub_col:
        parsed = pl.col(pub_col).cast(pl.String).str.to_datetime(strict=False, time_zone="UTC")
        df = df.with_columns(parsed.dt.convert_time_zone(TIMEZONE).alias("_published_local"))
        df = df.with_columns([
            pl.col("_published_local").dt.strftime("%Y-%m-%d").fill_null("Unknown").alias("publish_date"),
            pl.when(
                (pl.col("_published_local").dt.weekday() <= 5) & 
                (pl.col("_published_local").dt.hour() >= 8) & 
                (pl.col("_published_local").dt.hour() < 17)
            ).then(pl.lit("Office-hours")).otherwise(pl.lit("Off-hours")).alias("time_block"),
        ])
    else:
        df = df.with_columns([pl.lit("Unknown").alias("publish_date"), pl.lit("Unknown").alias("time_block")])

    cat_col = next((c for c in ("category_name", "category_id", "categoryId") if c in df.columns), None)
    if cat_col == "category_name":
        category_expr = pl.col(cat_col).map_elements(safe_part, return_dtype=pl.String)
    elif cat_col in ("category_id", "categoryId"):
        category_expr = pl.col(cat_col).cast(pl.Utf8).replace(CATEGORY_MAPPING).fill_null("Unknown").map_elements(safe_part, return_dtype=pl.String)
    else:
        category_expr = pl.lit("Unknown")
        
    df = df.with_columns(category_expr.alias("_category"))

    for keys, group in df.partition_by(["_category", "publish_date", "time_block"], as_dict=True, maintain_order=True).items():
        category, publish_date, time_block = keys
        out_dir = OUTPUT_DIR / f"category={safe_part(category)}" / f"publish_date={safe_part(publish_date)}" / f"time_block={safe_part(time_block)}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "non_trending.parquet"
        group.drop(["_published_local", "_category"], strict=False).write_parquet(out_file, compression="zstd")

    print(f"[OK] Đã phân vùng {df.height} non-trending records vào {OUTPUT_DIR}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())