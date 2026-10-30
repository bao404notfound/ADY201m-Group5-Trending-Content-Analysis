import polars as pl
import os
import shutil
from pathlib import Path
from datetime import datetime

def main():
    project_dir = Path(os.path.expanduser('~/ADY201m-Group5-Trending-Content-Analysis'))
    trending_dir = project_dir / 'data' / 'raw' / 'youtube' / 'trending'
    non_trending_path = project_dir / 'data' / 'raw' / 'youtube' / 'non_trending' / 'non_trending_raw.parquet'
    backup_path = non_trending_path.with_name("non_trending_raw_backup.parquet")

    # ==========================================================
    # CẤU HÌNH NGƯỠNG LỌC 
    # ==========================================================
    TARGET_TOTAL = 100000
    CUTOFF_DATE = datetime(2020, 1, 1)

    # ==========================================================
    # 1. SAO LƯU DỮ LIỆU GỐC & LOAD TRENDING
    # ==========================================================
    if non_trending_path.exists() and not backup_path.exists():
        shutil.copy2(non_trending_path, backup_path)
        print(f"Đã backup bản gốc an toàn ra: {backup_path}")

    trending_files = list(trending_dir.rglob("*.parquet"))
    if not trending_files:
        print("Không tìm thấy file Trending nào.")
        return

    df_trending_list = [pl.read_parquet(f) for f in trending_files]
    df_trending_all = pl.concat(df_trending_list, how="diagonal_relaxed").unique(subset=["video_id"])
    trending_count = len(df_trending_all)
    print(f"Tổng số video Trending độc nhất: {trending_count}")

    target_non_trending = TARGET_TOTAL - trending_count
    if target_non_trending <= 0:
        print("Trending đã vượt 100k. Không cần thêm Non-trending.")
        return

    # ==========================================================
    # 2. LOAD NON-TRENDING & CATEGORY BUCKETING (GOM CỤM)
    # ==========================================================
    if not non_trending_path.exists():
        print("Không tìm thấy file Non-trending gốc.")
        return

    df_non_trending = pl.read_parquet(non_trending_path).unique(subset=["video_id"])
    
    # Từ điển gom các thể loại ngách vào 6 siêu danh mục cốt lõi của Trending
    CATEGORY_BUCKETS = {
        "24": "1",   # Entertainment -> Film & Animation
        "23": "1",   # Comedy -> Film & Animation
        "15": "1",   # Pets & Animals -> Film & Animation
        "27": "28",  # Education -> Science & Technology
        "26": "28",  # Howto & Style -> Science & Technology
        "22": "25",  # People & Blogs -> News & Politics
        "19": "25",  # Travel & Events -> News & Politics
        "2": "17"    # Autos & Vehicles -> Sports
    }
    allowed_categories = ['1', '10', '17', '20', '25', '28']
    
    cat_col = next((c for c in ("categoryId", "category_id") if c in df_non_trending.columns), None)
    if cat_col:
        df_non_trending = df_non_trending.with_columns(
            pl.col(cat_col).cast(pl.Utf8).replace(CATEGORY_BUCKETS)
        )
        df_non_trending = df_non_trending.filter(pl.col(cat_col).is_in(allowed_categories))
        print(f"Số Non-trending sau khi gom cụm (Bucketing): {len(df_non_trending)}")

    # ==========================================================
    # 3. ÉP KIỂU DATETIME & LỌC THỜI GIAN
    # ==========================================================
    if df_non_trending.schema["published_at"] == pl.Utf8:
        df_non_trending = df_non_trending.with_columns(
            pl.col("published_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
        )
    else:
        df_non_trending = df_non_trending.with_columns(pl.col("published_at").cast(pl.Datetime, strict=False))

    df_filtered = df_non_trending.filter(pl.col("published_at") >= CUTOFF_DATE)
    print(f"Số Non-trending ĐẠT CHUẨN THỜI GIAN: {len(df_filtered)}")

    if len(df_filtered) <= target_non_trending:
        df_pruned = df_filtered
    else:
        df_pruned = df_filtered.sort("published_at", descending=True).head(target_non_trending)

    print(f"Đã chốt sổ tập Non-trending: {len(df_pruned)} records.")
    
    # ==========================================================
    # 4. GHI ĐÈ VÀ KÍCH HOẠT PHÂN VÙNG
    # ==========================================================
    df_pruned.write_parquet(non_trending_path)
    print("Đã ghi đè file non_trending_raw.parquet thành công.")
    print("Đang chạy script phân vùng (partition)...")
    os.system(f"python3 {project_dir}/src/ingestion/partition_non_trending.py")
    print("Thành công! Dữ liệu đã sẵn sàng để đẩy lên MinIO.")

if __name__ == "__main__":
    main()