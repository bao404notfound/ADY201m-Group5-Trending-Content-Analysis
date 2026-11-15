import polars as pl
import sys
import random
from datetime import datetime, timedelta
import re
import numpy as np  # TUI THÊM THƯ VIỆN NÀY ĐỂ RANDOM SIÊU TỐC BẰNG MA TRẬN

# ==========================================
# 1. CẤU HÌNH KẾT NỐI
# ==========================================
MINIO_STORAGE_OPTIONS = {
    "endpoint_url": "http://100.79.241.73:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "aws_region": "us-east-1"
}

MINIO_DATA_PATH = "s3://youtube-raw/**/*.parquet"
PG_URI = "postgresql://postgres:linhqn1234@100.95.205.7:5432/youtube_analytics"

# Đã xóa hàm Python simulate_crawled_at cũ vì nó gây treo máy (bottleneck)

def load_all_parquets():
    """
    Nạp dữ liệu mượt mà bằng s3fs, dùng diagonal concat để vá schema bị lệch.
    Đã bổ sung bộ lọc "tẩy" Timezone, rút trích mã Region trực tiếp từ đường dẫn file,
    Và thêm log theo dõi tiến độ.
    """
    try:
        import s3fs
        fs = s3fs.S3FileSystem(
            key=MINIO_STORAGE_OPTIONS["aws_access_key_id"],
            secret=MINIO_STORAGE_OPTIONS["aws_secret_access_key"],
            client_kwargs={
                "endpoint_url": MINIO_STORAGE_OPTIONS["endpoint_url"],
                "region_name": MINIO_STORAGE_OPTIONS["aws_region"]
            }
        )
        file_list = [f"s3://{f}" for f in fs.glob("youtube-raw/**/*.parquet")]
        if not file_list:
            raise ValueError("Không tìm thấy file parquet nào trong bucket!")
            
        dfs = []
        total_files = len(file_list)
        print(f"-> Tìm thấy {total_files} file Parquet. Bắt đầu đọc dữ liệu...")
        
        for i, file_path in enumerate(file_list):
            # In tiến độ sau mỗi 20 file để màn hình không bị "đứng yên"
            if i % 20 == 0 and i > 0:
                print(f"   ... Đã đọc {i}/{total_files} files ...")
                
            try:
                # 1. ĐỌC FILE LÊN TRƯỚC
                df_single = pl.read_parquet(
                    file_path, 
                    storage_options=MINIO_STORAGE_OPTIONS,
                    hive_partitioning=False
                )
                
                # 2. XỬ LÝ REGEX ĐỂ MÓC REGION TỪ ĐƯỜNG DẪN NGAY SAU KHI ĐỌC
                match = re.search(r'/([A-Z]{2})/', file_path)
                if match:
                    real_region = match.group(1)
                    df_single = df_single.with_columns(pl.lit(real_region).alias("region"))
                
                # 3. TẨY MÚI GIỜ (TIMEZONE) CHO TẤT CẢ CỘT DATETIME
                for col_name, dtype in df_single.schema.items():
                    if isinstance(dtype, pl.Datetime):
                        df_single = df_single.with_columns(
                            pl.col(col_name).dt.replace_time_zone(None)
                        )
                        
                dfs.append(df_single)
            except Exception as e:
                print(f"   [Cảnh báo] Bỏ qua file lỗi {file_path}: {e}")
                
        if not dfs:
            raise ValueError("Không có dữ liệu hợp lệ nào được đọc!")
            
        print("-> Gom (Concat) tất cả các file lại thành 1 bảng duy nhất...")
        return pl.concat(dfs, how="diagonal_relaxed")
        
    except Exception as e:
        print(f"Lỗi khi gom file Parquet: {e}")
        sys.exit(1)

def main():
    print("BƯỚC 1: HÚT VÀ LÀM SẠCH DỮ LIỆU THÔ TỪ MINIO...")
    try:
        df_raw = load_all_parquets()
        df_raw = df_raw.drop_nulls(subset=["video_id"])
        
        # Đồng bộ hóa tên cột
        if "published_at" in df_raw.columns:
            df_raw = df_raw.rename({"published_at": "publish_at"})
        elif "publishedAt" in df_raw.columns:
            df_raw = df_raw.rename({"publishedAt": "publish_at"})
            
        if "channelId" in df_raw.columns:
            df_raw = df_raw.rename({"channelId": "channel_id"})
        if "channelTitle" in df_raw.columns:
            df_raw = df_raw.rename({"channelTitle": "channel_title"})

        # ==========================================
        # 1. FIX ÉP KIỂU DATETIME (Đa định dạng chống rớt Null)
        # ==========================================
        if "publish_at" in df_raw.columns and df_raw.schema["publish_at"] == pl.Utf8:
            df_raw = df_raw.with_columns(
                pl.coalesce([
                    pl.col("publish_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False),
                    pl.col("publish_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S.%.fZ", strict=False),
                    pl.col("publish_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                ]).alias("publish_at")
            )

        # ==========================================
        # 2. GIẢI QUYẾT TRIỆT ĐỂ VỤ CATEGORY (GỘP THÀNH 6 NHÓM)
        # ==========================================
        ORIGINAL_MAPPING = {
            "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music", 
            "15": "Pets & Animals", "17": "Sports", "19": "Travel & Events", 
            "20": "Gaming", "22": "People & Blogs", "23": "Comedy", 
            "24": "Entertainment", "25": "News & Politics", "26": "Howto & Style", 
            "27": "Education", "28": "Science & Technology"
        }
        
        GROUPING_LOGIC = {
            "Comedy": "Film & Animation",
            "Entertainment": "Film & Animation",
            "Pets & Animals": "Film & Animation",
            "Education": "Science & Technology",
            "Howto & Style": "Science & Technology",
            "People & Blogs": "News & Politics",
            "Travel & Events": "News & Politics",
            "Autos & Vehicles": "Sports"
        }

        if "category_name" not in df_raw.columns:
            df_raw = df_raw.with_columns(pl.lit(None).cast(pl.Utf8).alias("category_name"))

        if "category_id" in df_raw.columns:
            df_raw = df_raw.with_columns(
                pl.when(pl.col("category_name").is_null() | (pl.col("category_name") == "Unknown"))
                .then(pl.col("category_id").cast(pl.Utf8).replace(ORIGINAL_MAPPING))
                .otherwise(pl.col("category_name"))
                .alias("category_name")
            )
            
        df_raw = df_raw.with_columns(
            pl.col("category_name").replace(GROUPING_LOGIC).fill_null("Unknown")
        )

        # ==========================================
        # 3. FIX CỨNG THỂ LOẠI BẰNG TỪ KHÓA
        # ==========================================
        if "title" in df_raw.columns:
            df_raw = df_raw.with_columns(
                pl.when(pl.col("title").str.to_lowercase().str.contains("phim|movie|tập phim|kinh dị|hoạt hình"))
                .then(pl.lit("Film & Animation"))
                .when(pl.col("title").str.to_lowercase().str.contains("nhạc|remix|mv|lofi|bolero|karaoke|bass"))
                .then(pl.lit("Music"))
                .otherwise(pl.col("category_name"))
                .alias("category_name")
            )

        # ==========================================
        # 4. FIX REGION NULL VÀ CHÈN CÁC CỘT THIẾU
        # ==========================================
        for col in ["channel_title", "channel_id", "region"]:
            if col not in df_raw.columns:
                default_val = "UN" if col == "region" else "Unknown"
                df_raw = df_raw.with_columns(pl.lit(default_val).cast(pl.Utf8).alias(col))
            else:
                default_val = "UN" if col == "region" else "Unknown"
                df_raw = df_raw.with_columns(pl.col(col).fill_null(default_val))

        # ==========================================
        # 5. XỬ LÝ CRAWLED_AT BẰNG VECTORIZATION (TỐC ĐỘ BÀN THỜ)
        # ==========================================
        if "crawled_at" not in df_raw.columns:
            df_raw = df_raw.with_columns(pl.lit(None).cast(pl.Datetime).alias("crawled_at"))
        elif df_raw.schema["crawled_at"] == pl.Utf8:
            df_raw = df_raw.with_columns(
                pl.coalesce([
                    pl.col("crawled_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False),
                    pl.col("crawled_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S.%.fZ", strict=False),
                    pl.col("crawled_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                ]).alias("crawled_at")
            )

        # --- BẮT ĐẦU FIX LỖI TREO MÁY Ở ĐÂY ---
        print("-> Đang vá lỗi cột crawled_at (NULL) bằng thuật toán Vectorized...")
        N = df_raw.height
        cutoff = datetime(2026, 9, 14)
        
        # 5.1. Tạo mảng Numpy random bù giờ cho toàn bộ file cùng 1 lúc (chỉ mất ~0.01 giây)
        # offset_pre: 0 đến 14 ngày (tính bằng phút)
        # offset_post: 2h đến 72h (tính bằng phút)
        df_raw = df_raw.with_columns([
            pl.Series("offset_pre", np.random.randint(0, 14*24*60, size=N)),
            pl.Series("offset_post", np.random.randint(120, 72*60, size=N))
        ])

        # 5.2. Tính toán mô phỏng crawled_at không dùng hàm map_elements
        df_raw = df_raw.with_columns(
            pl.when(pl.col("publish_at").is_null())
            .then(pl.lit(datetime.now()))
            .when(pl.col("publish_at") < cutoff)
            .then(pl.lit(cutoff) + pl.duration(minutes=pl.col("offset_pre")))
            .otherwise(pl.col("publish_at") + pl.duration(minutes=pl.col("offset_post")))
            .alias("simulated_crawled_at")
        )

        # 5.3. Đắp cái vừa mô phỏng vào những ô bị Null của crawled_at gốc và xóa cột tạm
        df_raw = df_raw.with_columns(
            pl.when(pl.col("crawled_at").is_null())
            .then(pl.col("simulated_crawled_at"))
            .otherwise(pl.col("crawled_at"))
            .alias("crawled_at")
        ).drop(["offset_pre", "offset_post", "simulated_crawled_at"])
        # --- KẾT THÚC FIX ---

        # Chốt chặn cuối cùng: Điền mốc thời gian mặc định nếu vẫn còn sót Null
        df_raw = df_raw.with_columns(
            pl.col("crawled_at").fill_null(datetime(2026, 9, 14, 0, 0, 0))
        )

        print(f"Đã nạp và lọc sơ bộ {df_raw.height} records hợp lệ từ Data Lake.")
    except Exception as e:
        print(f"Lỗi hệ thống trong Bước 1: {e}")
        sys.exit(1)

    print("\nBƯỚC 2: TÁCH VÀ XỬ LÝ BẢNG DIMENSION (dim_video)...")
    
    dim_cols = ["video_id", "title", "category_name", "channel_title", "channel_id", "region", "publish_at"]
    if "time_block" in df_raw.columns:
        dim_cols.append("time_block")
        
    df_dim_new = df_raw.select(dim_cols).unique(subset=["video_id"], keep="last")

    try:
        df_dim_existing = pl.read_database("SELECT video_id FROM dim_video", PG_URI)
        if not df_dim_existing.is_empty():
            df_dim_new = df_dim_new.filter(~pl.col("video_id").is_in(df_dim_existing["video_id"]))
    except Exception as e:
        print("Bảng dim_video trống, tiến hành nạp toàn bộ.")

    print(f"Chuẩn bị nạp {df_dim_new.height} videos mới vào dim_video.")

    print("\nBƯỚC 3: TÁCH VÀ XỬ LÝ BẢNG FACT (fact_youtube_metrics)...")
    if "is_trending" not in df_raw.columns:
        df_raw = df_raw.with_columns(pl.lit(False).alias("is_trending"))
    else:
        df_raw = df_raw.with_columns(pl.col("is_trending").cast(pl.Boolean).fill_null(False))

    df_fact = df_raw.select([
        "video_id", "crawled_at", "view_count", "like_count", 
        "comment_count", "is_trending"
    ])
    
    df_fact = df_fact.unique(subset=["video_id", "crawled_at"], keep="last")
    print(f"Chuẩn bị nạp {df_fact.height} records đo lường vào fact_youtube_metrics.")

    print("\nBƯỚC 4: BẮT ĐẦU BULK INSERT VÀO POSTGRESQL...")
    try:
        if df_dim_new.height > 0:
            df_dim_new.write_database(
                table_name="dim_video",
                connection=PG_URI,
                if_table_exists="append",
                engine="sqlalchemy"
            )
            print("Đã ghi xong bảng dim_video!")
        else:
            print("Không có video mới nào cần thêm vào dim_video.")

        df_fact.write_database(
            table_name="fact_youtube_metrics",
            connection=PG_URI,
            if_table_exists="append",
            engine="sqlalchemy"
        )
        print("Đã ghi xong bảng fact_youtube_metrics!")

    except Exception as e:
        print(f"Lỗi khi Bulk Insert vào Database: {e}")

    print("\nETL HOÀN TẤT!")

if __name__ == "__main__":
    main()