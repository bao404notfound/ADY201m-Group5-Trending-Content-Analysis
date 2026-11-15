import polars as pl

# Đường dẫn tới thư mục chứa data đã băm của bà
DIR = "data/processed/member_linh/trending/**/*.parquet"

def main():
    try:
        # Đọc và gộp tất cả các file parquet trong thư mục
        df = pl.scan_parquet(DIR).collect()
        
        print("=======================================")
        print("THỐNG KÊ DỮ LIỆU TRENDING ĐÃ CÀO")
        print("=======================================")
        print(f"1. Tổng số records (tích lũy qua các ngày) : {df.height:,} dòng")
        
        # Đếm số lượng video độc nhất (không tính trùng lặp nếu 1 video lọt top nhiều ngày)
        unique_videos = df.select("video_id").unique().height
        print(f"2. Tổng số video độc nhất                  : {unique_videos:,} video")
        
    except Exception as e:
        print(f"Chưa có dữ liệu hoặc đường dẫn sai. Lỗi: {e}")

if __name__ == "__main__":
    main()