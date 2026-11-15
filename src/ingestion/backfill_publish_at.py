import pandas as pd
from sqlalchemy import create_engine, text
from googleapiclient.discovery import build
import math
import time

# ==========================================
# CẤU HÌNH
# ==========================================
YOUTUBE_API_KEY = "AIzaSyBdtK_BKtCZTef1B3MWenWpSRLt_mlo9GA" # Điền key của nhóm vào
PG_URI = "postgresql+psycopg2://youtube_user:youtube_pass@localhost:5432/youtube_analysis"

def main():
    print("1. KẾT NỐI DATABASE VÀ TÌM CÁC VIDEO BỊ THIẾU PUBLISH_AT...")
    engine = create_engine(PG_URI)
    
    # Lấy danh sách video bị thiếu ngày đăng
    query_missing = "SELECT video_id FROM dim_video WHERE publish_at IS NULL;"
    df_missing = pd.read_sql(query_missing, engine)
    
    if df_missing.empty:
        print("Tuyệt vời! Không có video nào bị thiếu publish_at. Dừng chương trình.")
        return
        
    video_ids = df_missing['video_id'].tolist()
    total_missing = len(video_ids)
    print(f"Phát hiện {total_missing} video cần Backfill.")

    print("2. KHỞI TẠO YOUTUBE API...")
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    # API YouTube chỉ cho phép tối đa 50 ID mỗi lần gọi
    batch_size = 50
    total_batches = math.ceil(total_missing / batch_size)
    
    updates_made = 0
    
    print("3. BẮT ĐẦU CHẠY BACKFILL (GỌI API VÀ CẬP NHẬT DATABASE)...")
    with engine.begin() as conn: # Mở một Transaction
        for i in range(total_batches):
            batch_ids = video_ids[i * batch_size : (i + 1) * batch_size]
            ids_string = ",".join(batch_ids)
            
            try:
                # Gọi API với part="snippet" để lấy ngày đăng
                request = youtube.videos().list(
                    part="snippet",
                    id=ids_string
                )
                response = request.execute()
                
                # Duyệt qua kết quả trả về và Update thẳng vào PostgreSQL
                for item in response.get("items", []):
                    vid = item["id"]
                    pub_at = item["snippet"]["publishedAt"]
                    
                    # Cập nhật Database
                    update_query = text("""
                        UPDATE dim_video 
                        SET publish_at = :pub_at 
                        WHERE video_id = :vid
                    """)
                    conn.execute(update_query, {"pub_at": pub_at, "vid": vid})
                    updates_made += 1
                
                print(f"Đã xử lý xong batch {i+1}/{total_batches}...")
                
            except Exception as e:
                print(f"Lỗi ở batch {i+1}: {e}")
            
            # Nghỉ 0.5s để tránh bị YouTube khóa API vì spam quá nhanh (Rate Limit)
            time.sleep(0.5)

    print("-" * 50)
    print(f"BACKFILL HOÀN TẤT! Đã cập nhật thành công {updates_made}/{total_missing} video.")
    print("-" * 50)

if __name__ == "__main__":
    main()