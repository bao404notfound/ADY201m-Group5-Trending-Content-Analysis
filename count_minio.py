import io
import polars as pl
from minio import Minio
from collections import defaultdict

def main():
    client = Minio(
        "127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False 
    )

    bucket_name = "youtube-raw"

    if not client.bucket_exists(bucket_name):
        print(f"[LỖI] Bucket '{bucket_name}' không tồn tại trên server này.")
        return

    print(f"Đang quét toàn bộ dữ liệu trong bucket '{bucket_name}' để thống kê chi tiết...\n")
    
    objects = client.list_objects(bucket_name, recursive=True)
    
    stats = defaultdict(lambda: {"trending": 0, "non-trending": 0})
    video_ids_per_member = defaultdict(set)
    
    # Biến lưu trữ tổng hợp toàn cục để tính unique toàn Bucket
    global_trending_ids = set()
    global_nontrending_ids = set()
    
    total_files = 0
    
    for obj in objects:
        if not obj.object_name.endswith(".parquet"):
            continue
            
        total_files += 1
        parts = obj.object_name.split("/")
        if len(parts) >= 2:
            member_name = parts[0]
            dataset_type = "non-trending" if "non" in parts[1].lower() else "trending"
        else:
            member_name = "unknown"
            dataset_type = "unknown"

        try:
            response = client.get_object(bucket_name, obj.object_name)
            parquet_bytes = response.read()
            
            df = pl.read_parquet(io.BytesIO(parquet_bytes))
            stats[member_name][dataset_type] += df.height
            
            if "video_id" in df.columns:
                ids = df["video_id"].to_list()
                
                # Lưu ID cho từng cá nhân
                video_ids_per_member[member_name].update(ids)
                
                # Lưu ID cho tổng toàn cục
                if dataset_type == "trending":
                    global_trending_ids.update(ids)
                elif dataset_type == "non-trending":
                    global_nontrending_ids.update(ids)
                
        except Exception as e:
            print(f"  [LỖI] Không thể đọc file {obj.object_name}: {e}")
        finally:
            response.close()
            response.release_conn()
            
    print("==========================================")
    print("      BÁO CÁO CHI TIẾT TỪNG THÀNH VIÊN    ")
    print("==========================================")
    print(f"Tổng số file Parquet đã quét: {total_files}")
    print("-" * 42)
    
    for member, data in stats.items():
        total_member_records = data['trending'] + data['non-trending']
        print(f"Thành viên: {member.upper()}")
        print(f"   - Trending    : {data['trending']:,} records")
        print(f"   - Non-trending: {data['non-trending']:,} records")
        print(f"   - Tổng cộng   : {total_member_records:,} records")
        print(f"   - Video độc nhất (Unique IDs): {len(video_ids_per_member[member]):,}")
        print("-" * 42)

    members = list(video_ids_per_member.keys())
    if len(members) >= 2:
        print("PHÂN TÍCH TRÙNG LẶP GIỮA CÁC THÀNH VIÊN")
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                mem1 = members[i]
                mem2 = members[j]
                overlap = video_ids_per_member[mem1].intersection(video_ids_per_member[mem2])
                print(f"   - Số video trùng lặp giữa {mem1} và {mem2}: {len(overlap):,} video")
    else:
        print("Chưa có đủ dữ liệu từ 2 thành viên trở lên để đối chiếu trùng lặp.")
        
    print("\n==========================================")
    print("      BÁO CÁO TỔNG QUAN TOÀN BỘ BUCKET    ")
    print("==========================================")
    print(f"Tổng Unique Trending    : {len(global_trending_ids):,} video")
    print(f"Tổng Unique Non-trending: {len(global_nontrending_ids):,} video")
    
    # Hợp nhất 2 set để ra con số tổng cuối cùng (lọc toàn bộ trùng lặp chéo)
    global_all_ids = global_trending_ids.union(global_nontrending_ids)
    
    print("-" * 42)
    print(f"TỔNG SỐ RECORD THỰC TẾ (ĐÃ LỌC TRÙNG): {len(global_all_ids):,} video")
    print("==========================================")

if __name__ == "__main__":
    main()