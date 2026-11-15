import pyarrow.dataset as ds
import s3fs

# Kết nối qua mạng Tailscale vào máy chủ MinIO của leader
fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': 'http://100.79.241.73:9000'},
    key='minioadmin', 
    secret='minioadmin'
)

bucket = "youtube-raw"

def count_rows(path):
    try:
        dataset = ds.dataset(path, filesystem=fs, format="parquet")
        return dataset.count_rows()
    except Exception as e:
        return 0  

print("Đang quét máy chủ MinIO...")
linh_trending = count_rows(f"{bucket}/member_linh/trending/")
linh_nontrending = count_rows(f"{bucket}/member_linh/non-trending/")

bao_trending = count_rows(f"{bucket}/member_bao/trending/")
bao_nontrending = count_rows(f"{bucket}/member_bao/non-trending/")

print("\n=== TỔNG KẾT RECORD TRÊN MINIO ===")
print(f"1. Linh (Trending): {linh_trending:,} records")
print(f"2. Linh (Non-Trending): {linh_nontrending:,} records")
print(f"3. Bảo (Trending): {bao_trending:,} records")
print(f"4. Bảo (Non-Trending): {bao_nontrending:,} records")
print("-" * 35)
print(f"TỔNG CỘNG DATA: {linh_trending + linh_nontrending + bao_trending + bao_nontrending:,} records")