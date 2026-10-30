import os
from minio import Minio
from minio.error import S3Error

# ==========================================
# 1. CẤU HÌNH KẾT NỐI MINIO & ĐƯỜNG DẪN
# ==========================================
MINIO_ENDPOINT = "100.79.241.73:9000"  # IP Tailscale từ hình của bạn (API dùng cổng 9000, Web dùng 9001)
ACCESS_KEY = "minioadmin" # Thay bằng username bạn dùng đăng nhập web MinIO
SECRET_KEY = "minioadmin" # Thay bằng password bạn dùng đăng nhập web MinIO

BUCKET_NAME = "youtube-raw"
MEMBER_FOLDER = "member_dat"           # Thư mục của riêng bạn trên MinIO

# Đường dẫn tới thư mục chứa dữ liệu bạn đã crawl trên máy Ubuntu
# Ví dụ: nếu trong thư mục './data_crawl' của bạn đã chia sẵn 2 folder 'trending' và 'non-trending'
LOCAL_DATA_DIR = "./data_crawl"        

def upload_folder_to_minio():
    # Khởi tạo kết nối
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False # Dùng HTTP thường qua Tailscale
    )

    # Kiểm tra kết nối và bucket 'youtube-raw'
    if not client.bucket_exists(BUCKET_NAME):
        print(f"❌ Lỗi: Không tìm thấy bucket '{BUCKET_NAME}'!")
        return

    print(f"🚀 Bắt đầu đẩy dữ liệu từ '{LOCAL_DATA_DIR}' lên '{BUCKET_NAME}/{MEMBER_FOLDER}/'...")

    count = 0
    # Quét toàn bộ thư mục và file bên trong LOCAL_DATA_DIR trên Ubuntu
    for root, dirs, files in os.walk(LOCAL_DATA_DIR):
        for file in files:
            # Bỏ qua các file ẩn của hệ thống (nếu có)
            if file.startswith('.'):
                continue

            # 1. Đường dẫn thực tế của file trên máy Ubuntu
            local_file_path = os.path.join(root, file)

            # 2. Tính toán đường dẫn tương đối để giữ nguyên cấu trúc thư mục con (trending / non-trending)
            relative_path = os.path.relpath(local_file_path, LOCAL_DATA_DIR)
            
            # 3. Ghép thành đường dẫn trên MinIO: member_dat/trending/ten_file.json
            # (Đổi dấu \ thành / để chuẩn xác trên MinIO)
            minio_object_name = f"{MEMBER_FOLDER}/{relative_path}".replace("\\", "/")

            try:
                # Đẩy file lên MinIO
                client.fput_object(
                    bucket_name=BUCKET_NAME,
                    object_name=minio_object_name,
                    file_path=local_file_path
                )
                count += 1
                print(f"✅ [{count}] Đã đẩy: {minio_object_name}")
            except S3Error as err:
                print(f"❌ Lỗi khi đẩy file {local_file_path}: {err}")

    print(f"\n🎉 Hoàn tất! Đã đẩy tổng cộng {count} file lên thư mục '{MEMBER_FOLDER}'.")

if __name__ == "__main__":
    upload_folder_to_minio()