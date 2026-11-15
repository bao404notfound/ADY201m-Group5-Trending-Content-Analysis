import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split

# --- BƯỚC 0: TỰ ĐỘNG ĐỊNH VỊ THƯ MỤC DATA ---
# Lấy đường dẫn hiện tại của file code (nằm trong src/processing/)
# Lùi về 2 cấp để ra thư mục gốc của dự án, sau đó trỏ vào thư mục data/
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
data_dir = os.path.join(project_root, 'data')

# Tạo thư mục data nếu vô tình nó bị xóa mất (tránh lỗi văng code)
os.makedirs(data_dir, exist_ok=True)

# --- BƯỚC 1: KẾT NỐI DATABASE VÀ MÓC DỮ LIỆU ---
# (Nhớ thay thông tin user, password, tên_database của máy bà vào đây)
db_url = 'postgresql://postgres:linhqn1234@localhost:5432/youtube_analytics'
engine = create_engine(db_url)

# Dùng SQL JOIN thẳng 2 bảng fact_youtube_metrics và dim_video theo video_id
query = """
    SELECT 
        f.is_trending, f.log_views_per_day, f.has_clickbait, f.crawled_at,
        d.video_id, d.channel_id, d.title, d.category_name, d.time_block, d.publish_at
    FROM fact_youtube_metrics f
    JOIN dim_video d ON f.video_id = d.video_id
"""
# Đổ dữ liệu từ Database ra DataFrame
df = pd.read_sql(query, engine)

# --- BƯỚC 2: CHỐNG DATA LEAKAGE (RÒ RỈ DỮ LIỆU) ---
# Xóa ngay lập tức các cột mang tính nhận diện hoặc không có giá trị học thuật.
columns_to_drop = ['video_id', 'channel_id', 'title', 'crawled_at', 'publish_at']
df = df.drop(columns=columns_to_drop, errors='ignore')

# --- BƯỚC 3: ONE-HOT ENCODING (MÃ HÓA BIẾN PHÂN LOẠI) ---
# Tách cột Text (category_name, time_block) thành nhiều cột 0 và 1.
df = pd.get_dummies(df, columns=['category_name', 'time_block'], dtype=int)

# --- BƯỚC 4: CHIA TẬP TRAIN - TEST VÀ BẢO TOÀN SỰ CHÊNH LỆCH ---
# Tách riêng Biến mục tiêu (y) và Các biến đặc trưng (X)
X = df.drop(columns=['is_trending']) 
y = df['is_trending']                

# Cắt data 80-20. Dùng stratify=y để giữ nguyên tỷ lệ Trending gốc.
X_train, X_test, y_train, y_test = train_test_split(
    X, y, 
    test_size=0.2,     
    stratify=y,        
    random_state=42    
)

# --- BƯỚC 5: XUẤT 4 FILE CSV VÀO THƯ MỤC DATA ---
X_train.to_csv(os.path.join(data_dir, 'X_train.csv'), index=False)
X_test.to_csv(os.path.join(data_dir, 'X_test.csv'), index=False)
y_train.to_csv(os.path.join(data_dir, 'y_train.csv'), index=False)
y_test.to_csv(os.path.join(data_dir, 'y_test.csv'), index=False)

print(f"Đã xử lý xong! 4 file CSV đang nằm gọn gàng tại: {data_dir}")