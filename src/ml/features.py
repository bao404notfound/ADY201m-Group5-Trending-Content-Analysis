import pandas as pd
import numpy as np
import re

# Bộ từ khóa Clickbait đa ngôn ngữ từ phần EDA
CLICKBAIT_PATTERN = r"shocking|leaked|omg|unbelievable|secret|exposed|insane|gone wrong|sốc|lộ|kinh hoàng|bất ngờ|bí mật|vạch trần|không thể tin|chấn động"

def build_features(df: pd.DataFrame, training_columns=None) -> pd.DataFrame:
    """
    Hàm chuẩn hóa dữ liệu thô thành ma trận số học cho ML.
    - Nếu training_columns = None: Đang ở chế độ TRAIN (tự sinh cột One-Hot).
    - Nếu training_columns != None: Đang ở chế độ DỰ ĐOÁN DATA THẬT (ép khuôn đúng cột lúc Train).
    """
    data = df.copy()

    # 1. Xử lý thời gian đăng để tính số ngày tồn tại (tránh chia cho 0 bằng cách lấy tối thiểu 1 giờ = 0.0416 ngày)
    data['publish_at'] = pd.to_datetime(data['publish_at'], errors='coerce')
    data['crawled_at'] = pd.to_datetime(data['crawled_at'], errors='coerce').fillna(pd.Timestamp.now())
    
    days_diff = (data['crawled_at'] - data['publish_at']).dt.total_seconds() / 86400.0
    data['days_published'] = np.clip(days_diff, a_min=0.0416, a_max=None).fillna(1.0)

    # 2. Tính Vận tốc View (View Velocity) & Logarit hóa chống lệch phân phối
    data['view_count'] = pd.to_numeric(data['view_count'], errors='coerce').fillna(0)
    data['like_count'] = pd.to_numeric(data['like_count'], errors='coerce').fillna(0)
    data['comment_count'] = pd.to_numeric(data['comment_count'], errors='coerce').fillna(0)

    data['views_per_day'] = data['view_count'] / data['days_published']
    data['log_views_per_day'] = np.log1p(data['views_per_day'])

    # 3. Tính Tỷ lệ tương tác (Engagement Rates) - Vũ khí cực mạnh để nhận diện video viral
    safe_views = np.where(data['view_count'] > 0, data['view_count'], 1)
    data['like_rate'] = data['like_count'] / safe_views
    data['comment_rate'] = data['comment_count'] / safe_views

    # 4. Trích xuất đặc trưng từ Tiêu đề (Clickbait & Độ dài tiêu đề)
    data['title'] = data['title'].fillna("").astype(str)
    data['has_clickbait'] = data['title'].str.contains(CLICKBAIT_PATTERN, flags=re.IGNORECASE, regex=True).astype(int)
    data['title_length'] = data['title'].str.len()

    # 5. Chuẩn hóa các biến phân loại (Categorical)
    for col in ['category_name', 'time_block', 'region']:
        if col not in data.columns:
            data[col] = "Unknown"
        else:
            data[col] = data[col].fillna("Unknown").astype(str)

    # Chọn đúng các cột đặc trưng đưa vào mô hình (Loại bỏ ID, raw text để chống Data Leakage)
    feature_cols = [
        'log_views_per_day', 'like_rate', 'comment_rate', 
        'has_clickbait', 'title_length', 
        'category_name', 'time_block', 'region'
    ]
    X = data[feature_cols]

    # One-Hot Encoding
    X_encoded = pd.get_dummies(X, columns=['category_name', 'time_block', 'region'], drop_first=False)
    
    # Chuyển toàn bộ kiểu bool (True/False) sang int (1/0)
    for col in X_encoded.columns:
        if X_encoded[col].dtype == 'bool':
            X_encoded[col] = X_encoded[col].astype(int)

    # Nếu đang dự đoán data thật, ép khuôn cột cho khớp 100% với lúc Train
    if training_columns is not None:
        X_encoded = X_encoded.reindex(columns=training_columns, fill_value=0)

    return X_encoded