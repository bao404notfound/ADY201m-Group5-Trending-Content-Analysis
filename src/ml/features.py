import pandas as pd
import numpy as np
import re

# 1. TỪ ĐIỂN TỪ KHÓA ĐƯỢC CHIA LÀM 2 CẤP ĐỘ
STRONG_KEYWORDS = {
    'EN': ['REVEALED', 'EXPOSED', 'SECRET', 'WARNING', 'CONFESSION', 'GONE WRONG'],
    'DE': ['GEHEIM', 'ENTHÜLLT', 'SKANDAL', 'WARNUNG', 'DIE WAHRHEIT'],
    'BR': ['SEGREDO', 'REVELADO', 'URGENTE', 'ESCÂNDALO', 'A VERDADE', 'PERIGO'],
    'ID': ['RAHASIA', 'TERBONGKAR', 'BAHAYA', 'SKANDAL', 'FAKTA SEBENARNYA'],
    'VN': ['TIẾT LỘ', 'BÓC PHỐT', 'SỰ THẬT', 'CẢNH BÁO', 'SỐC', 'LỘ', 'BI KỊCH', 'GÓC KHUẤT'],
    'TH': ['ความลับ', 'แฉ', 'เตือนภัย', 'ความจริง'],
    'HI': ['रहस्य', 'खुलासा', 'चेतावनी', 'सच', 'खतरा'],
    'JP': ['閲覧注意', '極秘', '炎上'],
    'KR': ['최초공개', '진실', '비밀']
}

MEDIUM_KEYWORDS = {
    'EN': ['UNBELIEVABLE', 'INSANE', 'MUST WATCH', 'BEFORE IT IS DELETED'],
    'DE': ['UNGLAUBLICH', 'HEFTIG', 'KRASS', 'INSIDER'],
    'BR': ['INACREDITÁVEL', 'INSANO', 'VEJA AGORA'],
    'ID': ['GILA', 'TONTON SEKARANG', 'PARAH'],
    'VN': ['KHÔNG THỂ TIN NỔI', 'XEM NGAY', 'CỰC ĐỈNH', 'ĐỪNG BỎ LỠ'],
    'TH': ['เหลือเชื่อ', 'ต้องดู', 'ดราม่า'],
    'HI': ['अविश्वसनीय', 'अभी देखें'],
    'JP': ['衝撃', 'ヤバい', '神回', '爆笑'],
    'KR': ['충격', '대박', '역대급', '꼭 보세요', '소름']
}

# Tự động gộp toàn bộ từ khóa các ngôn ngữ thành 2 chuỗi Regex chuẩn xác
STRONG_PATTERN = "|".join(re.escape(kw) for kw_list in STRONG_KEYWORDS.values() for kw in kw_list)
MEDIUM_PATTERN = "|".join(re.escape(kw) for kw_list in MEDIUM_KEYWORDS.values() for kw in kw_list)

def build_features(df: pd.DataFrame, training_columns=None) -> pd.DataFrame:
    """
    Hàm chuẩn hóa dữ liệu thô thành ma trận số học cho ML.
    """
    data = df.copy()

    # 1. Xử lý thời gian đăng để tính số ngày tồn tại
    data['publish_at'] = pd.to_datetime(data['publish_at'], errors='coerce')
    data['crawled_at'] = pd.to_datetime(data['crawled_at'], errors='coerce').fillna(pd.Timestamp.now())
    
    days_diff = (data['crawled_at'] - data['publish_at']).dt.total_seconds() / 86400.0
    data['days_published'] = np.clip(days_diff, a_min=0.0416, a_max=None).fillna(1.0)

    # 2. Tính Vận tốc View (View Velocity) & Logarit hóa
    data['view_count'] = pd.to_numeric(data['view_count'], errors='coerce').fillna(0)
    data['like_count'] = pd.to_numeric(data['like_count'], errors='coerce').fillna(0)
    data['comment_count'] = pd.to_numeric(data['comment_count'], errors='coerce').fillna(0)

    data['views_per_day'] = data['view_count'] / data['days_published']
    data['log_views_per_day'] = np.log1p(data['views_per_day'])

    # 3. Tính Tỷ lệ tương tác (Engagement Rates)
    safe_views = np.where(data['view_count'] > 0, data['view_count'], 1)
    data['like_rate'] = data['like_count'] / safe_views
    data['comment_rate'] = data['comment_count'] / safe_views

    # 4. Chấm điểm Clickbait theo 2 cấp độ (2 = Cực mạnh, 1 = Phóng đại, 0 = Bình thường)
    data['title'] = data['title'].fillna("").astype(str)
    has_strong = data['title'].str.contains(STRONG_PATTERN, flags=re.IGNORECASE, regex=True)
    has_medium = data['title'].str.contains(MEDIUM_PATTERN, flags=re.IGNORECASE, regex=True)
    
    data['clickbait_score'] = np.select([has_strong, has_medium], [2, 1], default=0)
    data['title_length'] = data['title'].str.len()

    # 5. Chuẩn hóa biến phân loại (Đã loại bỏ 'region' để chống Data Leakage)
    for col in ['category_name', 'time_block']:
        if col not in data.columns:
            data[col] = "Unknown"
        else:
            data[col] = data[col].fillna("Unknown").astype(str)

    feature_cols = [
        'log_views_per_day', 'like_rate', 'comment_rate', 
        'clickbait_score', 'title_length', 
        'category_name', 'time_block'
    ]
    X = data[feature_cols]

    # One-Hot Encoding
    X_encoded = pd.get_dummies(X, columns=['category_name', 'time_block'], drop_first=False)
    
    for col in X_encoded.columns:
        if X_encoded[col].dtype == 'bool':
            X_encoded[col] = X_encoded[col].astype(int)

    # Ép khuôn cột khi dự đoán data thật
    if training_columns is not None:
        X_encoded = X_encoded.reindex(columns=training_columns, fill_value=0)

    return X_encoded