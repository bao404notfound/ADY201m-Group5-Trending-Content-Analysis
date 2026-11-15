import os
import joblib
import pandas as pd
from sqlalchemy import create_engine
from features import build_features

PG_URI = "postgresql+psycopg2://youtube_user:youtube_pass@localhost:5432/youtube_analysis"
MODEL_DIR = "model"
OUTPUT_RESULT_FILE = "ket_qua_du_doan.csv"

def predict_latest_non_trending():
    # 1. Nạp bộ não AI và danh sách cột đã lưu
    model_path = os.path.join(MODEL_DIR, "rf_trending_model.pkl")
    cols_path = os.path.join(MODEL_DIR, "model_columns.pkl")
    
    if not os.path.exists(model_path):
        raise FileNotFoundError("Chưa có file model! Chạy 'python src/ml/train_model.py' trước nhé.")

    model = joblib.load(model_path)
    training_columns = joblib.load(cols_path)

    # 2. Kết nối PostgreSQL kéo các video MỚI NHẤT hiện đang Non-Trending
    print("1. ĐANG KẾT NỐI POSTGRESQL ĐỂ TÌM VIDEO TIỀM NĂNG...")
    engine = create_engine(PG_URI)
    
    # Lấy bản ghi mới nhất của các video xuất bản trong 7 ngày qua nhưng CHƯA lên Trending
    query = """
    WITH latest_metrics AS (
        SELECT 
            video_id, crawled_at, view_count, like_count, comment_count, is_trending,
            ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY crawled_at DESC) as rn
        FROM fact_youtube_metrics
    )
    SELECT 
        d.video_id, d.title, d.channel_title, d.category_name, d.region, 
        d.publish_at, d.time_block,
        f.crawled_at, f.view_count, f.like_count, f.comment_count, f.is_trending
    FROM dim_video d
    JOIN latest_metrics f ON d.video_id = f.video_id
    WHERE f.rn = 1 
      AND f.is_trending = FALSE
      AND d.publish_at >= CURRENT_DATE - INTERVAL '60 days';
    """
    
    df_real = pd.read_sql(query, engine)
    
    if df_real.empty:
        print("Không tìm thấy video Non-Trending nào trong 7 ngày qua. Thử tăng phạm vi ngày lên nhé!")
        return

    print(f"-> Đã tìm thấy {len(df_real):,} videos mới đang ở trạng thái Non-Trending.")

    # 3. Ép toàn bộ data thật qua phễu chuẩn hóa đặc trưng
    print("2. ĐANG TÍNH TOÁN VẬN TỐC VIEW & MÃ HÓA ĐẶC TRƯNG...")
    X_real_encoded = build_features(df_real, training_columns=training_columns)

    # 4. Chạy dự đoán hàng loạt
    print("3. AI ĐANG CHẤM ĐIỂM XÁC SUẤT LÊN TRENDING...")
    predictions = model.predict(X_real_encoded)
    probabilities = model.predict_proba(X_real_encoded)[:, 1] # Xác suất lớp 1 (Trending)

    # 5. Gắn kết quả vào bảng gốc và sắp xếp từ cao xuống thấp
    df_result = df_real.copy()
    df_result['du_doan_trending'] = predictions
    df_result['trang_thai_du_doan'] = df_result['du_doan_trending'].map({
        1: "CÓ KHẢ NĂNG TRENDING", 
        0: "Bình thường"
    })
    df_result['xac_suat_trending (%)'] = (probabilities * 100).round(2)

    df_result = df_result.sort_values(by='xac_suat_trending (%)', ascending=False)

    # 6. Xuất ra file CSV và in Top 10 ứng viên sáng giá nhất
    df_result.to_csv(OUTPUT_RESULT_FILE, index=False, encoding="utf-8-sig")
    print(f"\nHOÀN TẤT! Đã xuất danh sách chấm điểm ra file: '{OUTPUT_RESULT_FILE}'")
    
    so_video_tiem_nang = (df_result['du_doan_trending'] == 1).sum()
    print(f"Phát hiện {so_video_tiem_nang} video có dấu hiệu bứt phá lên Top Trending!\n")

    print("TOP 10 VIDEO NON-TRENDING CÓ XÁC SUẤT BỨT PHÁ CAO NHẤT:")
    cols_to_show = ['video_id', 'region', 'view_count', 'xac_suat_trending (%)', 'trang_thai_du_doan', 'title']
    print(df_result[cols_to_show].head(10).to_string(index=False))

if __name__ == "__main__":
    predict_latest_non_trending()