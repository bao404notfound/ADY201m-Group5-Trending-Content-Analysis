import os
import joblib
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
from features import build_features

PG_URI = "postgresql+psycopg2://youtube_user:youtube_pass@localhost:5432/youtube_analysis"
MODEL_DIR = "model"
os.makedirs(MODEL_DIR, exist_ok=True)

def main():
    print("1. KẾT NỐI POSTGRESQL VÀ LẤY DỮ LIỆU HUẤN LUYỆN...")
    engine = create_engine(PG_URI)
    
    # Lấy bản ghi mới nhất của từng video trong năm 2026 (hoặc bỏ điều kiện năm nếu muốn train toàn bộ)
    query = """
    WITH latest_metrics AS (
        SELECT 
            video_id, crawled_at, view_count, like_count, comment_count, is_trending,
            ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY crawled_at DESC) as rn
        FROM fact_youtube_metrics
    )
    SELECT 
        d.video_id, d.title, d.category_name, d.region, d.publish_at, d.time_block,
        f.crawled_at, f.view_count, f.like_count, f.comment_count, f.is_trending
    FROM dim_video d
    JOIN latest_metrics f ON d.video_id = f.video_id
    WHERE f.rn = 1 AND d.publish_at >= '2026-01-01';
    """
    df_raw = pd.read_sql(query, engine)
    print(f"-> Đã tải {len(df_raw):,} videos hợp lệ.")

    print("\n2. TIỀN XỬ LÝ VÀ TẠO ĐẶC TRƯNG (FEATURE ENGINEERING)...")
    X = build_features(df_raw, training_columns=None)
    y = df_raw['is_trending'].astype(int)
    print(f"-> Số lượng đặc trưng đầu vào: {X.shape[1]} cột.")
    print(f"-> Tỷ lệ Trending thực tế: {y.mean()*100:.2f}%")

    print("\n3. CHIA TẬP TRAIN (80%) VÀ TEST (20%)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print("\n4. HUẤN LUYỆN RANDOM FOREST (COST-SENSITIVE LEARNING)...")
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=8,              # Giới hạn độ sâu chống học vẹt (Overfitting)
        min_samples_split=10,
        class_weight='balanced',  # Phạt nặng khi đoán sai lớp Trending thiểu số
        random_state=42,
        n_jobs=-1                 # Tận dụng tối đa nhân CPU để chạy nhanh
    )
    model.fit(X_train, y_train)

    print("\n5. ĐÁNH GIÁ MÔ HÌNH TRÊN TẬP KIỂM THỬ (TEST SET):")
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    print("-" * 55)
    print(classification_report(y_test, y_pred, target_names=['Non-Trending (0)', 'Trending (1)']))
    print(f"Điểm ROC-AUC: {roc_auc_score(y_test, y_prob):.4f}")
    print("Ma trận nhầm lẫn (Confusion Matrix):")
    print(confusion_matrix(y_test, y_pred))
    print("-" * 55)

    # In Top 5 yếu tố quyết định việc lên Trending
    importances = pd.DataFrame({
        'Feature': X.columns,
        'Importance': model.feature_importances_
    }).sort_values(by='Importance', ascending=False)
    print("\nTOP 5 YẾU TỐ QUAN TRỌNG NHẤT:")
    print(importances.head(5).to_string(index=False))

    print("\n6. LƯU BỘ NÃO AI VÀ CẤU TRÚC CỘT...")
    joblib.dump(model, os.path.join(MODEL_DIR, "rf_trending_model.pkl"))
    joblib.dump(list(X.columns), os.path.join(MODEL_DIR, "model_columns.pkl"))
    print(f"-> Hoàn tất! Đã lưu model vào thư mục '{MODEL_DIR}/'.")

if __name__ == "__main__":
    main()