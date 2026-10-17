-- Khởi tạo cấu trúc Database PostgreSQL cho YouTube Trending Analysis

-- 1. Bảng Dimension: Lưu thông tin tĩnh của Video
CREATE TABLE IF NOT EXISTS dim_video (
    video_id VARCHAR(50) PRIMARY KEY,
    channel_id VARCHAR(50),
    channel_title TEXT,
    title TEXT NOT NULL,
    category VARCHAR(100),
    published_at TIMESTAMP,
    publish_date DATE,
    time_block VARCHAR(20), -- 'Office-hours' hoặc 'Off-hours'
    is_trending BOOLEAN DEFAULT TRUE -- Phân biệt video Trending và Non-Trending
);

-- 2. Bảng Fact: Lưu các chỉ số biến động theo thời gian (Snapshot)
CREATE TABLE IF NOT EXISTS fact_metrics (
    metric_id SERIAL PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    view_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    CONSTRAINT fk_video
        FOREIGN KEY (video_id) 
        REFERENCES dim_video (video_id)
        ON DELETE CASCADE
);

-- 3. Tạo Index để tối ưu tốc độ truy vấn khi phân tích
CREATE INDEX IF NOT EXISTS idx_fact_video_id ON fact_metrics(video_id);
CREATE INDEX IF NOT EXISTS idx_dim_category ON dim_video(category);
CREATE INDEX IF NOT EXISTS idx_dim_time_block ON dim_video(time_block);
CREATE INDEX IF NOT EXISTS idx_dim_is_trending ON dim_video(is_trending);
