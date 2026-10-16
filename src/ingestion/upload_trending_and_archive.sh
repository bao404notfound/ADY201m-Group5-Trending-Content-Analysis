#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$HOME/ADY201m-Group5-Trending-Content-Analysis"
PYTHON="$PROJECT_DIR/.venv/bin/python"
PARTITION_SCRIPT="$PROJECT_DIR/src/ingestion/partition_trending.py"
CRAWLER_SCRIPT="$PROJECT_DIR/src/crawler/crawl_trending.py"
INPUT_DIR="$PROJECT_DIR/data/raw/youtube/trending"
PARTITION_DIR="$PROJECT_DIR/data/processed/member_linh/trending"
ARCHIVE_DIR="$INPUT_DIR/archive"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/ingestion.log"
MINIO_TARGET="myminio/youtube-raw/member_linh/trending"

mkdir -p "$LOG_DIR" "$ARCHIVE_DIR"
exec >> "$LOG_FILE" 2>&1

echo "=================================================="
echo "START PIPELINE TRENDING (LINH): $(date -Iseconds)"
cd "$PROJECT_DIR"

if ! mc ready myminio >/dev/null 2>&1; then
  echo "[ERROR] MinIO not available. Check connection."
  exit 1
fi
echo "[OK] MinIO ready"

CURRENT_EPOCH=$(date +%s)
TARGET_EPOCH=$(date -d "05:00:00" +%s)

# -- 1. Cào Trending (Nó sẽ quét lấy data mới nhất) --
echo "[STEP 1] Crawling trending snapshot..."
"$PYTHON" "$CRAWLER_SCRIPT"

if [ "$CURRENT_EPOCH" -ge "$TARGET_EPOCH" ]; then
  # -- 2. Băm tất cả file (cũ + mới) --
  echo "[STEP 2] Partitioning trending snapshots..."
  "$PYTHON" "$PARTITION_SCRIPT"

  # -- 3. Đẩy lên MinIO bằng lệnh mc --
  echo "[STEP 3] Uploading partitions (idempotent)..."
  UPLOAD_ERRORS=0
  if [[ -d "$PARTITION_DIR" ]]; then
    while IFS= read -r -d '' file; do
      relative="${file#"$PARTITION_DIR"/}"
      destination="$MINIO_TARGET/$relative"

      if mc stat "$destination" >/dev/null 2>&1; then
        continue
      fi

      if ! mc cp "$file" "$destination"; then
        UPLOAD_ERRORS=$((UPLOAD_ERRORS + 1))
      fi
    done < <(find "$PARTITION_DIR" -type f -name '*.parquet' -print0)
  fi

  if [[ $UPLOAD_ERRORS -gt 0 ]]; then
    echo "[ERROR] $UPLOAD_ERRORS upload(s) failed. Raw snapshots NOT archived."
    exit 1
  fi
  echo "[OK] All trending partitions uploaded"

  # -- 4. Archive (Dọn dẹp file đã đẩy) --
  ARCHIVED=0
  shopt -s nullglob
  for file in "$INPUT_DIR"/trending_*.parquet; do
    base="$(basename "$file")"
    snapshot="${base#trending_}"
    snapshot="${snapshot%.parquet}"
    SNAPSHOT_PARTITIONS=$(find "$PARTITION_DIR" -type f -name "trending_${snapshot}.parquet" 2>/dev/null | wc -l)
    if [[ $SNAPSHOT_PARTITIONS -gt 0 ]]; then
      mv "$file" "$ARCHIVE_DIR/$base"
      ARCHIVED=$((ARCHIVED + 1))
    fi
  done
  echo "[OK] Archived $ARCHIVED raw snapshot(s)"
else
  echo "[INFO] Before 5:00 AM. Skipping upload."
fi

echo "END pipeline: $(date -Iseconds)"
echo "=================================================="