#!/usr/bin/env bash
# upload_trending_and_archive.sh
# Orchestrates: crawl -> partition -> idempotent upload -> archive
# Run: bash src/ingestion/upload_trending_and_archive.sh
set -Eeuo pipefail

PROJECT_DIR="$HOME/ADY201m-Group5-Trending-Content-Analysis"
PYTHON="$PROJECT_DIR/.venv/bin/python"
PARTITION_SCRIPT="$PROJECT_DIR/src/ingestion/partition_trending.py"
CRAWLER_SCRIPT="$PROJECT_DIR/src/crawler/crawl_trending.py"
INPUT_DIR="$PROJECT_DIR/data/raw/youtube/trending"
PARTITION_DIR="$PROJECT_DIR/data/processed/trending_partitions"
ARCHIVE_DIR="$INPUT_DIR/archive"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/ingestion.log"
MINIO_TARGET="myminio/youtube-raw/member_bao/trending"

mkdir -p "$LOG_DIR" "$ARCHIVE_DIR"
exec >> "$LOG_FILE" 2>&1

echo "=================================================="
echo "START pipeline: $(date -Iseconds)"

cd "$PROJECT_DIR"

# -- 0. MinIO health check --
if ! mc ready myminio >/dev/null 2>&1; then
  echo "[ERROR] MinIO not available. Start MinIO first, then re-run."
  exit 1
fi
echo "[OK] MinIO ready"

# Calculate timestamps for the 5 AM threshold
CURRENT_EPOCH=$(date +%s)
TARGET_EPOCH=$(date -d "2026-10-10 05:00:00" +%s)

# -- 1. Crawl snapshots --
echo "[STEP 1] Crawling trending snapshot..."
"$PYTHON" "$CRAWLER_SCRIPT"
echo "[OK] Trending Crawl done"

echo "[STEP 1B] Crawling non-trending candidates (recent 2026)..."
"$PYTHON" "$PROJECT_DIR/src/crawler/crawl_non_trending_ids.py"
"$PYTHON" "$PROJECT_DIR/src/crawler/crawl_non_trending_metadata.py"
echo "[OK] Non-Trending Crawl done"

if [ "$CURRENT_EPOCH" -ge "$TARGET_EPOCH" ]; then
  echo "[INFO] Current time is past 5:00 AM. Proceeding to prune, partition, and upload..."

  # -- 2. Partition data --
  echo "[STEP 2] Partitioning trending snapshots..."
  "$PYTHON" "$PARTITION_SCRIPT"
  echo "[OK] Trending Partition done"

  echo "[STEP 2B] Pruning & Partitioning non-trending..."
  "$PYTHON" "$PROJECT_DIR/src/ingestion/prune_dataset.py"
  echo "[OK] Non-Trending Partition done"

# -- 3. Idempotent upload --
echo "[STEP 3] Uploading partitions (idempotent)..."
UPLOAD_ERRORS=0
if [[ -d "$PARTITION_DIR" ]]; then
  while IFS= read -r -d '' file; do
    relative="${file#"$PARTITION_DIR"/}"
    destination="$MINIO_TARGET/$relative"

    # Skip if object already exists on MinIO
    if mc stat "$destination" >/dev/null 2>&1; then
      echo "[SKIP] Already exists: $relative"
      continue
    fi

    echo "[UPLOAD] $relative"
    if mc cp "$file" "$destination"; then
      if mc stat "$destination" >/dev/null 2>&1; then
        echo "[UPLOAD OK] $relative"
      else
        echo "[ERROR] Upload verify failed: $relative"
        UPLOAD_ERRORS=$((UPLOAD_ERRORS + 1))
      fi
    else
      echo "[ERROR] Upload failed: $relative"
      UPLOAD_ERRORS=$((UPLOAD_ERRORS + 1))
    fi
  done < <(find "$PARTITION_DIR" -type f -name '*.parquet' -print0)
fi

if [[ $UPLOAD_ERRORS -gt 0 ]]; then
  echo "[ERROR] $UPLOAD_ERRORS upload(s) failed. Raw snapshots NOT archived. Fix and re-run."
  exit 1
fi
echo "[OK] All trending partitions uploaded"

echo "[STEP 3B] Uploading non-trending..."
bash "$PROJECT_DIR/src/ingestion/upload_non_trending_once.sh"
echo "[OK] Non-Trending Upload done"

# -- 4. Archive raw snapshots (only after ALL uploads succeed) --
echo "[STEP 4] Archiving raw snapshots..."
ARCHIVED=0
shopt -s nullglob
for file in "$INPUT_DIR"/trending_*.parquet; do
  base="$(basename "$file")"
  snapshot="${base#trending_}"
  snapshot="${snapshot%.parquet}"

  SNAPSHOT_PARTITIONS=$(find "$PARTITION_DIR" -type f -name "trending_${snapshot}.parquet" 2>/dev/null | wc -l)
  if [[ $SNAPSHOT_PARTITIONS -gt 0 ]]; then
    mv "$file" "$ARCHIVE_DIR/$base"
    echo "[ARCHIVE OK] $base (partitions: $SNAPSHOT_PARTITIONS)"
    ARCHIVED=$((ARCHIVED + 1))
  else
    echo "[WARNING] No partitions for $base - keeping raw"
  fi
done

  echo "[OK] Archived $ARCHIVED raw snapshot(s)"
else
  echo "[INFO] Current time is before 5:00 AM. Skipping partitioning and MinIO upload for now."
fi

echo "END pipeline: $(date -Iseconds)"
echo "=================================================="
