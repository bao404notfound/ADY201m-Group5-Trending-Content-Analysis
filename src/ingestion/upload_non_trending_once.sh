#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$HOME/ADY201m-Group5-Trending-Content-Analysis"
PARTITION_DIR="$PROJECT_DIR/data/processed/member_linh/non_trending"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/non_trending_upload.log"
MINIO_TARGET="myminio/youtube-raw/member_linh/non-trending"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=================================================="
echo "START non-trending upload: $(date -Is)"
mc ready myminio

if [[ ! -d "$PARTITION_DIR" ]]; then
  echo "[ERROR] Partition directory not found: $PARTITION_DIR"
  exit 1
fi

echo "[INFO] Using mc mirror to upload files efficiently..."
mc mirror --overwrite "$PARTITION_DIR/" "$MINIO_TARGET/"

echo "END non-trending upload: $(date -Is)"