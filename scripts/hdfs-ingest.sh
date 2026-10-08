#!/usr/bin/env bash
# Nạp CSV gốc vào vùng raw trên HDFS. Chạy trong container namenode (có /staging/raw chỉ đọc):
#   docker compose exec namenode bash /opt/bigdata/scripts/hdfs-ingest.sh 2019-Oct.csv [...]
# Không bao giờ ghi đè: file đã có cùng kích thước thì bỏ qua, khác kích thước thì dừng với lỗi.
set -euo pipefail
staging="${STAGING_DIR:-/staging/raw}"
target="${HDFS_RAW_DIR:-/data/ecommerce/raw}"
if [[ $# -lt 1 ]]; then echo "Usage: hdfs-ingest.sh FILE_NAME... (file nằm trong $staging)" >&2; exit 2; fi

hdfs dfs -mkdir -p "$target"
for name in "$@"; do
  if [[ "$name" == */* || "$name" != *.csv ]]; then echo "Chỉ nhận tên file .csv trong $staging: $name" >&2; exit 2; fi
  src="$staging/$name"; dst="$target/$name"
  if [[ ! -f "$src" ]]; then echo "Không thấy $src" >&2; exit 2; fi
  size=$(stat -c %s "$src")
  if hdfs dfs -test -e "$dst"; then
    existing=$(hdfs dfs -stat %b "$dst")
    if [[ "$existing" == "$size" ]]; then echo "SKIP $dst đã tồn tại ($size bytes)"; continue; fi
    echo "$dst đã tồn tại nhưng $existing != $size bytes; không ghi đè" >&2; exit 1
  fi
  start=$(date +%s)
  # -put ghi vào $dst._COPYING_ rồi mới đổi tên, nên lần chạy bị ngắt không để lại file trông như hoàn chỉnh.
  hdfs dfs -put "$src" "$dst"
  elapsed=$(( $(date +%s) - start ))
  written=$(hdfs dfs -stat %b "$dst")
  if [[ "$written" != "$size" ]]; then echo "Sai kích thước sau khi nạp: $written != $size" >&2; exit 1; fi
  echo "PUT $src -> $dst: $size bytes, ${elapsed}s"
done

hdfs dfs -ls -h "$target"
hdfs fsck "$target" -files -blocks
