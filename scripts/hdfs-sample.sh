#!/usr/bin/env bash
# Tạo mẫu tất định từ một file raw trên HDFS bằng DatasetTool sample. Chạy trong container bigdata:
#   docker compose run --rm bigdata scripts/hdfs-sample.sh /data/ecommerce/raw/2019-Oct.csv 0.01 21
# Manifest của file nguồn được tạo một lần ở results/hdfs-meta/<tên file>/ và dùng lại;
# DatasetTool sample vẫn quét lại nguồn và từ chối nếu fingerprint đã đổi.
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"
if [[ $# -ne 3 || "$1" != /* ]]; then echo 'Usage: hdfs-sample.sh /HDFS/PATH.csv RATE SEED' >&2; exit 2; fi
hdfs="${HDFS_URI:-hdfs://namenode:8020}"
# Client không đọc hdfs-site.xml của cluster, nên mặc định replication=3 trên cụm chỉ có 1 DataNode.
client_opts=(-Ddfs.replication=1)
source_path="$1"; rate="$2"; seed="$3"
name="$(basename "$source_path" .csv)"
meta="results/hdfs-meta/$name"
sample="$hdfs/data/ecommerce/raw/sample/$name-r$rate-s$seed.csv"

millis() { date +%s%3N; }
if [[ ! -f "$meta/input.json" ]]; then
  mkdir -p "$meta"
  t0=$(millis)
  scripts/run-local.sh --tool DatasetTool "${client_opts[@]}" preflight --input "$hdfs$source_path" --manifest "$meta/input.json" --report "$meta/preflight.json"
  echo "preflight_ms=$(( $(millis) - t0 ))" | tee -a "$meta/timings.txt"
fi
t0=$(millis)
scripts/run-local.sh --tool DatasetTool "${client_opts[@]}" sample --manifest "$meta/input.json" --output "$sample" --rate "$rate" --seed "$seed"
echo "sample_r${rate}_s${seed}_ms=$(( $(millis) - t0 ))" | tee -a "$meta/timings.txt"
