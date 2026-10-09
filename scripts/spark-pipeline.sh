#!/usr/bin/env bash
# Chuỗi Spark Java trên một file raw trong HDFS, chạy trong container spark:
#   A1 (RDD) -> ETL curated -> A2..A5 -> A7 features -> A8 labels
#   docker compose run --rm --service-ports spark bash scripts/spark-pipeline.sh /data/ecommerce/raw/2019-Oct.csv dk3
# Log từng job: docs/evidence/spark-java/<tag>-<job>.log; run_id: docs/evidence/spark-java/<tag>-run-ids.tsv
# (notebook ML đọc file run-ids này). Tương đương scripts/spark-pipeline.ps1 trên host.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -ne 2 || "$1" != /* ]]; then echo 'Usage: spark-pipeline.sh /HDFS/INPUT.csv TAG' >&2; exit 2; fi
input="$1"; tag="$2"
min_views="${MIN_VIEWS:-20}"; train_t0="${TRAIN_T0:-2019-10-15}"; test_t0="${TEST_T0:-2019-10-25}"
log_dir="docs/evidence/spark-java"
mkdir -p "$log_dir"
if [[ -e "$log_dir/$tag-run-ids.tsv" ]]; then echo "Đã có $log_dir/$tag-run-ids.tsv: chọn TAG mới" >&2; exit 2; fi

run_job() {
  local job="$1"; shift
  local log="$log_dir/$tag-$job.log"
  local t0; t0=$(date +%s)
  if ! bash scripts/spark-submit.sh "$job" "$@" > "$log" 2>&1; then
    tail -30 "$log" >&2; echo "Job $job lỗi, xem $log" >&2; exit 1
  fi
  # Dòng tổng kết của từng job: "<Job> xong: <run_id> ..."
  local line; line=$(grep -E " xong: " "$log" | tail -1)
  echo "$line ($(( $(date +%s) - t0 )) s)" >&2
  awk '{for (i = 1; i <= NF; i++) if ($i == "xong:") {print $(i + 1); exit}}' <<< "$line"
}

a1=$(run_job revenue --input "$input" --tag "$tag")
etl=$(run_job etl --input "$input" --tag "$tag" --duplicates false)
metrics=$(run_job metrics --curated-run-id "$etl" --revenue-run-id "$a1")
features=$(run_job features --curated-run-id "$etl" --min-views "$min_views")
labels=$(run_job labels --curated-run-id "$etl" --train-t0 "$train_t0" --test-t0 "$test_t0" --min-views "$min_views")
printf 'revenue\t%s\netl\t%s\nmetrics\t%s\nfeatures\t%s\nlabels\t%s\n' "$a1" "$etl" "$metrics" "$features" "$labels" \
  | tee "$log_dir/$tag-run-ids.tsv"
