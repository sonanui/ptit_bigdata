#!/usr/bin/env bash
# Chạy một job Spark Java trong container spark (compose.yaml), HDFS qua mạng Compose:
#   docker compose run --rm --service-ports spark bash scripts/spark-submit.sh JOB [--name value ...]
# JOB: revenue | etl | metrics | features | labels | publish (xem SparkTool). Tương đương scripts/spark-local.ps1 trên host.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${SPARK_APP_JAR:?Chạy trong container spark (biến SPARK_APP_JAR do image đặt)}"
: "${HDFS_URI:?Thiếu HDFS_URI}"
# run_id = thời gian + git sha + tag (SparkSupport.runId).
PTIT_GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
export PTIT_GIT_SHA
exec spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-8}" \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_URI" \
  --conf "spark.hadoop.dfs.client.use.datanode.hostname=${HDFS_USE_DATANODE_HOSTNAME:-false}" \
  --class vn.edu.bigdata.revenue.spark.SparkTool "$SPARK_APP_JAR" "$@"
