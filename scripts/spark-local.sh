#!/usr/bin/env bash
# Linux/macOS: build và chạy các job Spark Java (package vn.edu.bigdata.revenue.spark) trên host, HDFS trong Docker.
# Windows: dùng scripts/spark-local.ps1.
#   bash scripts/spark-local.sh build                 # ./mvnw -pl bigdata -Pspark package (gồm test Spark + MR) với JDK 17/21
#   bash scripts/spark-local.sh submit JOB [--name value ...]
#     JOB: revenue | etl | metrics | features | labels | publish   (xem SparkTool)
# Cấu hình máy: config/spark-local.env (JAVA_HOME, HDFS_URI, ...).
# Runtime Spark: spark-submit của PySpark trong .venv (cùng phiên bản với spark.version trong bigdata/pom.xml).
set -euo pipefail
if [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* ]]; then
  echo "Trên Windows dùng PowerShell: .\scripts\spark-local.ps1 (Git Bash hỏng khi gọi .cmd trong đường dẫn có dấu cách)" >&2; exit 2
fi
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"
env_file="config/spark-local.env"
if [[ ! -f "$env_file" ]]; then echo "Thiếu $env_file (sao chép từ config/spark-local.env.example)" >&2; exit 2; fi
while IFS='=' read -r key value; do
  [[ -z "$key" || "$key" == \#* ]] && continue
  export "$key=${value%$'\r'}"
done < "$env_file"
export PATH="$JAVA_HOME/bin:$PATH"
export PTIT_GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"

command="${1:-}"; shift || true
case "$command" in
  build)
    REVENUE_JAVA_HOME="$JAVA_HOME" ./mvnw -B -pl bigdata -Pspark package "$@"
    ;;
  submit)
    jar="bigdata/target/revenue-aggregation-spark.jar"
    if [[ ! -f "$jar" ]]; then echo "Chưa có $jar: chạy 'bash scripts/spark-local.sh build' trước" >&2; exit 2; fi
    submit=".venv/bin/spark-submit"
    exec "$submit" \
      --master "${SPARK_MASTER:-local[2]}" \
      --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
      --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-8}" \
      --class vn.edu.bigdata.revenue.spark.SparkTool "$jar" "$@"
    ;;
  *)
    echo "Usage: spark-local.sh {build [mvn args]|submit JOB [--name value ...]}" >&2; exit 2
    ;;
esac
