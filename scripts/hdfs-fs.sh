#!/usr/bin/env bash
# Hadoop FsShell trên HDFS từ container bigdata (image không cài Hadoop CLI), ví dụ:
#   scripts/hdfs-fs.sh -cat hdfs://namenode:8020/data/ecommerce/bench/.../run-manifest.json
# benchmark.py dùng làm "manifestReader" để đọc run-manifest.json của output trên HDFS.
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$PROJECT_ROOT/scripts/java-env.sh"
cd "$PROJECT_ROOT"
# FsShell.main bật strict mode nên cần một core-site.xml trên classpath; cấu hình thật truyền bằng -D.
shell_conf="$(mktemp -d)"; trap 'rm -rf "$shell_conf"' EXIT
echo '<configuration/>' > "$shell_conf/core-site.xml"
"$JAVA_BIN" -cp "$shell_conf:bigdata/target/revenue-aggregation.jar:$(cat .build-cache/classpath.txt)" org.apache.hadoop.fs.FsShell \
  -Dfs.defaultFS="${HDFS_URI:-hdfs://namenode:8020}" -Ddfs.replication=1 "$@"
