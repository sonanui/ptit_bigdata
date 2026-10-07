#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$PROJECT_ROOT/scripts/java-env.sh"
cd "$PROJECT_ROOT"
# Jar MapReduce của module bigdata và classpath Hadoop (scope compile) dùng chung cho các script MR.
jar="bigdata/target/revenue-aggregation.jar"
classpath=".build-cache/classpath.txt"
tool=RevenueTool
if [[ "${1:-}" == "--tool" ]]; then tool="${2:?Missing tool}"; shift 2; fi
case "$tool" in RevenueTool|DatasetTool) ;; *) echo "Unknown tool: $tool" >&2; exit 2;; esac
if [[ ! -f "$jar" ]]; then ./mvnw -pl bigdata package -DskipTests; fi
if [[ ! -f "$classpath" || bigdata/pom.xml -nt "$classpath" ]]; then
  ./mvnw -pl bigdata dependency:build-classpath -DincludeScope=compile -Dmdep.outputFile="$PROJECT_ROOT/$classpath"
  touch "$classpath"
fi
# config/local.properties -> -Dkey=value (bỏ dòng trống/chú thích); đứng trước nên -D của người gọi được ưu tiên.
conf_file="${REVENUE_CONF:-config/local.properties}"
conf_opts=()
if [[ -f "$conf_file" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line// }" || "$line" == \#* ]] && continue
    conf_opts+=("-D$line")
  done < "$conf_file"
fi
exec "$JAVA_BIN" -cp "$jar:$(cat "$classpath")" "vn.edu.bigdata.revenue.cli.$tool" "${conf_opts[@]}" -Dmapreduce.framework.name=local -Dfs.defaultFS=file:/// "$@"
