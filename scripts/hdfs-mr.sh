#!/usr/bin/env bash
# Group By doanh thu theo category_id bằng Hadoop MapReduce (LocalJobRunner), input và output trên HDFS.
# Chạy trong container bigdata khi namenode/datanode đang chạy:
#   docker compose run --rm bigdata scripts/hdfs-mr.sh /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv RUN_ID [v1 v3 ...]
# Output MR: /data/ecommerce/mr/<RUN_ID>/<variant>/ trên HDFS. Meta, run-manifest và revenue.csv: results/mr/<RUN_ID>/.
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$PROJECT_ROOT/scripts/java-env.sh"
cd "$PROJECT_ROOT"
if [[ $# -lt 2 || "$1" != /* ]]; then echo 'Usage: hdfs-mr.sh /HDFS/INPUT.csv RUN_ID [VARIANT...]' >&2; exit 2; fi
hdfs="${HDFS_URI:-hdfs://namenode:8020}"
# Client không đọc hdfs-site.xml của cluster, nên mặc định replication=3 trên cụm chỉ có 1 DataNode.
client_opts=(-Ddfs.replication=1)
input="$hdfs$1"; run_id="$2"; shift 2
variants=("$@")
if [[ ${#variants[@]} -eq 0 ]]; then variants=(v1 v2 v3 v4 v5); fi
local_root="results/mr/$run_id"; meta="$local_root/meta"; out="$hdfs/data/ecommerce/mr/$run_id"
if [[ -e "$local_root" ]]; then echo "Choose a new RUN_ID: $local_root already exists" >&2; exit 2; fi
mkdir -p "$meta"

# FsShell của hadoop-common thay cho lệnh hdfs (image bigdata không cài Hadoop CLI).
# FsShell.main bật strict mode nên cần một core-site.xml trên classpath; cấu hình thật truyền bằng -D.
shell_conf="$(mktemp -d)"; trap 'rm -rf "$shell_conf"' EXIT
echo '<configuration/>' > "$shell_conf/core-site.xml"
fs_shell() { "$JAVA_BIN" -cp "$shell_conf:bigdata/target/revenue-aggregation.jar:$(cat .build-cache/classpath.txt)" org.apache.hadoop.fs.FsShell -Dfs.defaultFS="$hdfs" "${client_opts[@]}" "$@"; }
millis() { date +%s%3N; }
step() { local label="$1"; shift; local t0; t0=$(millis); "$@"; echo -e "$label\t$(( $(millis) - t0 ))" >> "$meta/timings.tsv"; }

if fs_shell -test -e "$out"; then echo "HDFS output $out already exists" >&2; exit 2; fi
step preflight scripts/run-local.sh --tool DatasetTool "${client_opts[@]}" preflight --input "$input" --manifest "$meta/input.json" --report "$meta/preflight.json"
dictionary=()
if [[ " ${variants[*]} " == *" v4 "* || " ${variants[*]} " == *" v5 "* ]]; then
  step profile scripts/run-local.sh --tool DatasetTool "${client_opts[@]}" profile --manifest "$meta/input.json" --output "$meta/profile.json" --dictionary "$meta/groups.json"
  dictionary=(--dictionary "$meta/groups.json")
fi
for variant in "${variants[@]}"; do
  step "$variant" scripts/run-local.sh "${client_opts[@]}" --variant "$variant" --manifest "$meta/input.json" --preflight "$meta/preflight.json" --output "$out/$variant" --reducers 2 "${dictionary[@]}"
  fs_shell -get "$out/$variant/run-manifest.json" "$meta/$variant-run-manifest.json"
done
for variant in "${variants[@]:1}"; do
  scripts/run-local.sh --tool DatasetTool "${client_opts[@]}" compare --left "$out/${variants[0]}" --right "$out/$variant"
done
scripts/run-local.sh --tool DatasetTool "${client_opts[@]}" export --input "$out/${variants[0]}" --output "$out/revenue.csv"
fs_shell -get "$out/revenue.csv" "$local_root/revenue.csv"
fs_shell -ls -R "$out"
cat "$meta/timings.tsv"
