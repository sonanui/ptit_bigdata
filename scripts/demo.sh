#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"
root="${1:-results/optimization-demo}"
if [[ -e "$root" ]]; then echo "Choose a new demo directory: $root already exists" >&2; exit 2; fi
mkdir -p "$root/meta"
scripts/run-local.sh --tool DatasetTool preflight --input bigdata/src/test/resources/fixtures/events.csv --manifest "$root/meta/input.json" --report "$root/meta/preflight.json"
scripts/run-local.sh --tool DatasetTool profile --manifest "$root/meta/input.json" --output "$root/meta/profile.json" --dictionary "$root/meta/groups.json"
for variant in v1 v2 v3 v4 v5; do
  scripts/run-local.sh --variant "$variant" --manifest "$root/meta/input.json" --preflight "$root/meta/preflight.json" --output "$root/$variant" --reducers 2 --max-keys 1 --dictionary "$root/meta/groups.json"
done
for variant in v2 v3 v4 v5; do
  scripts/run-local.sh --tool DatasetTool compare --left "$root/v1" --right "$root/$variant"
done
scripts/run-local.sh --tool DatasetTool export --input "$root/v1" --output "$root/revenue.csv"
cat "$root/revenue.csv"
