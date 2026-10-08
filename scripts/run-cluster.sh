#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
tool=RevenueTool
if [[ "${1:-}" == "--tool" ]]; then tool="${2:?Missing tool}"; shift 2; fi
case "$tool" in RevenueTool|DatasetTool) ;; *) echo "Unknown tool: $tool" >&2; exit 2;; esac
exec hadoop jar "$PROJECT_ROOT/bigdata/target/revenue-aggregation.jar" "vn.edu.bigdata.revenue.cli.$tool" "$@"
