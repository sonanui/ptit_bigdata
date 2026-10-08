"""In bảng counters của các biến thể MapReduce từ run-manifest.json (do RevenueTool ghi cạnh output).

Dùng: py -3 scripts/mr_counters.py results/native/<RUN_ID>/meta
"""
import json
import sys
from pathlib import Path

TASK = [
    ("MAP_INPUT_RECORDS", "Map input records"),
    ("MAP_OUTPUT_RECORDS", "Map output records"),
    ("COMBINE_INPUT_RECORDS", "Combine input records"),
    ("COMBINE_OUTPUT_RECORDS", "Combine output records"),
    ("REDUCE_SHUFFLE_BYTES", "Reduce shuffle bytes"),
    ("REDUCE_INPUT_GROUPS", "Reduce input groups"),
    ("REDUCE_INPUT_RECORDS", "Reduce input records"),
    ("REDUCE_OUTPUT_RECORDS", "Reduce output records"),
]
OWN = ["HEADERS", "NON_PURCHASE", "VALID_PURCHASE", "MALFORMED_CSV", "INVALID_PRICE", "CACHE_FLUSHES"]


def counters(manifest):
    merged = {}
    for stage in manifest["stages"]:
        merged.update(stage["counters"])
    return {key.rsplit(".", 1)[-1]: value for key, value in merged.items()}


meta = Path(sys.argv[1])
runs = {p.name.split("-")[0]: json.loads(p.read_text(encoding="utf-8")) for p in sorted(meta.glob("v*-run-manifest.json"))}
names = list(runs)
table = {v: counters(m) for v, m in runs.items()}
print(f"{'Counter':30}" + "".join(f"{v.upper():>14}" for v in names))
for key, label in TASK:
    print(f"{label:30}" + "".join(f"{table[v].get(key, 0):>14,}" for v in names))
print("-- counter do chương trình đếm (RevenueCounters) --")
for key in OWN:
    if any(table[v].get(key) for v in names):
        print(f"{key:30}" + "".join(f"{table[v].get(key, 0):>14,}" for v in names))
print(f"{'Thời gian job (ms)':30}" + "".join(f"{sum(s['elapsedMillis'] for s in runs[v]['stages']):>14,}" for v in names))
print(f"{'validationStatus':30}" + "".join(f"{runs[v]['validationStatus']:>14}" for v in names))
