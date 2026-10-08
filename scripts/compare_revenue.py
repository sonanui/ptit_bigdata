#!/usr/bin/env python3
"""So khớp chính xác hai file revenue CSV (group_key,total_revenue,purchase_count,average_revenue).

  py -3 scripts/compare_revenue.py results/mr/<RUN>/revenue.csv results/spark/<RUN>/revenue.csv
Mã thoát 0 khi mọi nhóm khớp cả 4 cột dưới dạng chuỗi; 1 khi có khác biệt (in tối đa 20 dòng).
"""

import csv
import sys

COLUMNS = ["group_key", "total_revenue", "purchase_count", "average_revenue"]


def load(path):
    with open(path, encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != COLUMNS:
            raise SystemExit(f"{path}: header {reader.fieldnames} != {COLUMNS}")
        rows = {}
        for row in reader:
            if row["group_key"] in rows:
                raise SystemExit(f"{path}: trùng group_key {row['group_key']}")
            rows[row["group_key"]] = tuple(row[c] for c in COLUMNS[1:])
        return rows


def main(argv):
    sys.stdout.reconfigure(encoding="utf-8")
    if len(argv) != 3:
        raise SystemExit("Usage: compare_revenue.py LEFT.csv RIGHT.csv")
    left, right = load(argv[1]), load(argv[2])
    diffs = [f"only left: {k}" for k in sorted(left.keys() - right.keys())]
    diffs += [f"only right: {k}" for k in sorted(right.keys() - left.keys())]
    diffs += [f"{k}: {left[k]} != {right[k]}" for k in sorted(left.keys() & right.keys()) if left[k] != right[k]]
    total = sum(int(v[1]) for v in left.values())
    if diffs:
        print(f"KHÁC: {len(diffs)} khác biệt", *diffs[:20], sep="\n")
        return 1
    print(f"KHỚP: {len(left)} nhóm, tổng purchase_count={total}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
