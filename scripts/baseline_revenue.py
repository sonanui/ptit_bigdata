#!/usr/bin/env python3
"""Baseline độc lập cho A1 (E1): đọc CSV cục bộ bằng thư viện chuẩn Python, không qua Hadoop/Spark.

  py -3 scripts/baseline_revenue.py data/raw/2019-Oct.csv results/baseline/2019-Oct-revenue.csv
Chính sách giống PurchasePreparation: event_type.strip() == 'purchase'; giá Decimal(strip) có tối đa 2 chữ số
thập phân khác 0 và không âm; category_id gồm toàn chữ số. Ghi CSV cùng định dạng với DatasetTool export.
"""

import csv
import re
import sys
import time
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

HEADER = ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session"]
CATEGORY_ID = re.compile(r"[0-9]+")
CENT = Decimal("0.01")


def price_minor(text):
    try:
        value = Decimal(text.strip(" \t\r\n\x0b\x0c"))
    except InvalidOperation:
        return None
    if not value.is_finite() or value < 0 or value.quantize(CENT) != value:
        return None
    return int(value * 100)


def main(argv):
    sys.stdout.reconfigure(encoding="utf-8")
    if len(argv) != 3:
        raise SystemExit("Usage: baseline_revenue.py INPUT.csv OUTPUT.csv")
    start = time.perf_counter()
    sums, counts, reasons = {}, {}, {}
    with open(argv[1], encoding="utf-8", newline="") as handle:
        for row in csv.reader(handle):
            if row == HEADER:
                reason = "HEADER"
            elif len(row) != 9:
                reason = "MALFORMED_CSV"
            else:
                event = row[1].strip()
                if event in ("view", "cart", "remove_from_cart"):
                    reason = "NON_PURCHASE"
                elif event != "purchase":
                    reason = "UNKNOWN_EVENT"
                elif (minor := price_minor(row[6])) is None:
                    reason = "INVALID_PRICE"
                elif not CATEGORY_ID.fullmatch(row[3]) or len(row[3].encode()) > 256:
                    reason = "INVALID_GROUP"
                else:
                    reason = "VALID_PURCHASE"
                    sums[row[3]] = sums.get(row[3], 0) + minor
                    counts[row[3]] = counts.get(row[3], 0) + 1
            reasons[reason] = reasons.get(reason, 0) + 1
    with open(argv[2], "x", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["group_key", "total_revenue", "purchase_count", "average_revenue"])
        for key in sorted(sums):
            total = Decimal(sums[key]) / 100
            average = (total / counts[key]).quantize(CENT, rounding=ROUND_HALF_UP)
            writer.writerow([key, f"{total:.2f}", counts[key], f"{average:.2f}"])
    print(f"Baseline: {len(sums)} nhóm, reasons={reasons}, {time.perf_counter() - start:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
