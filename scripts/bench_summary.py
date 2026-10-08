#!/usr/bin/env python3
"""Tổng hợp thực nghiệm giai đoạn 3 (E2-E6) từ các lần đo thật thành bảng nhỏ cho báo cáo và web.

  py -3 scripts/bench_summary.py            # đọc results/benchmark-reports + docs/evidence/bench/spark
Đầu vào:
  - MR: results/benchmark-reports/<id>/runs.json (benchmark.py); bản sao runs.json/summary.csv/matrix.json được
    chép sang docs/evidence/bench/mr/<label>/ để commit cùng evidence.
  - Spark: docs/evidence/bench/spark/<label>/runs.json (scripts/spark-bench.ps1).
Đầu ra: docs/evidence/bench/serving/<experiment>.json (định dạng trang Benchmark của web, publish vào serving)
và docs/evidence/bench/SUMMARY.md (bảng Markdown). Ô nào chưa đo thì bỏ qua và ghi vào danh sách "missing";
không có số nào được điền tay.
"""

import json
import shutil
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "results" / "benchmark-reports"
EVIDENCE = ROOT / "docs" / "evidence" / "bench"
SPARK = EVIDENCE / "spark"
OUT = EVIDENCE / "serving"

# Báo cáo MR (benchmark.py) theo nhãn; cấu hình ở config/bench/<label>.json.
MR_REPORTS = {
    "mr-e2-d1": "4a650d42c2ab",
    "mr-e2-d2": "d700ad14bf4a",
    "mr-e2-d3": "e9b35a9b6869",
    "mr-e3-d2-maps2": "ba2683f05ff6",
    "mr-e3-d2-maps4": "572e8edf4c16",
}
# Kích thước dữ liệu: preflight của MR (bytes, số dòng gồm header).
DATASETS = {
    "D1": ("mẫu 1% tháng 10/2019 (seed 21)", ROOT / "results/mr/20261006-032500-88fd989-d1/meta/preflight.json"),
    "D2": ("mẫu 10% tháng 10/2019 (seed 21)", ROOT / "results/mr/20261006-034832-88fd989-d2/meta/preflight.json"),
    "D3": ("cả tháng 10/2019", ROOT / "results/mr/d3-meta/preflight.json"),
}
SCOPE_ONE_HOST = ("một máy Windows 11 (i5-9300HF 4 nhân/8 luồng, RAM 7,9 GB); HDFS 1 NameNode + 1 DataNode trong "
                  "Docker; MR = LocalJobRunner (không YARN), Spark = local[n] trên host")
missing = []


def stats(values):
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "runs": len(values)}


def mr_runs(label):
    report = REPORTS / MR_REPORTS[label]
    if not (report / "runs.json").exists():
        missing.append(f"MR {label}")
        return None
    dest = EVIDENCE / "mr" / label
    dest.mkdir(parents=True, exist_ok=True)
    for name in ("runs.json", "summary.csv", "matrix.json"):
        shutil.copyfile(report / name, dest / name)
    runs = json.loads((report / "runs.json").read_text(encoding="utf-8"))
    bad = [r for r in runs if not r.get("success")]
    if bad:
        raise SystemExit(f"{label}: có {len(bad)} lần chạy MR lỗi, không tổng hợp")
    return [r for r in runs if not r["warmup"]]


def mr_row(runs, variant, label, field="jobMillis"):
    values = [r[field] for r in runs if r["variant"] == variant]
    if not values:
        return None
    sample = next(r for r in runs if r["variant"] == variant)
    return {"label": label, **stats(values), "details": {
        "source": "mapreduce", "variant": sample["effectiveVariant"], "shuffleBytes": sample["shuffleBytes"],
        "mapOutputRecords": sample["mapOutputRecords"], "reduceInputRecords": sample["reduceInputRecords"]}}


def spark_runs(label):
    path = SPARK / label / "runs.json"
    if not path.exists():
        missing.append(f"Spark {label}")
        return None
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    runs = [r for r in data["runs"] if not r["warmup"]]
    if not all(r["success"] for r in data["runs"]):
        raise SystemExit(f"{label}: có lần chạy Spark lỗi hoặc lệch kết quả tham chiếu")
    return data["meta"], runs


def spark_row(label, row_label, field="computeMillis"):
    got = spark_runs(label)
    if got is None:
        return None
    meta, runs = got
    tm = runs[0]["taskMetrics"]
    return {"label": row_label, **stats([r[field] for r in runs]), "details": {
        "source": "spark", "mode": meta["mode"], "master": meta["master"], "shufflePartitions": meta["shufflePartitions"],
        "inputBytesRead": tm["inputBytesRead"], "inputRecordsRead": tm["inputRecordsRead"],
        "shuffleReadBytes": tm["shuffleReadBytes"], "tasks": tm["tasks"],
        "matchesReference": all(r.get("matchesReference") is not False for r in runs)}}


def write(name, experiment, title, dataset, metric, rows, notes, source, scope=SCOPE_ONE_HOST, unit="ms"):
    rows = [r for r in rows if r]
    if not rows:
        missing.append(f"{name}: không có ô nào")
        return None
    doc = {"experiment": experiment, "title": title, "dataset": dataset, "scope": scope, "metric": metric,
           "unit": unit, "rows": rows, "notes": notes, "source": source}
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{name}.json").write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return doc


def main():
    docs = []
    sizes = {}
    for code, (desc, path) in DATASETS.items():
        p = json.loads(path.read_text(encoding="utf-8"))
        sizes[code] = {"description": desc, "bytes": p["manifest"]["files"][0]["bytes"], "lines": p["lines"],
                       "validPurchases": p["validPurchaseCount"]}

    # E2: MR V1..V5 theo kích thước dữ liệu (job MR; end-to-end gồm preflight + validate ghi ở details).
    for code in ("D1", "D2", "D3"):
        label = f"mr-e2-{code.lower()}"
        runs = mr_runs(label)
        if runs is None:
            continue
        rows = []
        for v in ("v1", "v2", "v3", "v4", "v5"):
            r = mr_row(runs, v, f"MR {v.upper()}")
            if r:
                r["details"]["endToEnd"] = stats([x["endToEndMillis"] for x in runs if x["variant"] == v])
                rows.append(r)
        docs.append(write(f"e2-mr-variants-{code.lower()}", "E2", f"MapReduce V1–V5 trên {code}",
                          f"{code}: {sizes[code]['description']}", "Thời gian job MR", rows,
                          ["Thời gian job = submit tới hoàn tất job MR, không gồm preflight/validate của RevenueTool "
                           "(xem details.endToEnd).", "1 map slot (mặc định LocalJobRunner), 2 reducer, poll 100 ms."],
                          f"docs/evidence/bench/mr/{label}/runs.json"))

    # E3: số map chạy song song (mapreduce.local.map.tasks.maximum) trên D2.
    rows = []
    for label, maps in (("mr-e2-d2", 1), ("mr-e3-d2-maps2", 2), ("mr-e3-d2-maps4", 4)):
        runs = mr_runs(label)
        if runs:
            for v in ("v1", "v2"):
                rows.append(mr_row(runs, v, f"MR {v.upper()}, {maps} map song song"))
    docs.append(write("e3-mr-maps-d2", "E3", "MapReduce: số map task chạy song song (D2)",
                      f"D2: {sizes['D2']['description']}", "Thời gian job MR", rows,
                      ["LocalJobRunner chạy map trong một JVM; mapreduce.local.map.tasks.maximum = số luồng map."],
                      "docs/evidence/bench/mr/mr-e3-d2-maps*/runs.json"))

    # E4: Spark A1 theo API/định dạng, số core, shuffle partitions.
    rows = []
    for code in ("d1", "d2", "d3"):
        for mode, text in (("rdd-raw", "RDD trên CSV"), ("df-raw", "DataFrame trên CSV"),
                           ("df-curated", "DataFrame trên Parquet curated")):
            rows.append(spark_row(f"e4-{code}-{mode}", f"{code.upper()} · {text}"))
    docs.append(write("e4-spark-modes", "E4", "Spark A1: API và định dạng đầu vào (local[2], 8 partitions)",
                      "D1, D2, D3", "Thời gian tính (không gồm ghi Parquet, khởi động)", rows,
                      ["df-curated đọc Parquet đã qua ETL (chỉ 3 cột), nên không tính chi phí ETL một lần.",
                       "Mọi lần chạy được so khớp chính xác với kết quả tham chiếu (MR V1 hoặc baseline)."],
                      "docs/evidence/bench/spark/e4-*/runs.json"))
    rows = [spark_row("e4-d2-rdd-raw-c1", "local[1]"), spark_row("e4-d2-rdd-raw", "local[2]"),
            spark_row("e4-d2-rdd-raw-c4", "local[4]")]
    docs.append(write("e4-spark-cores-d2", "E4", "Spark A1 (RDD trên CSV): số core (D2)", f"D2: {sizes['D2']['description']}",
                      "Thời gian tính", rows, ["Máy có 4 nhân vật lý/8 luồng; HDFS (Docker) chạy cùng máy và chia CPU/RAM với Spark.",
                       "Thêm core không giảm thời gian: nhiều khả năng bị giới hạn bởi đọc HDFS qua cổng chuyển tiếp WSL2 (chưa đo riêng)."],
                      "docs/evidence/bench/spark/e4-d2-rdd-raw*/runs.json"))
    rows = [spark_row("e4-d2-df-raw", "8 partitions"), spark_row("e4-d2-df-raw-p64", "64 partitions"),
            spark_row("e4-d2-df-raw-p200", "200 partitions")]
    docs.append(write("e4-spark-partitions-d2", "E4", "Spark A1 (DataFrame trên CSV): spark.sql.shuffle.partitions (D2)",
                      f"D2: {sizes['D2']['description']}", "Thời gian tính", rows,
                      ["Số nhóm sau aggregate chỉ vài trăm, nhiều partition chủ yếu thêm task rỗng."],
                      "docs/evidence/bench/spark/e4-d2-df-raw*/runs.json"))

    # E5: cùng A1, cùng input HDFS. Hai phạm vi đo riêng.
    proc, e2e = [], []
    for code in ("D2", "D3"):
        runs = mr_runs(f"mr-e2-{code.lower()}")
        if runs:
            proc.append(mr_row(runs, "v1", f"{code} · MR V1 (1 map slot)"))
            proc.append(mr_row(runs, "v5", f"{code} · MR V5 (1 map slot)"))
            e2e.append(mr_row(runs, "v1", f"{code} · MR V1", "endToEndMillis"))
        c1 = "e4-d2-rdd-raw-c1" if code == "D2" else "e5-d3-rdd-raw-c1"
        c2 = f"e4-{code.lower()}-rdd-raw"
        proc.append(spark_row(c1, f"{code} · Spark RDD local[1]"))
        proc.append(spark_row(c2, f"{code} · Spark RDD local[2]"))
        e2e.append(spark_row(c2, f"{code} · Spark RDD local[2]", "wallMillis"))
    docs.append(write("e5-mr-vs-spark-processing", "E5", "MR và Spark cùng A1: chỉ thời gian xử lý", "D2, D3",
                      "Job MR / action Spark", proc,
                      ["MR: thời gian job (map + shuffle + reduce). Spark: các stage tính, không gồm ghi Parquet.",
                       "So sánh local[1] với MR 1 map slot là cặp cùng mức song song.",
                       "Môi trường I/O khác nhau: MR chạy trong container Linux cùng mạng Docker với DataNode; Spark chạy trên host Windows, đọc HDFS qua cổng 127.0.0.1 do Docker/WSL2 chuyển tiếp. Chênh lệch thời gian không chỉ do engine."],
                      "docs/evidence/bench/{mr,spark}"))
    docs.append(write("e5-mr-vs-spark-end-to-end", "E5", "MR và Spark cùng A1: end-to-end", "D2, D3",
                      "Thời gian end-to-end", e2e,
                      ["MR end-to-end gồm quét preflight lại + SHA-256 trước/sau job (RevenueTool, F4) trong container.",
                       "Spark end-to-end = thời gian đồng hồ của spark-submit (khởi động JVM + SparkSession + ghi)."],
                      "docs/evidence/bench/{mr,spark}"))

    # E6: theo kích thước dữ liệu; throughput = số dòng / thời gian.
    rows = []
    for code in ("D1", "D2", "D3"):
        runs = mr_runs(f"mr-e2-{code.lower()}")
        for r in ([mr_row(runs, "v1", f"{code} · MR V1"), mr_row(runs, "v5", f"{code} · MR V5")] if runs else []) + \
                 [spark_row(f"e4-{code.lower()}-rdd-raw", f"{code} · Spark RDD local[2]"),
                  spark_row(f"e4-{code.lower()}-df-curated", f"{code} · Spark DF Parquet local[2]")]:
            if r:
                r["details"]["linesPerSecond"] = round(sizes[code]["lines"] / (r["median"] / 1000))
                r["details"]["megabytesPerSecond"] = round(sizes[code]["bytes"] / 1e6 / (r["median"] / 1000), 1)
                rows.append(r)
    docs.append(write("e6-scaling", "E6", "Mở rộng theo kích thước dữ liệu (D1 → D2 → D3)", "D1, D2, D3",
                      "Thời gian xử lý", rows,
                      ["Đây là mở rộng theo kích thước dữ liệu trên một máy, không phải mở rộng cụm.",
                       "Throughput tính theo số dòng/byte CSV gốc, kể cả dòng không phải purchase.",
                       "Môi trường I/O khác nhau: MR chạy trong container Linux cùng mạng Docker với DataNode; Spark chạy trên host Windows, đọc HDFS qua cổng 127.0.0.1 do Docker/WSL2 chuyển tiếp. Chênh lệch thời gian không chỉ do engine."],
                      "docs/evidence/bench/{mr,spark}"))

    # E7: K-Means lặp có/không cache (notebook K-Means trên D3, pyspark.mllib RDD, 20 vòng lặp cố định).
    e7 = ROOT / "docs/evidence/ml/20261006-235739-b0376ef-d3/e7.csv"
    if e7.exists():
        import csv
        measured = [r for r in csv.DictReader(e7.open(encoding="utf-8")) if r["warmup"] == "False"]
        rows = []
        for mode, text in (("no_cache", "Không cache"), ("cache", "cache() + count() trước khi fit")):
            fit = [int(r["fit_ms"]) for r in measured if r["mode"] == mode]
            row = {"label": text, **stats(fit), "details": {"source": "spark-mllib"}}
            if mode == "cache":
                row["details"]["cacheCountMillis"] = stats([float(r["cache_count_ms"]) for r in measured if r["mode"] == mode])
            rows.append(row)
        docs.append(write("e7-kmeans-cache", "E7", "K-Means lặp: có và không cache (D3, 92 592 sản phẩm)",
                          "A7 của D3 (Parquet trên HDFS)", "Thời gian fit (20 vòng lặp, K = 2)", rows,
                          ["Chi phí nạp cache (count) báo riêng trong details.cacheCountMillis; tính cả bước này thì cache "
                           "không có lợi ở quy mô và số vòng lặp này."],
                          "docs/evidence/ml/20261006-235739-b0376ef-d3/e7.csv"))
    else:
        missing.append("E7 (e7.csv)")

    # Không đặt trong OUT: mọi file trong OUT được publish thành bảng benchmark của web.
    (EVIDENCE / "datasets.json").write_text(json.dumps(sizes, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Tổng hợp thực nghiệm E2–E6 (sinh bởi scripts/bench_summary.py, không sửa tay)", "",
             f"Phạm vi: {SCOPE_ONE_HOST}.", "", "Mỗi ô: 1 warmup + 3 lần đo; median (min–max), đơn vị ms.", ""]
    for d in filter(None, docs):
        lines += [f"## {d['experiment']} — {d['title']}", "", f"Chỉ số: {d['metric']}. Nguồn: `{d['source']}`", "",
                  "| Cấu hình | Median | Min | Max | Lần đo |", "|---|---:|---:|---:|---:|"]
        lines += [f"| {r['label']} | {r['median']:,.0f} | {r['min']:,.0f} | {r['max']:,.0f} | {r['runs']} |" for r in d["rows"]]
        lines += [""] + [f"- {n}" for n in d["notes"]] + [""]
    if missing:
        lines += ["## Chưa đo", ""] + [f"- {m}" for m in sorted(set(missing))]
    (EVIDENCE / "SUMMARY.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"{len([d for d in docs if d])} bảng -> {OUT}; chưa đo: {sorted(set(missing)) or 'không'}")


if __name__ == "__main__":
    sys.exit(main())
