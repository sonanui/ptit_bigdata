#!/usr/bin/env python3
"""Ma trận thực nghiệm Spark A1 (E4, E5) trong container spark; tương đương spark-bench.ps1 + spark-bench-all.ps1.

  docker compose run --rm spark python scripts/spark_bench.py --campaign config/bench/docker/campaign.json [--only e4-d1]

Mỗi ô: 1 warmup + 3 lần đo, mỗi lần một run_id mới trên HDFS; mọi lần chạy được so khớp chính xác với kết quả
tham chiếu (scripts/compare_revenue.py). Kết quả: <evidenceDir>/spark/<label>/{runs.json, summary.csv};
log spark-submit: results/spark-bench/<label>/. Ô đã có summary.csv thì bỏ qua.
"""
import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def curated_run_id(tag):
    for line in (ROOT / f"docs/evidence/spark-java/{tag}-run-ids.tsv").read_text(encoding="utf-8-sig").splitlines():
        name, _, value = line.partition("\t")
        if name == "etl":
            return value.strip()
    raise KeyError(f"Không có run_id etl cho {tag}")


def cells(campaign):
    out = []
    for code in ("d1", "d2", "d3"):
        for mode in ("rdd-raw", "df-raw", "df-curated"):
            out.append({"label": f"e4-{code}-{mode}", "data": code, "mode": mode})
    out += [{"label": "e4-d2-rdd-raw-c1", "data": "d2", "mode": "rdd-raw", "master": "local[1]"},
            {"label": "e4-d2-rdd-raw-c4", "data": "d2", "mode": "rdd-raw", "master": "local[4]"},
            {"label": "e4-d2-df-raw-p64", "data": "d2", "mode": "df-raw", "partitions": 64},
            {"label": "e4-d2-df-raw-p200", "data": "d2", "mode": "df-raw", "partitions": 200},
            {"label": "e5-d3-rdd-raw-c1", "data": "d3", "mode": "rdd-raw", "master": "local[1]"}]
    return out


def median(values):
    return statistics.median(values)


def run_cell(campaign, cell, warmups=1, repeats=3):
    data = campaign["datasets"][cell["data"]]
    master = cell.get("master", "local[2]")
    partitions = cell.get("partitions", 8)
    evidence = ROOT / campaign["evidenceDir"] / "spark" / cell["label"]
    logs = ROOT / "results" / "spark-bench" / cell["label"]
    if evidence.exists():
        raise SystemExit(f"{evidence} dở dang: kiểm tra runs.json rồi xóa thủ công")
    evidence.mkdir(parents=True)
    logs.mkdir(parents=True, exist_ok=True)
    job_args = ["--mode", cell["mode"]]
    curated = None
    if cell["mode"] == "df-curated":
        curated = curated_run_id(data["tag"])
        job_args += ["--curated-run-id", curated]
    else:
        job_args += ["--input", data["raw"]]
    env = dict(os.environ, SPARK_MASTER=master, SPARK_SHUFFLE_PARTITIONS=str(partitions), SPARK_DRIVER_MEMORY="1g")
    runs = []
    for i in range(warmups + repeats):
        log = logs / f"{i}.log"
        start = time.perf_counter()
        with log.open("w") as out:
            exit_code = subprocess.run(["bash", "scripts/spark-submit.sh", "revenue", *job_args, "--tag", f"{cell['label']}-{i}"],
                                       cwd=ROOT, env=env, stdout=out, stderr=subprocess.STDOUT).returncode
        wall = round((time.perf_counter() - start) * 1000)
        run = {"round": i, "warmup": i < warmups, "exitCode": exit_code, "wallMillis": wall, "success": False}
        done = [line for line in log.read_text(errors="replace").splitlines() if " xong: " in line]
        if exit_code == 0 and done:
            run_id = done[-1].split(" xong: ")[1].split()[0]
            record = json.loads((ROOT / "results/spark" / run_id / "revenue-run.json").read_text(encoding="utf-8"))
            run.update(runId=run_id, endToEndMillis=record["endToEndMillis"],
                       computeMillis=sum(s["elapsedMillis"] for s in record["stages"] if s["name"] != "write_parquet"),
                       writeMillis=next((s["elapsedMillis"] for s in record["stages"] if s["name"] == "write_parquet"), None),
                       groups=record["metrics"]["groups"], validPurchases=record["metrics"]["validPurchases"],
                       taskMetrics=record["taskMetrics"])
            cmp = subprocess.run([sys.executable, "scripts/compare_revenue.py", data["reference"],
                                  str(ROOT / "results/spark" / run_id / "revenue.csv")], cwd=ROOT, capture_output=True, text=True)
            run["matchesReference"] = cmp.returncode == 0
            run["compare"] = (cmp.stdout or cmp.stderr).strip().splitlines()[0] if (cmp.stdout or cmp.stderr) else ""
            run["success"] = run["matchesReference"]
        runs.append(run)
        print(f"  round {i} exit={exit_code} wall={wall}ms success={run['success']} {run.get('runId', '')}", flush=True)
    git = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    meta = {"label": cell["label"], "mode": cell["mode"], "input": None if curated else data["raw"], "curatedRunId": curated,
            "master": master, "shufflePartitions": partitions, "driverMemory": "1g", "warmups": warmups, "repeats": repeats,
            "reference": data["reference"], "gitSha": git, "host": "docker:spark", "startedRuns": len(runs)}
    (evidence / "runs.json").write_text(json.dumps({"meta": meta, "runs": runs}, indent=2), encoding="utf-8")
    measured = [r for r in runs if not r["warmup"] and r["success"]]
    if len(measured) < repeats:
        raise SystemExit(f"{cell['label']}: chỉ có {len(measured)} lần đo hợp lệ, cần {repeats}; xem {evidence}/runs.json")
    summary = {"label": cell["label"], "mode": cell["mode"], "master": master, "shufflePartitions": partitions, "runs": len(measured)}
    for field in ("wallMillis", "endToEndMillis", "computeMillis"):
        values = [r[field] for r in measured]
        summary.update({f"{field}Median": median(values), f"{field}Min": min(values), f"{field}Max": max(values)})
    for metric in ("inputBytesRead", "inputRecordsRead", "shuffleReadBytes", "shuffleWriteBytes", "tasks"):
        summary[f"{metric}Median"] = median([r["taskMetrics"][metric] for r in measured])
    summary.update(groups=measured[0]["groups"], validPurchases=measured[0]["validPurchases"],
                   allMatchReference=all(r.get("matchesReference") is not False for r in runs))
    with (evidence / "summary.csv").open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=list(summary))
        writer.writeheader()
        writer.writerow(summary)
    print(f"  median compute {summary['computeMillisMedian']:.0f} ms, groups {summary['groups']}, khớp tham chiếu: {summary['allMatchReference']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--only", default="")
    args = parser.parse_args()
    campaign = json.loads((ROOT / args.campaign).read_text(encoding="utf-8"))
    for cell in cells(campaign):
        if args.only and not cell["label"].startswith(args.only):
            continue
        if (ROOT / campaign["evidenceDir"] / "spark" / cell["label"] / "summary.csv").exists():
            print(f"skip {cell['label']} (đã có)")
            continue
        print(f"=== {cell['label']} {time.strftime('%H:%M:%S')}", flush=True)
        run_cell(campaign, cell)
    print(f"=== xong {time.strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
