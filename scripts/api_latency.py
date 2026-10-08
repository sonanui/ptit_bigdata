#!/usr/bin/env python3
"""Đo độ trễ API của web app (plan T6.8): p50/p95/max theo endpoint, request tuần tự từ một client.

  py -3 scripts/api_latency.py [--base http://127.0.0.1:8080] [--n 200] [--out docs/evidence/webapp/api-latency.json]
Lần gọi đầu mỗi endpoint được báo riêng ("cold": nạp + kiểm sha256 artifact, dựng mô hình); sau đó 1 warmup + n lần đo.
Mỗi response phải là HTTP 200, nếu không script dừng. Chỉ dùng thư viện chuẩn.
"""

import argparse
import json
import statistics
import time
import urllib.request
from pathlib import Path


def call(base, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=120) as resp:
        payload = resp.read()
        status = resp.status
    ms = (time.perf_counter() - t0) * 1000
    if status != 200:
        raise SystemExit(f"{method} {path}: HTTP {status}")
    return ms, json.loads(payload)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default="http://127.0.0.1:8080")
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--out", default="docs/evidence/webapp/api-latency.json")
    a = ap.parse_args()

    _, health = call(a.base, "GET", "/api/health")
    run = health["defaultRun"]
    _, km = call(a.base, "GET", "/api/ml/kmeans/models")
    _, kn = call(a.base, "GET", "/api/ml/knn/models")
    km_run, kn_run = km[0]["runId"], kn[0]["runId"]
    endpoints = [
        ("GET", "/api/runs", None),
        ("GET", f"/api/analytics/{run}/tables/revenue_by_category?sort=total_revenue&limit=20", None),
        ("GET", f"/api/analytics/{run}/tables/funnel_by_brand?q=sam&sort=purchases&limit=20", None),
        ("GET", f"/api/analytics/{run}/benchmarks/e4-spark-modes", None),
        ("GET", f"/api/ml/kmeans/{km_run}/clusters", None),
        ("GET", f"/api/ml/kmeans/{km_run}/products?q=samsung&limit=10", None),
        ("POST", "/api/ml/kmeans/predict", {"runId": km_run, "productId": "1002099"}),
        ("POST", "/api/ml/kmeans/predict", {"runId": km_run, "raw": {"views": 3153, "carts": 0, "purchases": 12, "medianPrice": 333.49, "distinctUsers": 2107}}),
        ("POST", "/api/ml/knn/predict", {"runId": kn_run, "productId": "1002099"}),
        ("POST", "/api/ml/knn/predict", {"runId": kn_run, "raw": {"views": 300, "carts": 3, "purchases": 4, "medianPrice": 120, "distinctUsers": 250, "recentViews": 140}}),
    ]
    rows = []
    for method, path, body in endpoints:
        cold, _ = call(a.base, method, path, body)
        call(a.base, method, path, body)
        samples = [call(a.base, method, path, body)[0] for _ in range(a.n)]
        q = statistics.quantiles(samples, n=100, method="inclusive")
        label = f"{method} {path.split('?')[0]}" + (" (productId)" if body and "productId" in body else " (raw)" if body else "")
        rows.append({"endpoint": label, "query": path.split("?")[1] if "?" in path else None, "coldMillis": round(cold, 1),
                     "n": a.n, "p50Millis": round(q[49], 2), "p95Millis": round(q[94], 2), "maxMillis": round(max(samples), 2)})
        print(f"{label:60s} cold {cold:8.1f} ms | p50 {q[49]:7.2f} | p95 {q[94]:7.2f} | max {max(samples):7.2f}")
    result = {"base": a.base, "servingRun": run, "kmeansRun": km_run, "knnRun": kn_run,
              "method": "tuần tự, 1 client, cùng máy; cold = lần gọi đầu sau khi khởi động backend",
              "criterion": "p95 < 300 ms (plan T6.8)", "allP95Below300": all(r["p95Millis"] < 300 for r in rows), "rows": rows}
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print("p95 < 300 ms ở mọi endpoint:", result["allP95Below300"], "->", a.out)


if __name__ == "__main__":
    main()
