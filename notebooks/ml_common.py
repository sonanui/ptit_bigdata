"""Tiện ích dùng chung cho notebook K-Means/KNN: SparkSession local đọc HDFS trong Docker, lưu vết lần chạy.

Phần lõi (ETL, Group By A1-A5, đặc trưng A7) là Java: package vn.edu.bigdata.revenue.spark.
Notebook chỉ đọc đặc trưng A7 đã được Java ghi lên HDFS (/data/ecommerce/features/product/run_id=...).
"""

import json
import os
import platform
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_ENV = PROJECT_ROOT / "config" / "spark-local.env"
EVIDENCE_JAVA = PROJECT_ROOT / "docs" / "evidence" / "spark-java"
RESULTS_ML = PROJECT_ROOT / "results" / "ml"
FEATURES_ROOT = "/data/ecommerce/features/product"
# Phải trùng ProductFeaturesJob.FEATURES (Java); notebook kiểm tra cột tồn tại khi đọc.
FEATURES = ["log_views", "log_carts", "log_purchases", "cart_rate", "purchase_rate", "log_median_price", "log_distinct_users"]
# A8 (ProductLabelJob, Java): đặc trưng trước mốc t0 + nhãn "có purchase trong [t0, t0 + label_days)".
LABELS_ROOT = "/data/ecommerce/features/product_label"
LABEL_FEATURES = FEATURES + ["recent_view_share"]


def param(name: str, default=None):
    """Tham số notebook qua biến môi trường (chạy bằng nbconvert); rỗng = dùng mặc định."""
    value = os.environ.get(name, "")
    return value if value.strip() else default


def load_local_env() -> None:
    """config/spark-local.env ghi đè biến môi trường (JAVA_HOME toàn máy có thể là JDK 23+, Spark 4.0 không hỗ trợ)."""
    for line in LOCAL_ENV.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ[key.strip()] = value.strip()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    if platform.system() == "Windows" and os.environ.get("HADOOP_HOME"):
        os.environ["PATH"] = str(Path(os.environ["HADOOP_HOME"]) / "bin") + os.pathsep + os.environ["PATH"]


def hdfs(path: str) -> str:
    return os.environ.get("HDFS_URI", "hdfs://localhost:8020") + path


def session(app: str):
    load_local_env()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.hadoop.fs.defaultFS", hdfs(""))
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=PROJECT_ROOT, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return "nogit"


def new_run_id(tag: str) -> str:
    return f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{git_sha()}-{tag}"


def pipeline_run_id(tag: str, job: str) -> str:
    """run_id do scripts/spark-pipeline.ps1 ghi lại (docs/evidence/spark-java/<tag>-run-ids.tsv)."""
    for line in (EVIDENCE_JAVA / f"{tag}-run-ids.tsv").read_text(encoding="utf-8-sig").splitlines():
        name, _, value = line.partition("\t")
        if name == job:
            return value.strip()
    raise KeyError(f"Không có run_id '{job}' cho {tag}")


def environment(spark) -> dict:
    jvm = spark.sparkContext._jvm
    return {
        "sparkVersion": spark.version,
        "javaVersion": jvm.System.getProperty("java.version"),
        "hadoopClientVersion": jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
        "pythonVersion": platform.python_version(),
        "master": spark.sparkContext.master,
        "driverMemory": spark.conf.get("spark.driver.memory"),
        "gitSha": git_sha(),
    }


def write_hdfs_text(spark, path: str, text: str) -> None:
    """Ghi file văn bản nhỏ (JSON/CSV) lên HDFS, create-only (overwrite=false)."""
    jvm = spark.sparkContext._jvm
    target = jvm.org.apache.hadoop.fs.Path(hdfs(path))
    fs = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(target, False)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


def read_hdfs_text(spark, path: str) -> str:
    """Đọc file văn bản nhỏ trên HDFS qua FileSystem API (spark.read bỏ qua file tên bắt đầu bằng '_')."""
    jvm = spark.sparkContext._jvm
    target = jvm.org.apache.hadoop.fs.Path(hdfs(path))
    fs = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.open(target)
    try:
        return jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


def write_hdfs_json(spark, path: str, value) -> None:
    write_hdfs_text(spark, path, json.dumps(value, indent=2, default=str))


class Trace:
    """Lưu vết thời gian từng bước; in ra ngay để output notebook giữ lại tiến trình."""

    def __init__(self, job: str, run_id: str, params: dict):
        self.started = time.perf_counter()
        self.record = {"job": job, "runId": run_id, "params": params, "startedAt": datetime.now(timezone.utc).isoformat(), "stages": [], "metrics": {}}

    @contextmanager
    def stage(self, name: str):
        t0 = time.perf_counter()
        print(f"[{datetime.now():%H:%M:%S}] bắt đầu: {name}")
        yield
        ms = round((time.perf_counter() - t0) * 1000)
        self.record["stages"].append({"name": name, "elapsedMillis": ms})
        print(f"[{datetime.now():%H:%M:%S}] xong: {name} ({ms} ms)")

    def finish(self, local_dir: Path) -> dict:
        self.record["endToEndMillis"] = round((time.perf_counter() - self.started) * 1000)
        local_dir.mkdir(parents=True, exist_ok=True)
        (local_dir / f"{self.record['job']}-run.json").write_text(json.dumps(self.record, indent=2, default=str), encoding="utf-8")
        return self.record


# --- KNN phân loại và metric nhị phân (numpy thuần, không cần scikit-learn) ---------------------
# Cùng thuật toán sẽ được viết lại bằng Java ở backend; giữ phép tính đơn giản, tất định để đối chiếu.


def knn_vote_share(train_x, train_y, query_x, k: int, chunk: int = 256):
    """Tỷ lệ láng giềng nhãn 1 trong k láng giềng gần nhất (Euclidean bình phương, cộng theo thứ tự cột).

    Hòa khoảng cách ở ranh giới thứ k: chọn điểm train có chỉ số nhỏ hơn trước (tất định).
    vote_share là tỷ lệ phiếu, KHÔNG phải xác suất đã hiệu chỉnh.
    """
    import numpy as np

    train_x = np.asarray(train_x, dtype=np.float64)
    train_y = np.asarray(train_y, dtype=np.float64)
    query_x = np.asarray(query_x, dtype=np.float64)
    if not 1 <= k <= len(train_x):
        raise ValueError(f"k={k} ngoài [1, {len(train_x)}]")
    out = np.empty(len(query_x))
    for start in range(0, len(query_x), chunk):
        q = query_x[start:start + chunk]
        d = np.zeros((len(q), len(train_x)))
        for j in range(train_x.shape[1]):
            diff = q[:, j, None] - train_x[None, :, j]
            d += diff * diff
        kth = np.partition(d, k - 1, axis=1)[:, k - 1]
        below = d < kth[:, None]
        tied = d == kth[:, None]
        need = k - below.sum(axis=1)
        chosen = below | (tied & (np.cumsum(tied, axis=1) <= need[:, None]))
        out[start:start + len(q)] = (chosen * train_y[None, :]).sum(axis=1) / k
    return out


def binary_metrics(y_true, y_pred) -> dict:
    import numpy as np

    y_true = np.asarray(y_true).astype(int)
    y_pred = np.asarray(y_pred).astype(int)
    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())
    tn = int(((y_true == 0) & (y_pred == 0)).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    specificity = tn / (tn + fp) if tn + fp else 0.0
    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "accuracy": (tp + tn) / len(y_true),
        "precision": precision, "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
        "balanced_accuracy": (recall + specificity) / 2,
    }


def average_precision(y_true, scores) -> float:
    """PR-AUC theo định nghĩa average precision (bậc thang), điểm bằng nhau được xét cùng lúc."""
    import numpy as np

    y_true = np.asarray(y_true).astype(int)
    scores = np.asarray(scores, dtype=np.float64)
    positives = y_true.sum()
    if positives == 0:
        return float("nan")
    order = np.argsort(-scores, kind="stable")
    s, y = scores[order], y_true[order]
    last = np.r_[np.nonzero(np.diff(s))[0], len(s) - 1]   # chỉ số cuối mỗi nhóm điểm bằng nhau
    tp = np.cumsum(y)[last]
    fp = (last + 1) - tp
    precision = tp / (tp + fp)
    recall = tp / positives
    return float(np.sum(np.diff(np.r_[0.0, recall]) * precision))


def best_threshold(y_true, scores, candidates) -> float:
    """Ngưỡng (dự đoán 1 khi score >= ngưỡng) cho F1 cao nhất; hòa thì chọn ngưỡng lớn hơn."""
    import numpy as np

    y = np.asarray(y_true).astype(int) == 1
    s = np.asarray(scores, dtype=np.float64)
    best = None
    for t in sorted(set(float(c) for c in candidates), reverse=True):
        pred = s >= t
        tp = int((pred & y).sum())
        precision = tp / int(pred.sum()) if pred.any() else 0.0
        recall = tp / int(y.sum()) if y.any() else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        if best is None or f1 > best[1]:
            best = (t, f1)
    return best[0]
