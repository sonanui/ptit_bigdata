# PTIT Big Data 2026 — Bài 21: Group By Aggregation

Phân tích dữ liệu hành vi thương mại điện tử (Kaggle, tháng 10/2019: 42,4 triệu sự kiện, 5,7 GB) theo luồng
**HDFS → Hadoop MapReduce và Apache Spark → Group By → đặc trưng → K-Means/KNN → web**.
Bài toán lõi: tổng và trung bình giá trị mua hàng theo danh mục (Group By), cài đặt bằng 5 biến thể MapReduce và đối chiếu
chính xác với Spark. Phần mở rộng: phân cụm sản phẩm (K-Means) và dự đoán sản phẩm có lượt mua trong 7 ngày tới (KNN).

**Thành viên mới:** đọc [`docs/HUONG_DAN_CHAY.md`](docs/HUONG_DAN_CHAY.md). Chỉ cần Docker là xem được web với kết quả thật:

```powershell
docker compose --profile web up -d --build      # http://localhost:8080
```

## Kết quả chính (dữ liệu cả tháng 10/2019, có bằng chứng trong `docs/evidence/`)

| Hạng mục | Kết quả |
|---|---|
| Đối chiếu Group By | MapReduce V1 = Spark A1 = baseline Python độc lập: 567/567 nhóm, 742 849 lượt mua hợp lệ, khớp từng đơn vị tiền |
| Tối ưu MapReduce | Map output 742 849 bản ghi (V1) → 15 511 (combiner V2) → 86 (V5); thời gian job V5 61 s so với V1 190 s (median, 1 máy) |
| K-Means sản phẩm | 92 592 sản phẩm, K = 2, silhouette 0,649 (baseline 0,316 và 0,183) |
| KNN phân loại | F1 0,621, PR-AUC 0,671 trên ảnh chụp test theo thời gian; vượt baseline, **thua** Logistic Regression (0,637/0,718) |
| Web | Spring Boot suy luận bằng Java khớp Spark/numpy 100%; React; API p95 < 40 ms |

Mọi số đo hiệu năng là trên **một máy** (Windows 11, 4 nhân, RAM 7,9 GB), không phải hiệu năng cụm. Chi tiết:
[`docs/evidence/bench/SUMMARY.md`](docs/evidence/bench/SUMMARY.md), [`docs/ML.md`](docs/ML.md).

## Cấu trúc repository

```text
ptit_bigdata/
├─ pom.xml                Maven aggregator: module bigdata + webapp/backend (khi JDK >= 17)
├─ bigdata/               Module Big Data (Java, package vn.edu.bigdata.revenue)
│  ├─ pom.xml             Hadoop 3.4.2 (Java 11); profile spark (Spark 4.0.4, Java 17) tự bật trên JDK >= 17
│  └─ src/main/java/vn/edu/bigdata/revenue/
│     ├─ hadoop/v1..v5    Mapper/Combiner/Partitioner/Reducer của 5 biến thể MapReduce
│     ├─ cli/             RevenueTool (chạy job MR), DatasetTool (preflight, sample, compare, export)
│     ├─ spark/           SparkTool: A1 (RDD), ETL Parquet, A2–A5, đặc trưng A7, nhãn A8, publish serving
│     └─ domain/ input/ output/ job/ metrics/ profile/
├─ notebooks/             K-Means và KNN (PySpark MLlib + numpy), thực thi bằng nbconvert, lưu output
├─ webapp/
│  ├─ backend/            Spring Boot 3.5 (Java 21): /api, đọc serving, suy luận K-Means/KNN
│  └─ frontend/           React 18 + TypeScript + Vite + ECharts, Nginx khi chạy Docker
├─ serving/               Serving run đã chốt (kết quả pipeline cho web), có manifest sha256
├─ scripts/               HDFS, MapReduce, Spark, benchmark, publish/sync, kiểm tra
├─ config/                Hadoop env, cấu hình MR (local.properties), benchmark, Spark local (mẫu)
├─ docs/                  Tài liệu và bằng chứng (evidence)
├─ compose.yaml           Profile bigdata (HDFS + MapReduce) và web (backend + frontend)
└─ Dockerfile             Image MapReduce (JDK 11)
```

## Tài liệu

| Tài liệu | Nội dung |
|---|---|
| [`docs/HUONG_DAN_CHAY.md`](docs/HUONG_DAN_CHAY.md) | Hướng dẫn chạy cho thành viên: xem web, sửa web, chạy lại pipeline |
| [`docs/END_TO_END.md`](docs/END_TO_END.md) | Kiến trúc, thứ tự chạy lại toàn bộ từ dataset, kịch bản demo |
| [`docs/PROJECT_AUDIT_AND_IMPLEMENTATION_PLAN.md`](docs/PROJECT_AUDIT_AND_IMPLEMENTATION_PLAN.md) | Kế hoạch duy nhất của dự án, trạng thái, việc còn lại (§19) |
| [`docs/algorithms.md`](docs/algorithms.md) | MapReduce V1–V5 và Spark: Map, Shuffle, Reduce, độ phức tạp |
| [`docs/design.md`](docs/design.md) | Thiết kế bài toán Group By và 5 biến thể MapReduce |
| [`docs/ML.md`](docs/ML.md) | Đặc trưng, nhãn, K-Means, KNN, đánh giá, lưu mô hình |
| [`docs/DATASET.md`](docs/DATASET.md) | Nguồn, giấy phép, checksum, schema dataset |
| [`docs/docker.md`](docs/docker.md), [`docs/spark-local.md`](docs/spark-local.md), [`docs/runbook.md`](docs/runbook.md) | Chạy HDFS/MapReduce trong Docker, Spark local trên Windows, chạy từng biến thể MR |
| [`docs/benchmark-report.md`](docs/benchmark-report.md) | Báo cáo đo MapReduce (synthetic ban đầu và trỏ tới số liệu thật) |
| [`docs/project-structure.md`](docs/project-structure.md) | Trách nhiệm từng thư mục và lớp |
| [`webapp/README.md`](webapp/README.md) | Kiến trúc web, API, hợp đồng serving artifact |
| [`REQUIRMENT.md`](REQUIRMENT.md) | Yêu cầu bổ sung phần ML/web và các quyết định |

## Build nhanh

```powershell
# Cả dự án trên máy (JDK 21; Git Bash, Linux hoặc macOS): module bigdata (25 test, gồm Spark) + backend (7 test)
./mvnw -B package
# MapReduce trong Docker (JDK 11): 18 unit + 6 integration test và demo fixture
docker compose build bigdata
docker compose run --rm bigdata ./mvnw -B -pl bigdata -Pintegration verify
docker compose run --rm bigdata
# Frontend
cd webapp/frontend; npm install; npm test; npm run build
```

## Quyết định chính

- **Hadoop trước, Spark sau:** HDFS (1 NameNode + 1 DataNode, Docker) là nơi lưu chung. MapReduce chạy LocalJobRunner (không YARN)
  đọc/ghi HDFS, chứng minh Map/Combine/Shuffle/Reduce và làm đối chứng. Spark làm ETL, Group By mở rộng, đặc trưng cho ML.
- Tiền tính bằng số nguyên đơn vị nhỏ nhất, trạng thái trung gian `(sum, count)`; AVG chỉ chia ở đầu ra, nên combiner đúng và
  MapReduce khớp Spark tuyệt đối.
- ML huấn luyện offline (Spark MLlib trong notebook); web chỉ đọc kết quả đã publish và suy luận, không huấn luyện, không đọc dữ liệu thô.
- Không bịa số: thiếu dữ liệu thì API báo lỗi; kết quả âm (KNN thua Logistic Regression) được báo cáo như đo được.

## Nguồn

[Dataset Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store),
[REES46](https://rees46.com), [Apache MapReduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html),
[Spark RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html).
