# Phân công dự kiến (bản nháp để nhóm thảo luận)

> Bản preview, chưa phải phân công chính thức. Nhóm thống nhất rồi mới điền vào bảng "Phân công nhiệm vụ" của báo cáo.
> Nguyên tắc: phần lõi trong module `bigdata` (Hadoop MapReduce và Spark) chia đều cho 4 người; phần mở rộng
> (học máy, web, Docker, biên tập báo cáo) chia để tổng khối lượng mỗi người tương đương. Mỗi người tự chạy lại và giải thích
> được phần của mình khi phản biện.

## 1. Phần lõi: 4 phần ngang nhau

| | Phần lõi phụ trách | Mã nguồn chính | Kết quả phải có |
|---|---|---|---|
| **TV1** | Dữ liệu vào và HDFS: nạp dữ liệu, preflight, lấy mẫu, đọc CSV, lọc purchase; MapReduce V1 cơ bản | `scripts/hdfs-ingest.sh`, `scripts/hdfs-sample.sh`, `DatasetPreflight`, `InputManifest`, `SamplingHash`, `CsvEventParser`, `PurchasePreparation`, `DirectPurchaseMapper`, `FinalRevenueReducer` | Báo cáo 3.2 bước 2, 4, 5, 7, 8; giải thích Map/Reduce trên dữ liệu 7 dòng tính tay |
| **TV2** | Tối ưu MapReduce mức 1: V2 Combiner, V3 gộp trong mapper; chạy job, kiểm tra kết quả, counters | `SumCountCombiner`, `InMapperPurchaseMapper`, `BoundedAccumulator`, `RevenueTool`, `JobExecutor`, `ResultValidator`, `scripts/mr_counters.py` | Bước 6, 9; bảng counters; so V1–V3 về số bản ghi và dữ liệu shuffle |
| **TV3** | Tối ưu MapReduce mức 2: V4 dùng từ điển, V5 gói batch; benchmark MapReduce; đối chiếu baseline | `DensePurchaseMapper`, `GroupDictionary`, `BatchPurchaseMapper`, `BatchPartitioner`, `BatchRevenueReducer`, `scripts/benchmark.py`, `scripts/baseline_revenue.py` | Bước 10, 13 (phần MR, synthetic); thí nghiệm E2, E3 |
| **TV4** | Spark: A1 bằng RDD/DataFrame, ETL ra Parquet, A2–A5; benchmark Spark | `RevenueJob`, `EventEtlJob`, `MetricsJob`, `SparkSupport`, `scripts/spark-pipeline.sh`, `scripts/spark_bench.py` | Bước 11, 13 (phần Spark), Spark UI; thí nghiệm E4–E6; đối chiếu Spark với MR |

## 2. Phần mở rộng và hạ tầng: 4 gói cân bằng

Mỗi gói có cùng cấu trúc: **một phần xử lý** (job/script), **một phần hiển thị hoặc tài liệu**, **một phần kiểm thử/bằng chứng**.
Học máy được tách làm hai gói (K-Means và KNN) để không dồn vào một người.

| | Gói mở rộng | Phần xử lý | Phần hiển thị / tài liệu | Kiểm thử / bằng chứng |
|---|---|---|---|---|
| **TV1** | Hạ tầng Docker và xuất kết quả | `compose.yaml`, `config/hadoop.env`, `Dockerfile` (bigdata), `docker/spark/Dockerfile`; job publish (`ServingPublishJob`), `scripts/serving-sync.ps1` | Hướng dẫn cài `docs/docker.md`, `README.md` | Cài lại từ đầu theo tài liệu trên máy khác; manifest SHA-256 của serving run khớp 100% |
| **TV2** | Học máy KNN | Job nhãn A8 (`ProductLabelJob`), notebook `knn_classifier.ipynb` | Trang web KNN (`KnnPage.tsx`) và API dự đoán KNN | Kiểm tra không rò rỉ dữ liệu theo t0; khoảng tin cậy bootstrap; test đối chiếu suy luận Java với notebook |
| **TV3** | Ứng dụng web và tổng hợp thực nghiệm | Backend chung: đọc serving run, kiểm tra SHA-256, API phân tích; `scripts/bench_summary.py` | Trang Tổng quan, Group By, Benchmark | Test backend (`InferenceTest`), test frontend (Vitest); `docs/evidence/docker/bench/SUMMARY.md` |
| **TV4** | Học máy K-Means | Job đặc trưng A7 (`ProductFeaturesJob`), notebook `kmeans_product.ipynb`, thí nghiệm E7 (cache) | Trang web K-Means (`KMeansPage.tsx`) và API phân cụm | Chọn K bằng silhouette, so hai baseline; test đối chiếu suy luận Java với Spark (`ServingParityTest`) |

Cách cân bằng: phần lõi mỗi người một phần ngang nhau (mục 1); ở phần mở rộng, hai gói học máy (TV2, TV4) tương đương nhau,
hai gói hạ tầng/web (TV1, TV3) mỗi gói đều có một phần xử lý, một phần giao diện hoặc tài liệu và một phần kiểm thử.
Nếu trong quá trình làm một gói phát sinh nhiều việc hơn, nhóm chuyển bớt phần tài liệu hoặc kiểm thử sang người có gói nhẹ hơn.

## 3. Báo cáo: mỗi người viết phần mình làm

| | Phần báo cáo |
|---|---|
| **TV1** | 1.1–1.2 (Big Data, Hadoop/HDFS); 2.1–2.3 (bài toán, hợp đồng dữ liệu); 3.1; 3.2 bước 1–8 và bước 14 |
| **TV2** | 1.5 và 2.4 (mô hình MapReduce, Map/Shuffle/Reduce); 2.5 phần V1–V3; 3.2 bước 9; 3.3 kiểm thử; 3.5 phần KNN |
| **TV3** | 2.5 phần V4–V5 và bảng tổng hợp độ phức tạp; 3.2 bước 10, 13, 15; 3.4 kết quả benchmark; 3.6 ứng dụng web |
| **TV4** | 1.3–1.4 (Spark, so sánh với Hadoop); 2.6 (cùng bài toán trên Spark); 3.2 bước 11–12; 3.5 phần đặc trưng và K-Means |
| **Chung** | Trưởng nhóm ghép bản cuối, viết Mở đầu, Kết luận, mục 3.7; kiểm tra số liệu khớp `reports/raw/`. Bảng ca demo làm chung |

## 4. Bảo đảm ai cũng hiểu toàn bộ luồng

- **Review chéo theo cặp:** TV1 ↔ TV4 (luồng dữ liệu vào → Spark), TV2 ↔ TV3 (các phương án MapReduce). Người review phải tự chạy lại được phần mình review.
- **Demo:** mỗi người trình bày 2 trong 8 ca demo ở mục 3.6, và trả lời được câu hỏi về Map, Shuffle, Reduce trên ví dụ 7 dòng.
- **Bằng chứng đóng góp:** commit Git theo tên từng người, log và ảnh trong `reports/raw/` tương ứng phần mình chạy, phần báo cáo mình viết.

## 5. Gán tên (nhóm điền)

| | Thành viên |
|---|---|
| TV1 | |
| TV2 | |
| TV3 | |
| TV4 | |
