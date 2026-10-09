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
| **TV4** | **Cài đặt Apache Spark** (image và container spark) và Spark: A1 bằng RDD/DataFrame, ETL ra Parquet, A2–A5; benchmark Spark | `docker/spark/Dockerfile`, service `spark` trong `compose.yaml`, `scripts/spark-submit.sh`, `RevenueJob`, `EventEtlJob`, `MetricsJob`, `SparkSupport`, `scripts/spark-pipeline.sh`, `scripts/spark_bench.py` | Bước 3 (phần image spark), 11, 13 (phần Spark), Spark UI; thí nghiệm E4–E6; đối chiếu Spark với MR |

## 2. Phần mở rộng và hạ tầng

Mỗi gói có cùng cấu trúc: **một phần xử lý** (job/script), **một phần hiển thị hoặc tài liệu**, **một phần kiểm thử/bằng chứng**.
Huấn luyện và đánh giá học máy giao cho TV3 (K-Means, đánh giá cả hai mô hình) và TV4 (KNN); TV2 nhận toàn bộ web;
TV1 nhận hạ tầng Hadoop và xuất kết quả.

| | Gói mở rộng | Phần xử lý | Phần hiển thị / tài liệu | Kiểm thử / bằng chứng |
|---|---|---|---|---|
| **TV1** | Hạ tầng Hadoop và xuất kết quả | `compose.yaml` (phần HDFS, bigdata, web), `config/hadoop.env`, `Dockerfile` (image bigdata); job publish (`ServingPublishJob`), `scripts/serving-sync.ps1` | Hướng dẫn cài `docs/docker.md` | Cài lại từ đầu theo tài liệu trên máy khác; manifest SHA-256 của serving run khớp 100% |
| **TV2** | Ứng dụng web (backend và frontend) và tổng hợp thực nghiệm | Backend chung: đọc serving run, kiểm tra SHA-256, API phân tích; `scripts/bench_summary.py` | **Toàn bộ frontend**: trang Tổng quan, Group By, Benchmark, K-Means (`KMeansPage.tsx`) và KNN (`KnnPage.tsx`) | Test backend `InferenceTest`, test frontend (Vitest); `docs/evidence/docker/bench/SUMMARY.md` |
| **TV3** | Học máy K-Means và đánh giá mô hình | Job đặc trưng A7 (`ProductFeaturesJob`), notebook `kmeans_product.ipynb` (chuẩn hóa, quét K, chọn K bằng silhouette, diễn giải cụm), thí nghiệm E7 (cache); **đánh giá cả hai mô hình**: so KNN với baseline lớp đa số, luật lịch sử và Logistic Regression, khoảng tin cậy bootstrap; so silhouette K-Means với hai baseline | API phân cụm (cung cấp dữ liệu cho trang K-Means do TV2 làm) | Suy luận numpy trùng Spark; test `ServingParityTest` (phần K-Means); bảng đánh giá khớp output notebook |
| **TV4** | Học máy KNN: nhãn và huấn luyện | Job nhãn A8 (`ProductLabelJob`), notebook `knn_classifier.ipynb` (mốc t0, chọn K và ngưỡng trên tập validation) | API dự đoán KNN (cung cấp dữ liệu cho trang KNN do TV2 làm) | Kiểm tra không rò rỉ dữ liệu theo t0; test `ServingParityTest` (phần KNN) |

Cách cân bằng:

- Phần lõi: mỗi người một phần ngang nhau (mục 1).
- Học máy (mảng đề xếp ở mức khó/nâng cao): TV3 huấn luyện K-Means và đánh giá cả hai mô hình; TV4 huấn luyện KNN. Người đánh
  giá KNN (TV3) không phải người huấn luyện KNN (TV4), nên phần đánh giá đóng vai trò kiểm tra chéo.
- TV2 nhận trọn web (backend và toàn bộ frontend, kể cả trang K-Means và KNN) để có một đầu mối; TV3, TV4 chỉ cung cấp API
  suy luận cho mô hình mình huấn luyện. TV1 nhận hạ tầng Hadoop và xuất kết quả.
- **Lưu ý:** với cách chia này, TV1 và TV2 không có phần học máy, còn TV3 khá nặng (V4–V5, benchmark MapReduce, K-Means và đánh
  giá hai mô hình). Nếu thấy lệch, có thể chuyển phần đánh giá KNN sang TV1 để TV1 có một mảnh học máy và TV3 bớt tải.
- Nếu trong quá trình làm một gói phát sinh nhiều việc hơn, nhóm chuyển bớt phần tài liệu hoặc kiểm thử sang người có gói nhẹ hơn.

## 3. Báo cáo: mỗi người viết phần mình làm

| | Phần báo cáo |
|---|---|
| **TV1** | 1.1–1.2 (Big Data, Hadoop/HDFS); 2.1–2.3 (bài toán, hợp đồng dữ liệu); 3.1; 3.2 bước 1–8 (bước 3 phần image bigdata), 14 |
| **TV2** | 1.5 và 2.4 (mô hình MapReduce, Map/Shuffle/Reduce); 2.5 phần V1–V3; 3.2 bước 9, 15; 3.3 kiểm thử; 3.6 ứng dụng web |
| **TV3** | 2.5 phần V4–V5 và bảng tổng hợp độ phức tạp; 3.2 bước 10, 13; 3.4 kết quả benchmark; 3.5 phần đặc trưng, K-Means (kèm E7) và đánh giá mô hình |
| **TV4** | 1.3–1.4 (Spark, so sánh với Hadoop); 2.6 (cùng bài toán trên Spark); 3.2 bước 3 (phần image spark), 11–12; 3.5 phần nhãn và KNN |
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
