# Báo cáo và ảnh chụp

- `Bao_cao_BTL_Group_By_Aggregation_Hadoop_MapReduce_cap_nhat_v7.docx`: bản mới nhất. Chương 3 viết lại theo đợt cài đặt và chạy
  **toàn bộ bằng Docker** ngày 08–09/10/2026 (HDFS, MapReduce, Spark, notebook ML, web), từ trạng thái sạch (volume HDFS cũ đã xoá).
  Mọi số liệu lấy từ lần chạy này; nguồn: `reports/raw/`, `docs/evidence/docker/`, `docs/evidence/spark-java/dk*`, `serving/20261009-024955-97bb8c2-dk3/`.
  Còn phải điền: mã sinh viên, tên nhóm, lớp, giảng viên, mã học phần, bảng phân công, điểm tự đánh giá.
  Khi mở bằng Word, chọn **Yes** để cập nhật trường cho số trang mục lục khớp với máy đang mở.
- `..._v6.docx`, `..._v4.docx`: bản cũ (v6 là luồng Spark chạy trên Windows), giữ để đối chiếu.
- Ảnh và output của đợt chạy trước (native Windows, Docker ngày 05–07/10) đã chuyển khỏi repo sang
  `E:\Library\ptit_bigdata-archive\20261008-before-docker\` trên máy của Nguyên; bản trong Git vẫn còn ở commit `97bb8c2`.

## Cách chụp

- Ảnh `dNN-*`: terminal tích hợp của IntelliJ IDEA (PowerShell), chụp cửa sổ IntelliJ rồi cắt vùng output.
  `t-dNN-*` là bản cắt sát chữ (bỏ dòng terminal lặp lại câu lệnh) dùng trong báo cáo; câu lệnh và output nguyên văn ở `raw/dNN-*.txt`.
- Ảnh `cNN-*` và `d03-compose-spark`, `d04-hadoop-env`, `d06-ingest-script`, `d07-variants-it`, `d08-fixture-csv`: editor của IntelliJ.
- Ảnh `w*`, `d05-namenode-*`, `d06-namenode-browse`, `d14/d15-sparkui-*`: trang web chụp bằng Edge headless
  (web ở http://localhost:8080, NameNode 9870, Spark UI 4040). `d14-sparkui-a1-*` chụp tự động trong lúc benchmark A1 cả tháng.
- `d18-nb-*`: hình do notebook K-Means vẽ (lấy từ output đã lưu trong `notebooks/kmeans_product.ipynb`).
- `d23-synthetic-chart`: vẽ từ `docs/evidence/docker/synthetic/summary.csv`.

## Các bước (báo cáo mục 3.2)

| Bước | Ảnh | Nội dung |
|---|---|---|
| 1 | `d01-tools` | Docker 29.8.1, Compose v5.5.1, WSL 3.0.1.0, chưa có volume HDFS |
| 2 | `d02-dataset` | `2019-Oct.csv` 5 668 612 855 byte, SHA-256 trùng `docs/DATASET.md` |
| 3 | `d03-build`, `d03-compose-spark`, `c06-spark-dockerfile` | Build image bigdata, spark: 25 test (7 Spark) đạt |
| 4 | `d04-*`, `d05-*` | Khởi động HDFS lần đầu, NameNode format, 1 DataNode live |
| 5 | `d06-*` | Nạp 5,67 GB trong 421 s, 43 block HEALTHY, chạy lại SKIP |
| 6 | `d07-*`, `d24-python-tests` | 18 + 6 test MapReduce (JDK 11), 5 test Python |
| 7 | `d08-*`, `c01`–`c03` | MapReduce V1–V5 trên 7 dòng, input/output trên HDFS |
| 8 | `d09-sample`, `c09` | Mẫu D1 1%, D2 10% (tất định, trùng byte với lần trước) |
| 9 | `d10-mr-runs`, `d11-mr-counters`, `c04`, `c10` | MapReduce dk1, dk2, dk3; counters trên D1 |
| 10 | `d12-mr-vs-baseline` | Baseline Python khớp 308/495/567 nhóm |
| 11 | `d13`, `d15-sparkui-etl-*`, `d16-parity`, `d17-hdfs-zones`, `c05`, `c07` | Chuỗi Spark dk1–dk3, Spark UI ETL (43 task), Spark = MR = baseline |
| 12 | `d18-*` | Notebook K-Means (K=2, silhouette 0,6494) và KNN (F1 0,6214) |
| 13 | `d19-*`, `d20-*`, `d14-sparkui-a1-*` | Benchmark MR, synthetic, Spark; Spark UI A1 (2 stage, shuffle 354 KiB) |
| 14 | `d21-serving` | Serving run `20261009-024955-97bb8c2-dk3`, 29/29 SHA-256 khớp |
| 15 | `d22-web-up`, `d25-api-kmeans` | Web trong Docker, 9 test frontend, 7 test backend, API K-Means |
| 3.6 | `w01`–`w09` | Các trang web của run cả tháng |
