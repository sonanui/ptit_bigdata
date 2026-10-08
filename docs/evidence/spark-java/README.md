# Bằng chứng giai đoạn 2 — Spark (Java) trên dữ liệu Kaggle thật

Chạy ngày 2026-10-06 trên host Windows 11 (Spark **local**, không Docker), HDFS trong Docker (`compose.yaml`).
Runtime: Spark 4.0.4 (`spark-submit` của PySpark trong `.venv`), JDK 21.0.12, Hadoop client 3.4.1 → HDFS 3.4.2.
`--master local[2]`, `--driver-memory 1g`, `spark.sql.shuffle.partitions=8`. Code: package `vn.edu.bigdata.revenue.spark`
(build `.\scripts\spark-local.ps1 build`, chạy `.\scripts\spark-pipeline.ps1`), commit nền `24861e8` + thay đổi chưa commit.

## Build và test

| Lệnh | Kết quả | File |
|---|---|---|
| `.\scripts\spark-local.ps1 build` (mvn `-Pspark package`, JDK 21) | BUILD SUCCESS; **23 test pass** = 18 test MR có sẵn (xác nhận tách `CsvEventParser.fields()` không đổi hành vi) + 5 test Spark | `build-test.log` |

Test Spark (`SparkJobsTest`): A1 trên fixture khớp oracle tính tay `expected-category-id.tsv`; phân loại dòng giống `PurchasePreparation`
(header, malformed thiếu/thừa trường, giá sai, nhóm sai, sự kiện lạ); ETL bảo toàn số dòng và lý do loại; A2–A5 trên fixture; A7 trên dữ liệu nhỏ tính tay.

## Kết quả trên D1 (1%) và D2 (10%) — `run_id` trong `d1-run-ids.tsv`, `d2-run-ids.tsv`

| Kiểm tra | D1 | D2 |
|---|---|---|
| A1 Spark (RDD `reduceByKey`) so với MR V1 (`docs/evidence/hdfs`) | **KHỚP** 308 nhóm, Σcount 7 464 | **KHỚP** 495 nhóm, Σcount 74 120 |
| A1 Spark so với baseline Python stdlib (`scripts/baseline_revenue.py`) | **KHỚP** | xem mục E1 trong plan |
| Lý do phân loại A1 = counter MR | HEADER 1, NON_PURCHASE 417 145, VALID 7 464 | HEADER 1, NON_PURCHASE 4 172 073, VALID 74 120 |
| ETL: rows in = valid + Σ loại | 424 610 = 424 609 + 1 (HEADER) | 4 246 194 = 4 246 193 + 1 |
| Parquet đọc lại = valid | 424 609 | 4 246 193 |
| A1 = A2 (purchase trên category_id hợp lệ) | 7 464 = 7 464 | 74 120 = 74 120 |
| Σevents mỗi bảng A2/A4/A5 = số dòng curated | đạt | đạt |
| Số dòng A2 / A4 / A5 | 595 / 743 / 2 474 | 614 / 744 / 3 139 |
| A7: sản phẩm / giữ lại (views ≥ 20) | 59 424 / 3 148 | 125 128 / 25 084 |

Lineage A1 (`*/revenue-rdd-lineage.txt`): `HadoopRDD` (block HDFS) → `flatMapToPair` (map) → `ShuffledRDD` (`reduceByKey`: gộp phía map, shuffle hash, gộp cuối).

## Chất lượng dữ liệu (D2, `d2/*/quality.json`)

- Chỉ có `view` 4 079 774, `cart` 92 299, `purchase` 74 120; **không có `remove_from_cart`** trong tháng 10/2019.
- `category_code` rỗng 1 353 242 dòng (31,9%), `brand` rỗng 612 424 (14,4%); `category_id`, `user_session` không rỗng.
- Giá bằng 0: 6 772 sự kiện, **0 purchase** giá 0. Mỗi `category_id` ứng với tối đa một `category_code`.
- Trùng hoàn toàn (trên trường đã chuẩn hóa): 586 dòng thừa trong 478 nhóm; giữ nguyên theo chính sách no-dedup như MR.
- Lưu ý: D1/D2 lấy mẫu theo **dòng**, nên số distinct user/session trên mẫu không đại diện cho cả tháng.

## Thời gian (một lần chạy, không phải benchmark)

| Job | D1 end-to-end ms | D2 end-to-end ms | Bước chính D2 |
|---|---:|---:|---|
| revenue (A1) | 10 682 | 34 277 | map+reduceByKey 28 256 |
| etl | 33 435 | 235 182 | ghi Parquet 103 576; đếm lý do 52 091; chất lượng 74 749 |
| metrics (A2–A5) | 28 120 | 99 375 | — |
| features (A7) | 24 815 | 179 578 | product_stats 163 005 (countDistinct + percentile_approx) |

Job metrics/features D2 chạy trùng lúc với `spotless:apply` trong Docker nên thời gian có nhiễu. Số liệu hiệu năng chính thức thuộc E2–E6.

## Ghi chú vận hành

- Lần chạy Python PySpark trước đó (đã bỏ theo yêu cầu: lõi viết bằng Java) cho thấy ETL D2 bị `OutOfMemoryError` với 1 GB khi persist dòng rộng;
  bản Java không persist, đọc raw 2 lượt (ghi curated, đếm lý do) và đo chất lượng trên Parquet đã ghi.
- `d*-*.log` là log `spark-submit` đầy đủ (mức INFO trước khi job đặt WARN).

## D3 — cả tháng 10/2019 (2026-10-06/07, commit `b0376ef` + thay đổi chưa commit)

Chạy bằng `.\scripts\spark-pipeline.ps1 -InputPath /data/ecommerce/raw/2019-Oct.csv -Tag d3` (`local[2]`, driver 1 GB, 8 shuffle partitions),
tổng 83 phút. run_id ở `d3-run-ids.tsv`, log `d3-*.log`, artifact cục bộ `d3/<run_id>/`.
Lỗi `ShutdownHookManager ... Failed to delete ...\Temp\spark-*` ở cuối một số log là Spark không xóa được thư mục tạm trên Windows khi tắt; job đã ghi "xong" trước đó.

| Kiểm tra | Kết quả D3 | Bằng chứng |
|---|---|---|
| Spark A1 (RDD) = baseline Python độc lập | Khớp: 567 nhóm, 742 849 purchase | `d3-baseline-compare.txt` |
| Spark A1 = MR V1 (output `e2-d3/e9b35a9b6869-1-v1` của E2) | Khớp tuyệt đối 567/567 nhóm (sum_minor, count, avg) | `d3/revenue_parity-mr-v1-vs-spark.json` (bước publish) |
| ETL bảo toàn số dòng | 42 448 765 vào = 42 448 764 hợp lệ + 1 header | `d3/20261006-224118-b0376ef-d3/etl-run.json` |
| Σpurchase A2 = Σcount A1 | 742 849 = 742 849 | `d3/20261006-230505-b0376ef-d3/metrics-run.json` |
| A7: sản phẩm / giữ (views ≥ 20) | 166 794 / 92 592; views p50/p75/p90/p95/p99 = 25/90/322/714/3 091 | `features-summary.json` |
| A8: train (t0 = 2019-10-15) / test (t0 = 2019-10-25) | 60 876 sp (28,0% dương) / 64 254 sp (25,3% dương); max event_time lịch sử < t0 ở cả hai | `d3/20261006-233917-b0376ef-d3/labels-summary.json` |

Chất lượng cả tháng (`quality.json`): view 40 779 399, cart 926 516, purchase 742 849, không có `remove_from_cart`; `category_code` rỗng 13 515 609 dòng (31,8%),
`brand` rỗng 6 113 008 (14,4%), `user_session` rỗng 2 dòng; giá 0: 68 673 sự kiện, 0 purchase giá 0; 166 794 sản phẩm, 3 022 290 user, 9 244 421 phiên.
Không đếm trùng lặp ở D3 (`--duplicates false`: phép đếm trùng toàn cột quá nặng cho RAM máy).

| Job (một lần chạy, không phải benchmark) | End-to-end ms | Bước chính |
|---|---:|---|
| revenue (A1, RDD) | 392 938 | map+reduceByKey 383 691 |
| etl | 1 414 028 | ghi Parquet 778 575; đếm lý do 383 258; chất lượng 246 242 |
| metrics (A2–A5) | 409 981 | — |
| features (A7) | 1 617 927 | product_stats 1 597 742 (countDistinct user + percentile_approx giá trên 42 triệu dòng) |
| labels (A8) | 1 076 345 | ghi 2 ảnh chụp 1 051 174 |
| publish serving | 21 714 | — |
