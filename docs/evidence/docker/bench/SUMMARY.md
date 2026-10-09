# Tổng hợp thực nghiệm E2–E6 (sinh bởi scripts/bench_summary.py, không sửa tay)

Phạm vi: một máy Windows 11 (i5-9300HF 4 nhân/8 luồng, RAM 7,9 GB); mọi thành phần chạy trong Docker Desktop (WSL2): HDFS 1 NameNode + 1 DataNode, MR = LocalJobRunner (không YARN) trong container bigdata, Spark = local[n] trong container spark.

Mỗi ô: 1 warmup + 3 lần đo; median (min–max), đơn vị ms.

## E2 — MapReduce V1–V5 trên D1

Chỉ số: Thời gian job MR. Nguồn: `docs/evidence/docker/bench/mr/mr-e2-d1/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| MR V1 | 6,703 | 3,982 | 8,828 | 3 |
| MR V2 | 4,322 | 4,275 | 5,307 | 3 |
| MR V3 | 4,209 | 3,474 | 7,813 | 3 |
| MR V4 | 4,847 | 4,842 | 5,261 | 3 |
| MR V5 | 3,296 | 2,413 | 6,247 | 3 |

- Thời gian job = submit tới hoàn tất job MR, không gồm preflight/validate của RevenueTool (xem details.endToEnd).
- 1 map slot (mặc định LocalJobRunner), 2 reducer, poll 100 ms.

## E2 — MapReduce V1–V5 trên D2

Chỉ số: Thời gian job MR. Nguồn: `docs/evidence/docker/bench/mr/mr-e2-d2/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| MR V1 | 28,864 | 26,687 | 30,630 | 3 |
| MR V2 | 22,747 | 21,779 | 24,076 | 3 |
| MR V3 | 26,741 | 23,188 | 41,307 | 3 |
| MR V4 | 25,886 | 22,466 | 35,785 | 3 |
| MR V5 | 10,268 | 8,257 | 16,670 | 3 |

- Thời gian job = submit tới hoàn tất job MR, không gồm preflight/validate của RevenueTool (xem details.endToEnd).
- 1 map slot (mặc định LocalJobRunner), 2 reducer, poll 100 ms.

## E2 — MapReduce V1–V5 trên D3

Chỉ số: Thời gian job MR. Nguồn: `docs/evidence/docker/bench/mr/mr-e2-d3/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| MR V1 | 235,790 | 234,198 | 258,404 | 3 |
| MR V2 | 264,119 | 256,242 | 286,006 | 3 |
| MR V5 | 90,494 | 78,992 | 111,137 | 3 |

- Thời gian job = submit tới hoàn tất job MR, không gồm preflight/validate của RevenueTool (xem details.endToEnd).
- 1 map slot (mặc định LocalJobRunner), 2 reducer, poll 100 ms.

## E3 — MapReduce: số map task chạy song song (D2)

Chỉ số: Thời gian job MR. Nguồn: `docs/evidence/docker/bench/mr/mr-e3-d2-maps*/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| MR V1, 1 map song song | 28,864 | 26,687 | 30,630 | 3 |
| MR V2, 1 map song song | 22,747 | 21,779 | 24,076 | 3 |
| MR V1, 2 map song song | 18,813 | 17,317 | 26,440 | 3 |
| MR V2, 2 map song song | 20,848 | 19,624 | 30,435 | 3 |
| MR V1, 4 map song song | 22,575 | 18,791 | 29,123 | 3 |
| MR V2, 4 map song song | 25,985 | 23,056 | 30,305 | 3 |

- LocalJobRunner chạy map trong một JVM; mapreduce.local.map.tasks.maximum = số luồng map.

## E4 — Spark A1: API và định dạng đầu vào (local[2], 8 partitions)

Chỉ số: Thời gian tính (không gồm ghi Parquet, khởi động). Nguồn: `docs/evidence/docker/bench/spark/e4-*/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| D1 · RDD trên CSV | 4,764 | 3,807 | 6,089 | 3 |
| D1 · DataFrame trên CSV | 7,521 | 7,435 | 9,376 | 3 |
| D1 · DataFrame trên Parquet curated | 6,525 | 5,869 | 14,389 | 3 |
| D2 · RDD trên CSV | 20,361 | 19,678 | 35,563 | 3 |
| D2 · DataFrame trên CSV | 31,457 | 23,363 | 57,715 | 3 |
| D2 · DataFrame trên Parquet curated | 14,323 | 8,557 | 14,537 | 3 |
| D3 · RDD trên CSV | 182,556 | 179,351 | 189,761 | 3 |
| D3 · DataFrame trên CSV | 229,698 | 224,287 | 242,600 | 3 |
| D3 · DataFrame trên Parquet curated | 24,942 | 10,959 | 31,414 | 3 |

- df-curated đọc Parquet đã qua ETL (chỉ 3 cột), nên không tính chi phí ETL một lần.
- Mọi lần chạy được so khớp chính xác với kết quả tham chiếu (MR V1 hoặc baseline).

## E4 — Spark A1 (RDD trên CSV): số core (D2)

Chỉ số: Thời gian tính. Nguồn: `docs/evidence/docker/bench/spark/e4-d2-rdd-raw*/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| local[1] | 45,775 | 32,236 | 47,623 | 3 |
| local[2] | 20,361 | 19,678 | 35,563 | 3 |
| local[4] | 31,231 | 24,675 | 32,058 | 3 |

- Máy có 4 nhân vật lý/8 luồng; HDFS (Docker) chạy cùng máy và chia CPU/RAM với Spark.
- Máy có 4 nhân vật lý/8 luồng; HDFS, MR và Spark cùng chia CPU/RAM của máy ảo Docker (8 CPU logic, khoảng 6,2 GB RAM).

## E4 — Spark A1 (DataFrame trên CSV): spark.sql.shuffle.partitions (D2)

Chỉ số: Thời gian tính. Nguồn: `docs/evidence/docker/bench/spark/e4-d2-df-raw*/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| 8 partitions | 31,457 | 23,363 | 57,715 | 3 |
| 64 partitions | 40,220 | 38,323 | 43,755 | 3 |
| 200 partitions | 49,843 | 36,297 | 58,606 | 3 |

- Số nhóm sau aggregate chỉ vài trăm, nhiều partition chủ yếu thêm task rỗng.

## E5 — MR và Spark cùng A1: chỉ thời gian xử lý

Chỉ số: Job MR / action Spark. Nguồn: `docs/evidence/docker/bench/{mr,spark}`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| D2 · MR V1 (1 map slot) | 28,864 | 26,687 | 30,630 | 3 |
| D2 · MR V5 (1 map slot) | 10,268 | 8,257 | 16,670 | 3 |
| D2 · Spark RDD local[1] | 45,775 | 32,236 | 47,623 | 3 |
| D2 · Spark RDD local[2] | 20,361 | 19,678 | 35,563 | 3 |
| D3 · MR V1 (1 map slot) | 235,790 | 234,198 | 258,404 | 3 |
| D3 · MR V5 (1 map slot) | 90,494 | 78,992 | 111,137 | 3 |
| D3 · Spark RDD local[1] | 232,911 | 227,706 | 243,858 | 3 |
| D3 · Spark RDD local[2] | 182,556 | 179,351 | 189,761 | 3 |

- MR: thời gian job (map + shuffle + reduce). Spark: các stage tính, không gồm ghi Parquet.
- So sánh local[1] với MR 1 map slot là cặp cùng mức song song.
- MR (container bigdata) và Spark (container spark) cùng đọc HDFS qua mạng Compose trong cùng máy ảo WSL2; hai engine vẫn khác JVM, khác cách đọc CSV và khác chi phí khởi động, nên chênh lệch không chỉ do mô hình MapReduce hay Spark.

## E5 — MR và Spark cùng A1: end-to-end

Chỉ số: Thời gian end-to-end. Nguồn: `docs/evidence/docker/bench/{mr,spark}`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| D2 · MR V1 | 75,027 | 70,108 | 77,359 | 3 |
| D2 · Spark RDD local[2] | 37,806 | 33,923 | 53,308 | 3 |
| D3 · MR V1 | 615,395 | 612,329 | 667,497 | 3 |
| D3 · Spark RDD local[2] | 206,653 | 201,451 | 217,017 | 3 |

- MR end-to-end gồm quét preflight lại + SHA-256 trước/sau job (RevenueTool, F4) trong container.
- Spark end-to-end = thời gian đồng hồ của spark-submit (khởi động JVM + SparkSession + ghi).

## E6 — Mở rộng theo kích thước dữ liệu (D1 → D2 → D3)

Chỉ số: Thời gian xử lý. Nguồn: `docs/evidence/docker/bench/{mr,spark}`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| D1 · MR V1 | 6,703 | 3,982 | 8,828 | 3 |
| D1 · MR V5 | 3,296 | 2,413 | 6,247 | 3 |
| D1 · Spark RDD local[2] | 4,764 | 3,807 | 6,089 | 3 |
| D1 · Spark DF Parquet local[2] | 6,525 | 5,869 | 14,389 | 3 |
| D2 · MR V1 | 28,864 | 26,687 | 30,630 | 3 |
| D2 · MR V5 | 10,268 | 8,257 | 16,670 | 3 |
| D2 · Spark RDD local[2] | 20,361 | 19,678 | 35,563 | 3 |
| D2 · Spark DF Parquet local[2] | 14,323 | 8,557 | 14,537 | 3 |
| D3 · MR V1 | 235,790 | 234,198 | 258,404 | 3 |
| D3 · MR V5 | 90,494 | 78,992 | 111,137 | 3 |
| D3 · Spark RDD local[2] | 182,556 | 179,351 | 189,761 | 3 |
| D3 · Spark DF Parquet local[2] | 24,942 | 10,959 | 31,414 | 3 |

- Đây là mở rộng theo kích thước dữ liệu trên một máy, không phải mở rộng cụm.
- Throughput tính theo số dòng/byte CSV gốc, kể cả dòng không phải purchase.
- MR (container bigdata) và Spark (container spark) cùng đọc HDFS qua mạng Compose trong cùng máy ảo WSL2; hai engine vẫn khác JVM, khác cách đọc CSV và khác chi phí khởi động, nên chênh lệch không chỉ do mô hình MapReduce hay Spark.

## E7 — K-Means lặp: có và không cache (D3, 92 592 sản phẩm)

Chỉ số: Thời gian fit (20 vòng lặp, K = 2). Nguồn: `docs/evidence/docker/ml/20261008-201313-97bb8c2-dk3/e7.csv`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| Không cache | 3,532 | 2,846 | 3,785 | 3 |
| cache() + count() trước khi fit | 1,376 | 1,152 | 1,875 | 3 |

- Chi phí nạp cache (count) báo riêng trong details.cacheCountMillis; tính cả bước này thì cache không có lợi ở quy mô và số vòng lặp này.

