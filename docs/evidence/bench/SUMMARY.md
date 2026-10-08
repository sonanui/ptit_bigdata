# Tổng hợp thực nghiệm E2–E6 (sinh bởi scripts/bench_summary.py, không sửa tay)

Phạm vi: một máy Windows 11 (i5-9300HF 4 nhân/8 luồng, RAM 7,9 GB); HDFS 1 NameNode + 1 DataNode trong Docker; MR = LocalJobRunner (không YARN), Spark = local[n] trên host.

Mỗi ô: 1 warmup + 3 lần đo; median (min–max), đơn vị ms.

## E2 — MapReduce V1–V5 trên D1

Chỉ số: Thời gian job MR. Nguồn: `docs/evidence/bench/mr/mr-e2-d1/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| MR V1 | 4,753 | 4,325 | 6,607 | 3 |
| MR V2 | 4,394 | 4,097 | 8,128 | 3 |
| MR V3 | 5,906 | 4,411 | 14,352 | 3 |
| MR V4 | 7,670 | 6,820 | 7,987 | 3 |
| MR V5 | 3,206 | 2,778 | 4,146 | 3 |

- Thời gian job = submit tới hoàn tất job MR, không gồm preflight/validate của RevenueTool (xem details.endToEnd).
- 1 map slot (mặc định LocalJobRunner), 2 reducer, poll 100 ms.

## E2 — MapReduce V1–V5 trên D2

Chỉ số: Thời gian job MR. Nguồn: `docs/evidence/bench/mr/mr-e2-d2/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| MR V1 | 29,221 | 21,346 | 37,034 | 3 |
| MR V2 | 27,161 | 23,890 | 31,702 | 3 |
| MR V3 | 33,581 | 22,199 | 45,334 | 3 |
| MR V4 | 31,956 | 26,350 | 33,017 | 3 |
| MR V5 | 10,418 | 9,841 | 19,278 | 3 |

- Thời gian job = submit tới hoàn tất job MR, không gồm preflight/validate của RevenueTool (xem details.endToEnd).
- 1 map slot (mặc định LocalJobRunner), 2 reducer, poll 100 ms.

## E2 — MapReduce V1–V5 trên D3

Chỉ số: Thời gian job MR. Nguồn: `docs/evidence/bench/mr/mr-e2-d3/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| MR V1 | 189,970 | 180,149 | 190,204 | 3 |
| MR V2 | 185,022 | 166,170 | 185,153 | 3 |
| MR V5 | 61,236 | 59,069 | 74,822 | 3 |

- Thời gian job = submit tới hoàn tất job MR, không gồm preflight/validate của RevenueTool (xem details.endToEnd).
- 1 map slot (mặc định LocalJobRunner), 2 reducer, poll 100 ms.

## E3 — MapReduce: số map task chạy song song (D2)

Chỉ số: Thời gian job MR. Nguồn: `docs/evidence/bench/mr/mr-e3-d2-maps*/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| MR V1, 1 map song song | 29,221 | 21,346 | 37,034 | 3 |
| MR V2, 1 map song song | 27,161 | 23,890 | 31,702 | 3 |
| MR V1, 2 map song song | 20,128 | 17,244 | 48,759 | 3 |
| MR V2, 2 map song song | 18,242 | 17,552 | 24,470 | 3 |
| MR V1, 4 map song song | 29,340 | 21,768 | 31,056 | 3 |
| MR V2, 4 map song song | 30,842 | 28,642 | 31,661 | 3 |

- LocalJobRunner chạy map trong một JVM; mapreduce.local.map.tasks.maximum = số luồng map.

## E4 — Spark A1: API và định dạng đầu vào (local[2], 8 partitions)

Chỉ số: Thời gian tính (không gồm ghi Parquet, khởi động). Nguồn: `docs/evidence/bench/spark/e4-*/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| D1 · RDD trên CSV | 4,989 | 4,919 | 6,001 | 3 |
| D1 · DataFrame trên CSV | 10,519 | 8,791 | 12,286 | 3 |
| D1 · DataFrame trên Parquet curated | 7,512 | 7,012 | 14,054 | 3 |
| D2 · RDD trên CSV | 28,345 | 24,462 | 33,987 | 3 |
| D2 · DataFrame trên CSV | 40,438 | 34,011 | 41,985 | 3 |
| D2 · DataFrame trên Parquet curated | 10,182 | 9,154 | 14,985 | 3 |
| D3 · RDD trên CSV | 227,302 | 218,846 | 231,547 | 3 |
| D3 · DataFrame trên CSV | 272,220 | 271,424 | 273,698 | 3 |
| D3 · DataFrame trên Parquet curated | 18,279 | 15,287 | 18,719 | 3 |

- df-curated đọc Parquet đã qua ETL (chỉ 3 cột), nên không tính chi phí ETL một lần.
- Mọi lần chạy được so khớp chính xác với kết quả tham chiếu (MR V1 hoặc baseline).

## E4 — Spark A1 (RDD trên CSV): số core (D2)

Chỉ số: Thời gian tính. Nguồn: `docs/evidence/bench/spark/e4-d2-rdd-raw*/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| local[1] | 31,056 | 27,383 | 33,118 | 3 |
| local[2] | 28,345 | 24,462 | 33,987 | 3 |
| local[4] | 36,500 | 25,012 | 37,592 | 3 |

- Máy có 4 nhân vật lý/8 luồng; HDFS (Docker) chạy cùng máy và chia CPU/RAM với Spark.
- Thêm core không giảm thời gian: nhiều khả năng bị giới hạn bởi đọc HDFS qua cổng chuyển tiếp WSL2 (chưa đo riêng).

## E4 — Spark A1 (DataFrame trên CSV): spark.sql.shuffle.partitions (D2)

Chỉ số: Thời gian tính. Nguồn: `docs/evidence/bench/spark/e4-d2-df-raw*/runs.json`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| 8 partitions | 40,438 | 34,011 | 41,985 | 3 |
| 64 partitions | 36,156 | 32,588 | 40,704 | 3 |
| 200 partitions | 42,866 | 37,625 | 43,425 | 3 |

- Số nhóm sau aggregate chỉ vài trăm, nhiều partition chủ yếu thêm task rỗng.

## E5 — MR và Spark cùng A1: chỉ thời gian xử lý

Chỉ số: Job MR / action Spark. Nguồn: `docs/evidence/bench/{mr,spark}`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| D2 · MR V1 (1 map slot) | 29,221 | 21,346 | 37,034 | 3 |
| D2 · MR V5 (1 map slot) | 10,418 | 9,841 | 19,278 | 3 |
| D2 · Spark RDD local[1] | 31,056 | 27,383 | 33,118 | 3 |
| D2 · Spark RDD local[2] | 28,345 | 24,462 | 33,987 | 3 |
| D3 · MR V1 (1 map slot) | 189,970 | 180,149 | 190,204 | 3 |
| D3 · MR V5 (1 map slot) | 61,236 | 59,069 | 74,822 | 3 |
| D3 · Spark RDD local[1] | 269,895 | 267,091 | 277,246 | 3 |
| D3 · Spark RDD local[2] | 227,302 | 218,846 | 231,547 | 3 |

- MR: thời gian job (map + shuffle + reduce). Spark: các stage tính, không gồm ghi Parquet.
- So sánh local[1] với MR 1 map slot là cặp cùng mức song song.
- Môi trường I/O khác nhau: MR chạy trong container Linux cùng mạng Docker với DataNode; Spark chạy trên host Windows, đọc HDFS qua cổng 127.0.0.1 do Docker/WSL2 chuyển tiếp. Chênh lệch thời gian không chỉ do engine.

## E5 — MR và Spark cùng A1: end-to-end

Chỉ số: Thời gian end-to-end. Nguồn: `docs/evidence/bench/{mr,spark}`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| D2 · MR V1 | 82,332 | 69,036 | 85,302 | 3 |
| D2 · Spark RDD local[2] | 49,216 | 39,361 | 55,363 | 3 |
| D3 · MR V1 | 525,119 | 502,770 | 541,674 | 3 |
| D3 · Spark RDD local[2] | 246,771 | 239,435 | 249,243 | 3 |

- MR end-to-end gồm quét preflight lại + SHA-256 trước/sau job (RevenueTool, F4) trong container.
- Spark end-to-end = thời gian đồng hồ của spark-submit (khởi động JVM + SparkSession + ghi).

## E6 — Mở rộng theo kích thước dữ liệu (D1 → D2 → D3)

Chỉ số: Thời gian xử lý. Nguồn: `docs/evidence/bench/{mr,spark}`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| D1 · MR V1 | 4,753 | 4,325 | 6,607 | 3 |
| D1 · MR V5 | 3,206 | 2,778 | 4,146 | 3 |
| D1 · Spark RDD local[2] | 4,989 | 4,919 | 6,001 | 3 |
| D1 · Spark DF Parquet local[2] | 7,512 | 7,012 | 14,054 | 3 |
| D2 · MR V1 | 29,221 | 21,346 | 37,034 | 3 |
| D2 · MR V5 | 10,418 | 9,841 | 19,278 | 3 |
| D2 · Spark RDD local[2] | 28,345 | 24,462 | 33,987 | 3 |
| D2 · Spark DF Parquet local[2] | 10,182 | 9,154 | 14,985 | 3 |
| D3 · MR V1 | 189,970 | 180,149 | 190,204 | 3 |
| D3 · MR V5 | 61,236 | 59,069 | 74,822 | 3 |
| D3 · Spark RDD local[2] | 227,302 | 218,846 | 231,547 | 3 |
| D3 · Spark DF Parquet local[2] | 18,279 | 15,287 | 18,719 | 3 |

- Đây là mở rộng theo kích thước dữ liệu trên một máy, không phải mở rộng cụm.
- Throughput tính theo số dòng/byte CSV gốc, kể cả dòng không phải purchase.
- Môi trường I/O khác nhau: MR chạy trong container Linux cùng mạng Docker với DataNode; Spark chạy trên host Windows, đọc HDFS qua cổng 127.0.0.1 do Docker/WSL2 chuyển tiếp. Chênh lệch thời gian không chỉ do engine.

## E7 — K-Means lặp: có và không cache (D3, 92 592 sản phẩm)

Chỉ số: Thời gian fit (20 vòng lặp, K = 2). Nguồn: `docs/evidence/ml/20261006-235739-b0376ef-d3/e7.csv`

| Cấu hình | Median | Min | Max | Lần đo |
|---|---:|---:|---:|---:|
| Không cache | 5,770 | 5,753 | 6,954 | 3 |
| cache() + count() trước khi fit | 4,849 | 4,387 | 5,932 | 3 |

- Chi phí nạp cache (count) báo riêng trong details.cacheCountMillis; tính cả bước này thì cache không có lợi ở quy mô và số vòng lặp này.

