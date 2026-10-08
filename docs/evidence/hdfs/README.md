# Bằng chứng giai đoạn 1 — HDFS + Hadoop MapReduce trên dữ liệu Kaggle thật

Chạy ngày 2026-10-06 trên Windows 11 + Docker Desktop 29.8.1 (WSL2, 6,2 GB RAM / 8 CPU cho Docker).
HDFS: `apache/hadoop:3.4.2`, 1 NameNode + 1 DataNode, replication 1, block 128 MB (`config/hadoop.env`).
MR: `ptit-bigdata:local` (JDK 11.0.29, Hadoop 3.4.2 **LocalJobRunner**, không YARN), input và output trên `hdfs://namenode:8020`.
Mã nguồn Java: commit `88fd989`, không sửa; chỉ thêm script/compose.

## Tóm tắt

| Task | Kết quả | File |
|---|---|---|
| T1.1 HDFS | 1 DataNode live, safe mode OFF, `dfs.replication=1`, `dfs.blocksize=134217728` | `t1.1-hdfs-up.log` |
| T1.3 Ingest | `2019-Oct.csv` → `/data/ecommerce/raw/`: 5 668 612 855 bytes (= file gốc), 43 block, fsck HEALTHY, 0 under-replicated; 705 s. Chạy lại: `SKIP`, không ghi đè | `t1.3-ingest-oct.log`, `t1.3-ingest-oct-rerun.log` |
| T1.4 Preflight nguồn | Đọc qua HDFS: SHA-256 `fedd9384…5b80` **trùng** SHA-256 file gốc trên host (`docs/DATASET.md`); 42 448 765 dòng; 742 849 purchase hợp lệ; 0 lỗi; 285 s | `source-2019-Oct/` |
| T1.4 Mẫu D1 (rate 0.01, seed 21) | 424 609 sự kiện, 7 464 purchase, 56 696 727 bytes, SHA-256 `f15fa446…7bc7`; 574 s | `sample-2019-Oct-r0.01-s21/`, `t1.4-sample-oct-r0.01.log` |
| T1.4 Tất định | Lấy mẫu lại cùng rate/seed ra đường dẫn khác: **cùng SHA-256** `f15fa446…7bc7` | `determinism-2019-Oct-r0.01-s21-rerun/`, `t1.4-sample-oct-r0.1-and-determinism.log` |
| T1.4 Mẫu D2 (rate 0.1, seed 21) | 4 246 193 sự kiện, 74 120 purchase, 567 001 795 bytes, SHA-256 `49b67a6f…44ee`; 627 s | `sample-2019-Oct-r0.1-s21/` |
| T1.5 D0 trên HDFS | Fixture 7 dòng: V1–V5 valid, 4× "Results equal", `revenue.csv` khớp oracle | `t1.5-d0-hdfs-smoke-03.log` |
| T1.5 D1 | V1–V5 valid, 4× "Results equal", 308 nhóm `category_id` | `20261006-032500-88fd989-d1/`, `t1.5-d1-mr-*.log` |
| T1.5 D2 | V1–V5 valid, 4× "Results equal", 495 nhóm | `20261006-034832-88fd989-d2/`, `t1.5-d2-mr-*.log` |

## Counter MR (một lần chạy, không phải benchmark)

Bảo toàn số dòng: `MAP_INPUT_RECORDS` = `HEADERS` + `NON_PURCHASE` + `VALID_PURCHASE`; `INVALID_PRICE` = 0, `MALFORMED_CSV` = 0 ở cả D1 và D2.

| Dữ liệu | Biến thể | Map input | Map output records | Reduce shuffle bytes | Preflight trong RevenueTool (ms) | Job (ms) | End-to-end (ms) |
|---|---|---:|---:|---:|---:|---:|---:|
| D1 | V1 | 424 610 | 7 464 | 283 644 | 3 156 | 11 179 | 16 744 |
| D1 | V2 | 424 610 | 7 464 | 11 716 | 3 990 | 10 726 | 17 267 |
| D1 | V3 | 424 610 | 308 | 11 716 | 3 150 | 5 813 | 11 001 |
| D1 | V4 | 424 610 | 308 | 11 716 | 3 844 | 6 209 | 12 094 |
| D1 | V5 | 424 610 | 2 | 6 196 | 4 408 | 6 394 | 14 182 |
| D2 | V1 | 4 246 194 | 74 120 | 2 816 620 | 32 341 | 56 287 | 103 274 |
| D2 | V2 | 4 246 194 | 74 120 | 66 864 | 30 446 | 45 742 | 94 413 |
| D2 | V3 ⚠ | 4 246 194 | 1 758 | 66 864 | 59 975 | 36 387 | 117 229 |
| D2 | V4 ⚠ | 4 246 194 | 1 758 | 66 864 | 87 069 | 128 211 | 250 148 |
| D2 | V5 | 4 246 194 | 10 | 35 340 | 39 671 | 11 746 | 65 493 |

⚠ **Số đo thời gian D2-V3 và D2-V4 bị nhiễu:** trong lúc chạy, host đang tải/giải nén image `apache/spark` và chạy một container kiểm tra. Các cột counter (record, byte) không bị ảnh hưởng; cột thời gian của hai dòng này **không được dùng** làm số liệu. Toàn bộ bảng là một lần chạy, chưa có warmup/lặp lại; số liệu hiệu năng chính thức thuộc E2 (§12 của plan).

Thời gian các bước khác (`timings.tsv`, tính cả khởi động JVM): D1 preflight 10,2 s, profile 15,6 s; D2 preflight 53,2 s, profile 76,1 s.

## Nhận xét đã kiểm chứng và điểm chưa giải thích

- **F4 có thật:** preflight đọc HDFS đơn luồng, CPU container ~100% một lõi, khoảng 17–18 MB/s (5,67 GB trong 285 s). Lấy mẫu quét nguồn thêm 2 lần (574–627 s mỗi mẫu). Trên D2, phần preflight bên trong `RevenueTool` (30–40 s) cùng cỡ với thời gian job (12–56 s).
- **F5, F6 không xảy ra trên tháng 10:** preflight toàn file 0 lỗi; counter `INVALID_PRICE`/`MALFORMED_CSV` = 0 trên D1, D2. Với toàn file chỉ biết tổng purchase hợp lệ (742 849), chưa đếm purchase bị loại.
- **`HDFS_BYTES_READ` chưa giải thích:** luôn bằng đúng 6,0 lần kích thước file đầu vào, giống nhau cho mọi biến thể. Giả thuyết (chưa xác minh): với LocalJobRunner, counter FileSystem lấy từ thống kê dùng chung của cả JVM nên tính cả các lượt đọc ngoài task. Không dùng counter này làm số liệu E2.
- **Replication phía client:** container `bigdata` không đọc `hdfs-site.xml` của cluster nên mặc định ghi replication 3. Lần chạy `t1.5-d0-hdfs-smoke-02.log` bị lỗi này (đã `setrep 1`); các script hiện truyền `-Ddfs.replication=1`. `t1.5-d0-hdfs-smoke.log` là lần thử thất bại do `FsShell` cần `core-site.xml` (đã sửa trong `scripts/hdfs-mr.sh`).
- Nạp HDFS chỉ đạt ~7,7 MB/s vì đọc nguồn từ ổ E: qua bind mount của WSL2; đây là chi phí một lần.
