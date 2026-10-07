# Bằng chứng các lần chạy (evidence)

Mỗi thư mục là nhật ký kết quả **đã chạy thật**: log, manifest, `_run.json`, bảng kết quả. Nội dung được giữ nguyên như lúc ghi để
truy vết; số liệu trong báo cáo và web phải trỏ được về đây.

| Thư mục | Nội dung |
|---|---|
| `windows-docker/` | Giai đoạn 0: build và test trong Docker trên Windows, demo fixture |
| `hdfs/` | Giai đoạn 1: HDFS, nạp `2019-Oct.csv`, mẫu D1/D2 tất định, MapReduce V1–V5 trên HDFS |
| `spark-java/` | Giai đoạn 2: job Spark Java trên D1, D2, D3; `plans/` chứa lineage RDD và physical plan DataFrame thật |
| `bench/` | Giai đoạn 3: benchmark MapReduce (`mr/`) và Spark (`spark/`), bảng tổng hợp `SUMMARY.md`, bảng cho web `serving/` |
| `ml/` | Giai đoạn 4: K-Means, KNN (D2 và D3), mô hình, metric, log notebook |
| `webapp/` | Giai đoạn 6: build/test backend và frontend, Docker, độ trễ API, kiểm tra bản clone |
| `default-poll5000/`, `*.json`, `summary.csv` ở thư mục này | Benchmark synthetic ban đầu trên máy tác giả (macOS), xem `docs/benchmark-report.md` |

**Đường dẫn đã đổi sau ngày 2026-10-07.** Code lõi chuyển vào module con `bigdata/`: đường dẫn `src/...`, `pom.xml`,
`target/revenue-aggregation*.jar` ghi trong các log và README trước ngày này tương ứng với `bigdata/src/...`, `bigdata/pom.xml`,
`bigdata/target/...` hiện nay. Web gộp một container (`webapp/Dockerfile`, service `webapp`) nay tách thành `web-backend` và `web-frontend`.
