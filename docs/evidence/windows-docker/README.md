# Bằng chứng giai đoạn 0 — tái lập trên Windows + Docker

Chạy ngày 2026-10-06 trên máy Windows 11 Home (i5-9300HF, RAM 7,9 GB), Docker Desktop 29.8.1 (WSL2, Docker thấy 6,2 GB RAM / 8 CPU).
Commit mã nguồn: `88fd989` + `.gitattributes` (chưa commit) — working tree LF.
Runtime trong container `ptit-bigdata:local`: OpenJDK 11.0.29, Apache Maven 3.9.11, Python 3.12.3. Hadoop 3.4.2 (LocalJobRunner, `fs.defaultFS=file:///`).

Dữ liệu dùng: **chỉ fixture 7 dòng** `src/test/resources/fixtures/events.csv` (D0). Chưa có dữ liệu Kaggle thật, chưa có HDFS.

| Task | Lệnh | Kết quả | File |
|---|---|---|---|
| T0.2 | `docker compose run --rm bigdata ./mvnw -B -Pintegration verify` | `BUILD SUCCESS`: enforcer JDK 11 pass, 18 unit test + 6 integration test pass, `spotless:check` pass | `mvn-integration-verify.log` |
| T0.2 | `docker compose run --rm bigdata python3 -m unittest discover -v -s scripts -p 'test_*.py'` | 5 test pass | `python-unittest.log` |
| T0.3 | `docker compose run --rm bigdata` (`scripts/demo.sh results/docker-demo`) | V1–V5 `validationStatus=valid`; `compare` V1 với V2..V5: 4 lần "Results equal"; `revenue.csv` khớp `expected-category-id.tsv` (1 → 60.00/3/20.00; 2 → 0.03/3/0.01) | `demo-fixture.log`, `demo-revenue.csv`, `demo-manifests/` |
| T0.4 | `docker compose run --rm bigdata ./mvnw -B -o dependency:tree` | Có `org.apache.hadoop:hadoop-hdfs-client:jar:3.4.2:provided` (bắc cầu); có mặt trong `.build-cache/classpath.txt` mà `scripts/run-local.sh` dùng | `mvn-dependency-tree.log` |

Ghi chú:

- `endToEndMillis` trong `demo-manifests/` (6,4–8,2 s cho 7 dòng) chủ yếu là chi phí khởi động JVM/LocalJobRunner và chu kỳ poll; **không** dùng làm số liệu hiệu năng.
- Có `hadoop-hdfs-client` trên classpath mới chỉ là điều kiện cần để đọc `hdfs://`; việc đọc/ghi HDFS thật chưa được xác minh (T1.5).
