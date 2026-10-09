# Môi trường local bằng Docker

Image `ptit-bigdata:local` chứa Java 11, Maven 3.9.11, Python 3 và module `bigdata` đã build (jar `bigdata/target/revenue-aggregation.jar`). MapReduce chạy bằng LocalJobRunner trong container `bigdata`. HDFS là hai container riêng (`namenode`, `datanode`, image `apache/hadoop:3.4.2`), xem mục HDFS bên dưới. Không có YARN. Phân bổ Docker/host đầy đủ: `docs/PROJECT_AUDIT_AND_IMPLEMENTATION_PLAN.md` §7.6. Java/Maven không cần cài trên host.

`compose.yaml` chia ba profile: `bigdata` (`bigdata`, `namenode`, `datanode`), `spark` (`spark`: Spark 4.0.4 + notebook ML, xem mục Spark bên dưới) và `web` (`web-backend`, `web-frontend`).
Gọi đích danh service thì profile tự bật (`docker compose up -d namenode datanode`, `docker compose run --rm bigdata ...`);
`docker compose --profile web up -d` chỉ khởi động web, không bật HDFS. Cần Docker Engine/Desktop và Docker Compose plugin; build cần Internet để tải base image, apt packages và Maven dependencies.

## Build và demo

```bash
mkdir -p data/raw data/samples results
docker compose build bigdata
docker compose run --rm bigdata
```

Demo chạy preflight/profile, năm variants, so output với V1 và export CSV. Kết quả nằm `results/docker-demo` trên host. Output phải mới; lần chạy sau chọn đường dẫn khác:

```bash
docker compose run --rm bigdata bash scripts/demo.sh results/docker-demo-02
```

Source/config/scripts nằm trong image; sửa chúng thì build lại. Chỉ data và results được bind-mount. Không đưa dataset Kaggle, cache, tools hay results host vào build context.

## Kiểm thử

```bash
docker compose run --rm bigdata ./mvnw -B -pl bigdata -Pintegration verify
docker compose run --rm bigdata python3 -m unittest discover -s scripts -p 'test_*.py'
```

Image build compile/package; hai lệnh trên chạy test. Không ép platform amd64 để image có thể chạy theo architecture host nếu base image hỗ trợ.

## Dữ liệu và benchmark

Chuẩn bị synthetic workload và metadata, dùng các thư mục output chưa tồn tại:

```bash
docker compose run --rm bigdata python3 scripts/generate_workload.py --output data/samples/optimization-200k.csv --rows 200000 --groups 32 --purchase-rate 1 --seed 21
docker compose run --rm bigdata scripts/prepare-input.sh data/samples/optimization-200k.csv results/optimization-meta
docker compose run --rm bigdata scripts/run-local.sh --tool DatasetTool profile --manifest results/optimization-meta/input.json --output results/optimization-meta/profile.json --dictionary results/optimization-meta/groups.json
docker compose run --rm bigdata python3 scripts/benchmark.py --matrix config/benchmark-matrix.json
```

CSV thật đặt dưới data/raw trên host rồi dùng cùng CLI với đường dẫn tương đối `data/raw/...`. Manifest tạo bên host bằng đường dẫn tuyệt đối không thể dùng nguyên trạng bên container: tạo preflight/profile trong container để giữ URI chính xác.

## Chạy CLI hoặc shell

```bash
docker compose run --rm bigdata scripts/run-local.sh --variant v3 --manifest results/optimization-meta/input.json --preflight results/optimization-meta/preflight.json --output results/docker-v3 --group-by category_id --reducers 2 --max-keys 10000
docker compose run --rm bigdata bash
```

Không dùng Compose cũng được:

```bash
docker build -t ptit-bigdata:local .
docker run --rm -v "$PWD/data:/opt/bigdata/data" -v "$PWD/results:/opt/bigdata/results" ptit-bigdata:local
```

## HDFS (pseudo-distributed, 1 NameNode + 1 DataNode)

Cấu hình nằm trong `config/hadoop.env` (replication 1, block 128 MB, `fs.defaultFS=hdfs://namenode:8020`). Dữ liệu HDFS nằm trong named volume `hdfs-namenode`, `hdfs-datanode` (đĩa ảo WSL2, tức ổ C: trên Windows).

```bash
docker compose up -d namenode datanode
docker compose exec namenode hdfs dfsadmin -report        # phải có "Live datanodes (1)"
```

UI: NameNode http://127.0.0.1:9870, DataNode http://127.0.0.1:9864. Client Spark chạy local trên host dùng `hdfs://localhost:8020` (NameNode publish cổng 8020, DataNode khai báo hostname `localhost` và publish 9866), xem `docs/spark-local.md`.

Dataset gốc do người dùng tự đặt vào `data/raw/` (xem `docs/DATASET.md`); pipeline không tự tải. Nạp vào HDFS (chạy lại an toàn: file đã có cùng kích thước thì bỏ qua, khác kích thước thì báo lỗi, không bao giờ ghi đè):

```bash
docker compose exec namenode bash /opt/bigdata/scripts/hdfs-ingest.sh 2019-Oct.csv
```

Trên Git Bash, thêm `MSYS_NO_PATHCONV=1` trước các lệnh `docker compose` có đường dẫn tuyệt đối kiểu Linux, nếu không Git Bash sẽ đổi `/opt/...` thành đường dẫn Windows. PowerShell không cần.

Mẫu tất định và MapReduce đọc/ghi HDFS (chạy trong `bigdata`; output phải mới):

```bash
docker compose run --rm bigdata scripts/hdfs-sample.sh /data/ecommerce/raw/2019-Oct.csv 0.01 21
docker compose run --rm bigdata scripts/hdfs-mr.sh /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv <RUN_ID> [v1 v3 v5]
```

Container `bigdata` không đọc `hdfs-site.xml` của cluster, nên các script truyền `-Ddfs.replication=1` từ phía client. Nếu gọi `scripts/run-local.sh` trực tiếp với URI `hdfs://`, phải tự thêm tùy chọn này, nếu không file ghi ra có replication 3 và bị under-replicated.

Tắt HDFS: `docker compose stop namenode datanode` (giữ dữ liệu). `docker compose down -v` **xóa** volume HDFS. Chỉ dùng khi chắc chắn muốn nạp lại từ đầu.

## Spark và notebook ML (container `spark`, profile `spark`)

Image `ptit-bigdata-spark:local` (`docker/spark/Dockerfile`): JDK 21, PySpark 4.0.4 (cung cấp `spark-submit`), numpy, pandas,
nbconvert; jar `revenue-aggregation-spark.jar` được build và chạy 25 test ngay khi build image. Repo bind-mount vào `/opt/project`,
nên log, `results/` và `docs/evidence/` ghi thẳng ra máy. Spark đọc HDFS qua `hdfs://namenode:8020` trong mạng Compose
(`HDFS_USE_DATANODE_HOSTNAME=false`). `--service-ports` mở Spark UI ở http://localhost:4040 khi job đang chạy.

```bash
docker compose build spark
docker compose run --rm --service-ports spark bash scripts/spark-pipeline.sh /data/ecommerce/raw/2019-Oct.csv dk3   # A1, ETL, A2-A5, A7, A8
docker compose run --rm -e ML_TAG=dk3 spark bash -c "cd notebooks && jupyter nbconvert --to notebook --execute --inplace kmeans_product.ipynb"
docker compose run --rm --service-ports spark python scripts/spark_bench.py --campaign config/bench/docker/campaign.json
docker compose run --rm spark python scripts/bench_summary.py --campaign config/bench/docker/campaign.json
docker compose run --rm spark bash scripts/spark-submit.sh publish --tag dk3 ...   # tham số: báo cáo mục 3.2 bước 14
```

Image `bigdata` chép `config/` lúc build: ma trận benchmark tạo sau khi build thì mount thêm `-v <repo>/config:/opt/bigdata/config:ro`.
Toàn bộ quy trình đã chạy ngày 2026-10-08/09 (báo cáo v7, `reports/raw/`, `docs/evidence/docker/`).

## Web app (2 container, profile `web`)

```bash
docker compose --profile web up -d --build      # http://localhost:8080 (React qua Nginx), API thử trực tiếp: http://localhost:8081/api/health
docker compose --profile web down
```

- `web-backend` (`webapp/backend/Dockerfile`): Spring Boot, JRE 21, chỉ phục vụ `/api`; mount `./serving` chỉ đọc tại `/serving`;
  healthcheck gọi `/api/health`; nạp sẵn mô hình của serving run mặc định khi khởi động (tắt bằng `SERVING_WARMUP=false`).
- `web-frontend` (`webapp/frontend/Dockerfile`, `nginx.conf`): build React (chạy `npm test` trong lúc build) rồi phục vụ bằng Nginx;
  chuyển `/api/` sang `web-backend:8080`; route React được trả `index.html`. Khởi động sau khi backend healthy.
- Không cần HDFS/Spark khi chạy web; RAM giới hạn 768 MB (backend) và 128 MB (frontend).

## Phạm vi kiểm chứng

Ngày 2026-10-06, trên Windows 11 + Docker Desktop 29.8.1 (WSL2): build image, `-Pintegration verify`, test Python và demo fixture đều chạy được (`docs/evidence/windows-docker/`); HDFS lên được, nạp `2019-Oct.csv` (43 block, HEALTHY) và chạy V1–V5 trên fixture đặt trên HDFS (`docs/evidence/hdfs/`). Benchmark trong `docs/evidence/` (không thuộc thư mục con) vẫn là số đo trên macOS của tác giả, không phải trong container.
Ngày 2026-10-07, sau khi tách module `bigdata` và web 2 container: build image, `-pl bigdata -Pintegration verify` (18 + 6 test, spotless),
demo fixture, MR V1/V5 trên D1 qua HDFS (khớp kết quả trước refactor) và web qua Nginx đều chạy lại được.
