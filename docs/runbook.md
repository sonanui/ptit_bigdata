# Chạy và đo MapReduce (module `bigdata`)

Tài liệu cho người làm phần Hadoop MapReduce: build, test, chạy từng biến thể, benchmark. Luồng đầy đủ HDFS → MR/Spark → ML → web:
`docs/END_TO_END.md`. Chạy trên HDFS và dữ liệu Kaggle thật: `docs/docker.md`.

Module MapReduce biên dịch theo Java 11 (enforcer `[11,12)`), nên cách chạy chính thức là **trong container `bigdata`**
(image Maven + JDK 11, build từ `Dockerfile` ở gốc repo). Máy có JDK 11 có thể chạy trực tiếp các lệnh `./mvnw`, `scripts/*.sh`.

## Build và kiểm thử

```bash
docker compose build bigdata
docker compose run --rm bigdata ./mvnw -B -pl bigdata -Pintegration verify      # 18 unit + 6 integration test + spotless
docker compose run --rm bigdata python3 -m unittest discover -s scripts -p 'test_*.py'
docker compose run --rm bigdata bash scripts/demo.sh results/my-demo            # fixture 7 dòng, V1–V5, so khớp, export CSV
```

Jar MapReduce: `bigdata/target/revenue-aggregation.jar`; classpath Hadoop: `.build-cache/classpath.txt` (tạo khi build image
hoặc lần đầu chạy `scripts/run-local.sh`). Build trên host với JDK ≥ 17 sẽ bật profile `spark` và ra
`bigdata/target/revenue-aggregation-spark.jar` (xem `docs/spark-local.md`).

## Cấu hình Hadoop

`scripts/run-local.sh` nạp `config/local.properties` thành các tham số `-Dkey=value` (chọn file khác bằng `REVENUE_CONF=...`).
`-D` truyền thêm khi gọi script được ưu tiên hơn file. Giá trị trong file là cấu hình đã dùng cho mọi kết quả công bố
(trùng mặc định Hadoop 3.4.2) và được ghi lại trong `run-manifest.json` (`configuration`). Đổi giá trị là đổi điều kiện thực nghiệm.

## Dữ liệu synthetic (kiểm soát skew/cardinality)

```bash
python3 scripts/generate_workload.py --output data/samples/optimization-200k.csv --rows 200000 --groups 32 --purchase-rate 1 --seed 21
./scripts/prepare-input.sh data/samples/optimization-200k.csv results/optimization-meta
./scripts/run-local.sh --tool DatasetTool profile --manifest results/optimization-meta/input.json --output results/optimization-meta/profile.json --dictionary results/optimization-meta/groups.json
python3 scripts/benchmark.py --matrix config/benchmark-matrix.json
```

Dữ liệu synthetic luôn ghi nhãn synthetic, không thay cho dữ liệu Kaggle. Benchmark trên dữ liệu thật dùng `config/bench/mr-*.json`
(D1, D2, D3 trên HDFS), kết quả ở `docs/evidence/bench/SUMMARY.md`.

## Chạy từng biến thể

```bash
./scripts/run-local.sh --variant v5 --manifest results/optimization-meta/input.json --preflight results/optimization-meta/preflight.json \
  --dictionary results/optimization-meta/groups.json --output results/my-v5 --group-by category_id --reducers 2 --max-keys 10000
```

- Mọi đường dẫn output/metadata phải mới (create-only); chạy lại cần thư mục mới.
- V1–V3 không cần dictionary; V4–V5 bắt buộc dictionary cùng fingerprint input, policy, group mode. Cả năm biến thể đều là **một job**.
- K > 100 000 nhóm: dùng V3 (bounded); không tự đổi thuật toán giữa benchmark.
- Benchmark: 1 warmup + 3 lần đo mỗi biến thể, thứ tự ngẫu nhiên, cùng reducers = 2, poll hoàn tất 100 ms cho mọi biến thể
  (mặc định 5000 ms của Hadoop che chênh lệch trên job nhỏ). Chi phí profile của V4/V5 đo riêng.

## Chạy trên HDFS

```bash
docker compose up -d namenode datanode
docker compose run --rm bigdata scripts/hdfs-mr.sh /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv <RUN_ID> v1 v5
```

Output MR trên `hdfs://namenode:8020/data/ecommerce/mr/<RUN_ID>/`, manifest và `revenue.csv` ở `results/mr/<RUN_ID>/`.
Cụm YARN: `scripts/run-cluster.sh` cùng CLI, cấu hình theo `config/cluster.properties` (mẫu, chưa chạy thử).

## Mã thoát và đọc kết quả

Exit 0: job và kiểm tra hợp lệ; 1: job/kiểm tra thất bại; 2: lỗi CLI/artifact/input. Không coi riêng `_SUCCESS` là đủ: input
bị đổi trong lúc chạy làm manifest `failed` dù job đã xong. Counters `MAP_OUTPUT_RECORDS`, `REDUCE_INPUT_RECORDS`,
`MAP_OUTPUT_BYTES`, shuffle/spill và thời gian từng bước nằm trong `run-manifest.json`.
