# Spark local (Java) và notebook ML

Phân bổ thành phần: HDFS và Hadoop MapReduce chạy trong Docker (`docs/docker.md`); Spark và notebook chạy **local trên máy**.
Bảng đầy đủ ở `docs/PROJECT_AUDIT_AND_IMPLEMENTATION_PLAN.md` §7.6.

| Phần | Ngôn ngữ / vị trí |
|---|---|
| ETL raw → curated, Group By A1–A5, đặc trưng A7, nhãn A8, publish serving | Java, module `bigdata`, `bigdata/src/main/java/vn/edu/bigdata/revenue/spark/` (điểm chạy `SparkTool`) |
| K-Means, KNN | Notebook Python, `notebooks/kmeans_product.ipynb`, `notebooks/knn_classifier.ipynb` (KNN hướng B cũ trên D2: `knn_product.ipynb`) |

## Cài đặt một lần (Windows, PowerShell)

1. JDK **17 hoặc 21** (Spark 4.0 không chạy trên JDK 23+). Không cần đổi `JAVA_HOME` toàn máy.
2. Hadoop cho Windows có `bin\winutils.exe` và `bin\hadoop.dll` (bản 3.x), đặt ở thư mục bất kỳ.
3. Python 3.12 và môi trường ảo trong repo:

   ```powershell
   py -3.12 -m venv .venv
   .venv\Scripts\pip install -r notebooks\requirements.txt
   ```

   `pyspark==4.0.4` cung cấp `spark-submit`; phiên bản phải trùng `spark.version` trong `pom.xml` (profile `spark`).
4. Sao chép `config\spark-local.env.example` thành `config\spark-local.env` (không commit) và sửa `JAVA_HOME`, `HADOOP_HOME`.
   Giá trị trong file này ghi đè biến môi trường của máy.
5. Bật HDFS: `docker compose up -d namenode datanode` (cổng 8020, 9866 publish trên `127.0.0.1`).
   Trên máy RAM 8 GB nên thêm `autoMemoryReclaim=dropCache` vào mục `[experimental]` của `~/.wslconfig` để VM Docker trả lại RAM.

## Build và test

```powershell
.\scripts\spark-local.ps1 build              # mvn -pl bigdata -Pspark package: 18 test MR + 7 test Spark, ra bigdata\target\revenue-aggregation-spark.jar
.\scripts\spark-local.ps1 build -DskipTests  # chỉ build
```

Maven 3.9.11 được `mvnw` tải vào `.tools\` ở lần đầu (cần Internet). Build mặc định cho MR (Java 11) vẫn chạy trong Docker và
không compile package `spark`.

## Chạy pipeline Spark (Java)

```powershell
.\scripts\spark-pipeline.ps1 -InputPath /data/ecommerce/raw/sample/2019-Oct-r0.1-s21.csv -Tag d2 -Duplicates
```

Chuỗi: `revenue` (A1, RDD `reduceByKey`) → `etl` (curated Parquet + `quality.json`) → `metrics` (A2–A5, kiểm tra A1 = A2) →
`features` (A7) → `labels` (A8, nhãn KNN theo mốc t0). Bước `publish` chạy riêng sau notebook (xem `docs/END_TO_END.md`). `run_id` ghi ở `docs\evidence\spark-java\<tag>-run-ids.tsv`; kết quả nhỏ ở `results\spark\<run_id>\`; output đầy đủ trên HDFS.
Mọi output phải mới (`errorifexists`); chạy lại sẽ tạo `run_id` mới. Chạy riêng một job:

```powershell
.\scripts\spark-local.ps1 submit revenue --input /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv --tag d1
```

Đối chiếu A1 với MR và baseline độc lập:

```powershell
py -3 scripts\compare_revenue.py results\mr\<MR_RUN>\revenue.csv results\spark\<A1_RUN>\revenue.csv
py -3 scripts\baseline_revenue.py <file CSV cục bộ> results\baseline\<tên>.csv
```

## Chạy notebook ML

Notebook đọc `run_id` đặc trưng/nhãn từ `docs\evidence\spark-java\<TAG>-run-ids.tsv` (biến môi trường `ML_TAG`, mặc định `d3`;
các tham số khác xem `docs/ML.md`). Thực thi và lưu kèm output:

```powershell
cd notebooks
..\.venv\Scripts\jupyter-nbconvert.exe --to notebook --execute --inplace --ExecutePreprocessor.timeout=7200 kmeans_product.ipynb
..\.venv\Scripts\jupyter-nbconvert.exe --to notebook --execute --inplace --ExecutePreprocessor.timeout=7200 knn_classifier.ipynb
```

Hoặc mở bằng Jupyter/VS Code với kernel của `.venv`. Kết quả: `results\ml\<run_id>\` và HDFS `/data/ecommerce/ml/{kmeans,knn}/run_id=...`.

## Linux/macOS

`bash scripts/spark-local.sh build|submit ...` (đọc cùng `config/spark-local.env`, không cần `HADOOP_HOME`). Chưa được kiểm chứng trên máy nhóm.

## Lỗi đã gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `getSubject is not supported` | JDK 23+ | Đặt `JAVA_HOME` JDK 17/21 trong `config\spark-local.env` |
| `Python was not found` khi `spark-submit` | `python` là alias Microsoft Store | Launcher tự đặt `SPARK_HOME` vào `.venv` |
| `Permission denied: user=<tên Windows>` khi ghi HDFS | Cây `/data` thuộc user `hadoop` | `HADOOP_USER_NAME=hadoop` trong `config\spark-local.env` |
| Git Bash đổi `/data/...` thành `C:/Program Files/Git/data/...` | Chuyển đổi đường dẫn của MSYS | Dùng PowerShell (`*.ps1`) |
| File ghi từ client có replication 3, block thiếu bản sao | Client không đọc `hdfs-site.xml` của cụm | Job đặt `dfs.replication=1` |
