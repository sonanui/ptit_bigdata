# Báo cáo và ảnh chụp

- `Bao_cao_BTL_Group_By_Aggregation_Hadoop_MapReduce_cap_nhat_v5.docx`: báo cáo cập nhật từ mẫu v4 của nhóm (v4 giữ nguyên trên máy, không commit).
  Còn phải điền: mã sinh viên, tên nhóm, lớp, giảng viên, mã học phần, bảng phân công, điểm tự đánh giá.
  Khi mở bằng Word, chọn **Yes** để cập nhật trường, cho số trang mục lục khớp với máy đang mở.
- `images/native/`: ảnh từng bước cài đặt và chạy Hadoop native trên máy (dùng trong báo cáo mục 3.2, 3.6).
- `images/`: ảnh phương án Docker và các kết quả cả tháng.
- `raw/`: output văn bản gốc của các ảnh terminal (để copy số liệu, không cần gõ lại từ ảnh).

## Ảnh cài đặt và chạy trên máy (Hadoop native) — `images/native/`

Chụp cửa sổ PowerShell thật trên laptop Windows ngày 2026-10-08 (tiêu đề cửa sổ để mặc định), theo đúng các bước ở báo cáo mục 3.2.
Script dùng: `scripts/native/*.ps1`. Giao diện web chụp từ `http://localhost:5173` (backend và frontend chạy trên máy, không Docker).

| Ảnh | Bước |
|---|---|
| `n01-tools.png` | 1. JDK 21, Hadoop 3.4.2, winutils, Python |
| `n03-setup-native.png` | 3. Sinh cấu hình native (`core-site.xml`, `hdfs-site.xml`) |
| `n04-format.png` | 4. Format NameNode |
| `n05-start-hdfs.png`, `n05b-namenode-window.png`, `n05c-datanode-window.png`, `n05d-namenode-ui.png`, `n05e-namenode-datanodes.png` | 5. Khởi động HDFS, log NameNode/DataNode, giao diện 9870 |
| `n06-ingest.png`, `n07-fsck.png`, `n07b-namenode-browse.png` | 6. Nạp 2019-Oct.csv (46 s, chạy lại SKIP), fsck 43 block |
| `n08-build.png` | 7. Build + 25 test |
| `n09-mr-fixture.png` | 8. MapReduce trên 7 dòng: 60.00/3/20.00 và 0.03/3/0.01 |
| `n10-sample.png` | 9. Preflight cả tháng + mẫu 1% (D1n) |
| `n11-mr-d1.png`, `n12-mr-counters.png` | 10. MapReduce V1–V3 trên D1n và bảng counters |
| `n13-spark-pipeline.png`, `n13b-sparkui-a1-jobs.png`, `n13c-sparkui-etl-sql.png`, `n13d-sparkui-etl-stages.png` | 11. Chuỗi Spark và Spark UI |
| `n14-parity.png` | 12. MR = Spark = baseline (313 nhóm) |
| `n15-notebooks.png` | 13. Notebook K-Means, KNN trên D1n |
| `n16-publish.png`, `n17-serving-sync.png` | 14. Xuất serving run và tải về máy |
| `n18-web-backend-build.png`, `n18b-web-frontend-build.png`, `n19-start-web.png`, `n19b-backend-window.png`, `n19c-frontend-window.png` | 15. Build và chạy web trên máy |
| `n20*`, `n21*`, `n22*` | Giao diện web (tổng quan, Group By, thực nghiệm, K-Means hot/thường, KNN) |

Kết quả chạy native (meta, run-manifest, revenue.csv, notebook có output): `results/native/` (không commit, thư mục `results/` nằm trong `.gitignore`).

## Ảnh phương án Docker và bản cả tháng — `images/`

| Ảnh | Nội dung | Nguồn |
|---|---|---|
| `50-setup-host-tools.png` | Phiên bản Docker, WSL, Git, JDK 21, winutils, Python/.venv, Node trên máy | PowerShell |
| `52-hdfs-start.png`, `53-hadoop-config.png` | Khởi động HDFS; Hadoop 3.4.2, tiến trình NameNode/DataNode, `core-site.xml`/`hdfs-site.xml` sinh từ `config/hadoop.env` | `docker compose up`, `exec namenode` |
| `54-bigdata-image.png` | JDK 11, Maven 3.9.11, Hadoop 3.4.2 trong image bigdata | `docker compose run bigdata` |
| `55-ingest.png` | Dataset gốc, nạp vào HDFS lần đầu và chạy lại (SKIP, không ghi đè) | `hdfs-ingest.sh` |
| `56-build-tests.png` | 18 + 6 test MapReduce; 25 test profile spark (chạy lại 2026-10-08) | `mvnw verify`, `spark-local.ps1 build` |
| `57-spark-setup.png` | `config/spark-local.env`, `spark-submit --version` | PowerShell |
| `59-notebook-setup.png` | `.venv`, nbconvert, môi trường và parity của notebook | `notebooks/kmeans_product.ipynb` |
| `60-serving.png`, `58-web-setup.png` | Serving run + manifest; container web và `/api/health` | `serving/`, `docker compose` |
| `01-compose-ps.png` | Container HDFS và web đang chạy | `docker compose ps` |
| `02-hdfs-report.png` | 1 DataNode live, dung lượng, replication | `hdfs dfsadmin -report`, `getconf` |
| `03-hdfs-fsck.png` | `2019-Oct.csv`: 43 block, HEALTHY | `hdfs fsck -files -blocks` |
| `04-hdfs-zones.png`, `05-hdfs-curated.png` | Các vùng raw/curated/agg/features/ml/serving; Parquet theo `event_date` | `hdfs dfs -ls`, `-du` |
| `20/21/22-namenode-*.png` | Giao diện NameNode: tổng quan, DataNode, duyệt `/data/ecommerce/raw` | http://localhost:9870 |
| `06-mr-d1.png` | MR V1, V2, V5 trên mẫu D1, input và output trên HDFS | run `20261008-report-d1` |
| `07-mr-counters.png` | Bảng counters V1/V2/V5 | `results/mr/20261008-report-d1/meta/*-run-manifest.json` |
| `08-mr-output.png` | `part-r-*` và `revenue.csv` trên HDFS | `hdfs dfs -cat` |
| `09-demo-fixture.png` | Dữ liệu 7 dòng, V1–V5 khớp kết quả tính tay | `scripts/demo.sh` |
| `10-spark-submit.png`, `11-spark-output.png` | Spark A1 trên D2, output Parquet và `_run.json` | run `20261008-002203-aa12c3c-d2` |
| `spark-ui-*.png` | Spark UI: job, 2 stage (lúc chạy và lúc xong), executor | http://localhost:4040 |
| `12-parity.png` | MR = Spark = baseline trên D1/D2/D3 | `docs/evidence/spark-java/*compare*.txt` |
| `13–16-api-*.png` | Gọi API K-Means (hot, thường, nhập tay ít lượt xem) và KNN | `POST /api/ml/.../predict` |
| `30–37*-web-*.png` | Các trang web; bản `a`/`b` là phần cắt để dán vào báo cáo | http://localhost:8080 |
| `40/41-nb-*.png` | Biểu đồ trong notebook K-Means (phân phối đặc trưng, elbow/silhouette) | `notebooks/kmeans_product.ipynb` |
| `42–45-nb-*.png` | Output văn bản của notebook K-Means/KNN | notebook đã chạy trên D3 |

Số thời gian trong các ảnh MR/Spark là **một lần chạy** để minh họa, không phải số benchmark;
số benchmark chính thức ở `docs/evidence/bench/SUMMARY.md`.
