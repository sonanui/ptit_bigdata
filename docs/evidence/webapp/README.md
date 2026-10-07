# Bằng chứng web app (2026-10-07)

Serving run `20261007-015257-b0376ef-d3` (D3, 29 file, manifest sha256: `docs/evidence/spark-java/d3/serving-manifest-20261007-015257.json`),
đồng bộ bằng `scripts/serving-sync.ps1` (sha256 khớp 29/29).

## Backend (Spring Boot 3.5.16, JDK 21.0.12)

- `mvn -B clean package` trong `webapp/backend` với `SERVING_DIR=../../serving`: 7 test pass (`backend-clean-package.log`).
  `ServingParityTest` trên artifact thật: K-Means 92 592/92 592 sản phẩm cùng cụm với Spark; KNN 64 254/64 254 sản phẩm test
  cùng `vote_share` và nhãn với notebook; đặc trưng tính lại từ số đếm thô lệch < 1e-12.
- Chạy thật `java -Xmx512m -jar target/revenue-webapp.jar` (không HDFS cũng được), gọi bằng curl:
  `/api/health` (UP, 2 run), `/api/runs`, bảng `revenue_by_category` (567 dòng), `/api/ml/kmeans/models`, `/clusters`, `/products`,
  `POST /api/ml/kmeans/predict` theo productId (cụm khớp Spark) và theo số đếm thô (cảnh báo ngoài miền khi views < 20 hoặc carts > views),
  `POST /api/ml/knn/predict` theo productId (trả 15 láng giềng, vote_share, nhãn thật).
  Ca lỗi: thiếu `recentViews` → 422; gửi cả productId và raw → 422; mô hình không tồn tại → 404; số âm → 400.
  Khi thư mục serving rỗng: `/api/health` trả `NO_DATA`, API mô hình trả 404 (không có số giả).
- Lỗi đã sửa trong lúc xác minh: `AnalyticsController.sortKey` không compile; mô hình bị liệt kê trùng khi nhiều serving run chứa
  cùng một mô hình (`ModelRegistry.list` giờ giữ bản trong serving run mới nhất; sau sửa `/models` trả 1 mô hình mỗi loại).

## Frontend (React 18 + TypeScript + Vite 6 + ECharts)

- `npm install` + `npm run build` (`tsc -b && vite build`) đạt; bundle 1,25 MB (cảnh báo > 500 kB, chưa tách chunk).
- Kiểm tra thủ công trên Chrome (`http://localhost:8080`, backend phục vụ `frontend/dist`):
  Tổng quan (dataset d3, 42 448 765 dòng vào ETL, badge "KHỚP TUYỆT ĐỐI" 567/567, danh sách 29 artifact);
  `/ml/kmeans` (K = 2, silhouette 0,6494, hồ sơ cụm; chọn sản phẩm 1002099 → cụm 0, "Spark đã gán: 0 · khớp");
  `/ml/knn` (bảng test KNN/baseline/Logistic Regression, confusion matrix; chọn sản phẩm 1000978 → "có purchase", vote_share 1,
  nhãn thật 1, 15 láng giềng); Benchmark (11 bảng E2–E7, 11 biểu đồ, không lỗi JavaScript).
- Ảnh chụp: chụp màn hình bị timeout khi tab Chrome ở trạng thái nền (`document.visibilityState = hidden`); nội dung trang được
  xác minh qua DOM/văn bản thay cho ảnh.

## Docker image (T6.10)

- `docker compose --profile web build webapp` (multi-stage: Node 24 build React → Maven/JDK 21 build + 7 unit test, parity test tự bỏ qua
  vì image không có serving → JRE 21): thành công, log `docker-build.log`.
- Đã **tắt HDFS** (`docker compose stop namenode datanode`) rồi `docker compose --profile web up -d webapp`: `/api/health` = UP,
  `servingDir=/serving` (mount chỉ đọc), `/` và `/ml/knn` trả 200 (React do Spring Boot phục vụ). RAM container 149 MiB / giới hạn 768 MiB.

## Độ trễ API (T6.8) — `api-latency.json`

`py -3 scripts/api_latency.py` trên container: request tuần tự, 1 client cùng máy, 1 warmup + 200 lần đo mỗi endpoint.

| Endpoint | Lần gọi đầu (cold) ms | p50 ms | p95 ms |
|---|---:|---:|---:|
| `GET /api/runs` | 21 | 18,5 | 39,8 |
| `GET .../tables/revenue_by_category` | 145 | 5,9 | 30,2 |
| `GET .../tables/funnel_by_brand?q=` | 64 | 4,6 | 23,9 |
| `GET .../benchmarks/e4-spark-modes` | 16 | 4,3 | 25,2 |
| `GET /api/ml/kmeans/{run}/clusters` | 64 | 7,9 | 30,4 |
| `GET /api/ml/kmeans/{run}/products?q=` | **6 511** | 9,6 | 30,2 |
| `POST /api/ml/kmeans/predict` (productId / raw) | 194 / 53 | 8,6 / 8,6 | 29,9 / 21,7 |
| `POST /api/ml/knn/predict` (productId / raw) | **9 202** / 10 | 10,7 / 12,7 | 30,2 / 31,4 |

Tiêu chí p95 < 300 ms: **đạt** ở mọi endpoint. Hạn chế: lần gọi đầu cần nạp và kiểm sha256 `assignments.csv` (12,8 MB),
`products.csv` (11,6 MB) và `train_set.csv` (10,7 MB) nên mất 6–9 s; nên gọi trước (warm-up) trước khi demo.
KNN brute force trên 60 876 dòng train × 8 đặc trưng mất khoảng 10 ms mỗi dự đoán.

## Test tự động frontend

`npm test` (Vitest 3 + jsdom + Testing Library, `src/test/pages.test.tsx`): 7/7 test đạt. Dữ liệu giả chỉ nằm trong `src/test`
(`fakeApi.ts` thay `fetch`, ECharts thay bằng stub) và không vào bản build (`tsconfig.json` loại `src/test`). Nội dung:
định dạng số; lỗi ProblemDetail → thông báo; trang K-Means khi chưa có mô hình và khi API lỗi 503; trang KNN chọn sản phẩm →
POST đúng body → hiển thị nhãn/láng giềng; form số đếm thô gửi đủ 6 trường và hiện lỗi 422. Đã thử cố ý sửa sai 2 kỳ vọng
→ 2 test fail như dự kiến, hoàn tác → 6/6 đạt.

## Thiết kế lại giao diện (2026-10-07)

- Thanh điều hướng dọc theo thứ tự pipeline (1 Pipeline → 2 Group By → 3 MapReduce và Spark → 4 K-Means → 5 KNN), chọn serving run ở chân thanh;
  trang Pipeline mở đầu bằng kết quả đối chiếu MR/Spark và sơ đồ luồng; font Be Vietnam Pro + JetBrains Mono đóng gói cục bộ (`@fontsource`, không cần internet);
  một theme ECharts chung (`src/chart.tsx`); số trên trục và bảng theo định dạng Việt Nam; dưới 900 px thanh điều hướng chuyển thành hàng ngang.
- Lỗi phát hiện và sửa trong lúc làm: bảng Brand báo `422: Không có cột revenue` và cột doanh thu funnel trống (CSV thật là `revenue_minor`, nay đổi sang
  tiền bằng cách chia 100); cột F1 trên trang KNN bị làm tròn thành 1/0 (nhầm là cột đếm) — có test hồi quy, đã thử đưa lại lỗi thì test fail;
  `when()` làm sập trang khi ngày không hợp lệ — có test.
- Kiểm tra bằng ảnh chụp Edge headless ở 1440 px và 600 px trên dữ liệu D3 thật; image Docker build lại và phục vụ CSS mới.

## Kiểm tra bản clone mới (2026-10-07, commit `b4aa7df`)

`git clone --branch feat/nguyennd` vào thư mục trống rồi làm đúng mức 1 của `docs/HUONG_DAN_CHAY.md`:
- `serving/20261007-015257-b0376ef-d3` có trong bản clone, sha256 khớp manifest 29/29 file (thư mục `serving/**` đánh dấu `-text`).
- `docker compose --profile web build --no-cache webapp` thành công (1 phút 35 giây, base image đã có sẵn trên máy), `up -d webapp`:
  `/api/health` UP; parity 567/567; bảng Brand trả dữ liệu; dự đoán K-Means khớp cụm Spark; dự đoán KNN trả 15 láng giềng, khớp notebook;
  trang `/` hiển thị giao diện mới.
- Lỗi phát hiện nhờ kiểm tra này: luật `.gitignore` cũ `serving/` đã chặn nhầm `webapp/backend/.../webapp/serving/` (`ServingRepository`),
  nên commit `5ab0f9b` thiếu 3 file backend; đã thêm ở `b4aa7df`.
- Giới hạn: kiểm tra trên cùng máy (Docker đã có base image); chưa thử trên máy thành viên khác.

## Tách 2 container và nạp sẵn mô hình (2026-10-07)

- `docker compose --profile web build` (backend: Maven/JDK 21 → JRE 21; frontend: Node 24 `npm ci`, `npm test`, `npm run build` → Nginx 1.27):
  thành công trong 1 phút 52 giây (log `docker-build.log`). `docker compose --profile web up -d` chỉ khởi động `web-backend` (healthy qua
  `/api/health`) và `web-frontend`; HDFS/MapReduce đã gắn profile `bigdata` nên không bị bật theo.
- Qua Nginx cổng 8080: `/`, `/ml/knn`, `/groupby` trả `index.html` (200), asset không tồn tại trả 404, `index.html` có `Cache-Control: no-cache`;
  `/api/...` được chuyển sang backend. Backend thử trực tiếp ở 8081: `/api/health` 200.
- `ModelWarmup`: log "Đã nạp sẵn mô hình kmeans" 6 giây và "knn" 12 giây sau khi ứng dụng sẵn sàng. Lần gọi ML đầu tiên sau đó:
  `GET /api/ml/kmeans/{run}/products` 0,071 s, `POST /api/ml/knn/predict` 0,468 s (trước đây 6,5 s và 9,2 s).
- Sửa kèm: `ServingRepository` cache manifest ở map riêng để tránh `computeIfAbsent` lồng nhau trên cùng `ConcurrentHashMap`.
- Build toàn dự án từ `pom.xml` gốc (`mvn -B clean package`, JDK 21): `bigdata` 25 test, `webapp/backend` 7 test (parity K-Means 92 592/92 592,
  KNN 64 254/64 254) đều pass.

## Viết lại câu chữ giao diện theo góp ý nhóm trưởng (2026-10-07)

Góp ý: câu chữ trên web trừu tượng, khó hiểu. Đã sửa: bỏ thuật ngữ nội bộ khỏi giao diện (serving run, publish, artifact, manifest,
A1–A8, parity, vote_share, t0) hoặc giải thích ngay tại chỗ; đổi tên cột/chỉ số kỹ thuật sang tiếng Việt (`src/labels.ts`); mỗi chỉ số
có câu "cách đọc" (silhouette, inertia, Precision, Recall, F1, PR-AUC, accuracy); mô tả từng cụm K-Means được ghép từ số liệu của
bảng hồ sơ cụm (không đặt tên tùy ý); kết quả dự đoán thành câu ("4/15 sản phẩm giống nhất đã được mua…"); trang benchmark có câu hỏi
cho từng thí nghiệm, chú giải D1–D3, V1–V5, cách đọc biểu đồ, ghi chú kỹ thuật thu gọn; cảnh báo ngoài miền của backend viết lại.
Kiểm tra: 8 test Vitest, 4 test `InferenceTest`, build image và ảnh chụp các trang trên dữ liệu D3.

## Chưa làm

- Training API (P2), lịch sử dự đoán, tách chunk bundle JS. Danh sách đầy đủ: plan §19.
