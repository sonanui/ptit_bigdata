Tiếp tục triển khai project theo CLAUDE.md và implementation plan hiện tại.

CÓ MỘT YÊU CẦU QUAN TRỌNG CẦN BỔ SUNG:

Project không chỉ dừng ở Big Data Analytics mà còn có phần NGHIÊN CỨU HỌC MÁY với K-MEANS và KNN.

Vì vậy cần xây dựng ML pipeline tương đối hoàn chỉnh:

Big Data
→ HDFS
→ Hadoop MapReduce / Spark
→ ETL + Group By
→ Feature Engineering
→ ML Training
→ Model Evaluation
→ Model Persistence
→ Serving / Inference
→ Backend API
→ Frontend Web

Backend + Frontend vẫn là lớp ứng dụng/trình diễn, nhưng đối với ML phải hỗ trợ cả:
- xem model/training result
- xem evaluation
- gọi inference
- hiển thị kết quả ML trên Web.

Không được biến Backend thành nơi train model thay cho Spark MLlib.

==================================================
1. KIỂM TRA ML HIỆN TẠI
   ==================================================

Trước khi code, rà soát toàn bộ source hiện tại và xác định:

- K-Means đã implement đến đâu.
- KNN đã implement đến đâu.
- Feature engineering đã có chưa.
- Training pipeline đã có chưa.
- Evaluation đã có chưa.
- Model persistence đã có chưa.
- Model artifact format hiện tại là gì.
- Có sử dụng Spark MLlib hay không.
- Backend hiện tại có API nào liên quan ML.
- Frontend hiện tại có màn hình ML nào.
- Phần nào đang mock.
- Phần nào mới là scaffold.

Phân loại rõ:
DONE-VERIFIED
IMPLEMENTED-UNVERIFIED
PARTIAL
MOCK
MISSING
BLOCKED

Không được giả định KNN/K-Means đã hoàn thành chỉ vì source có class/model name tương ứng.

==================================================
2. K-MEANS PIPELINE
   ==================================================

Xây dựng K-Means theo đúng flow:

Big Data processing
→ Product/User-level features
→ Feature preprocessing/scaling nếu cần
→ K-Means training bằng Spark MLlib
→ Evaluation
→ Model persistence
→ Cluster result persistence
→ Serving artifact
→ Backend
→ Frontend

Feature phải xuất phát từ dữ liệu đã được Big Data pipeline xử lý.

Ví dụ product-level features có thể bao gồm tùy theo dữ liệu thực tế:

- số lượt view
- số lượt cart
- số lượt remove_from_cart
- số lượt purchase
- revenue
- số user khác nhau
- price
- conversion-related features

KHÔNG tự ý thêm feature nếu dataset/source hiện tại không hỗ trợ.

K cần có khả năng cấu hình.

Training phải tạo:
- model
- run_id
- feature metadata
- K
- số records
- training duration
- evaluation metrics
- cluster distribution
- model path
- training timestamp

Nếu phù hợp với implementation hiện tại:
- inertia / within-cluster cost
- silhouette score
- cluster size

Có thể hỗ trợ nhiều K để nghiên cứu lựa chọn K.

Ví dụ:

K = 2
K = 3
K = 4
K = 5

Sau đó lưu evaluation result để phục vụ báo cáo.

Không hard-code metric.

==================================================
3. KNN PIPELINE
   ==================================================

KNN phải được thiết kế đúng bản chất supervised learning.

Trước tiên kiểm tra dataset có label phù hợp hay chưa.

Nếu dataset không có label trực tiếp:
- không được tự ý giả tạo label mà không giải thích.
- cần xây dựng label có cơ sở nghiệp vụ rõ ràng từ dữ liệu nếu muốn triển khai KNN.
- phải document cách tạo label.
- phải tránh data leakage.

Ví dụ có thể nghiên cứu bài toán:

Input:
product/user/session features

Output:
một class/label được định nghĩa từ dữ liệu.

Nhưng CHỈ triển khai nếu source và dataset hiện tại hỗ trợ hợp lý.

Pipeline:

Processed Big Data
→ Feature Engineering
→ Label generation
→ Train/Test split
→ Feature scaling
→ KNN training/index
→ Evaluation
→ Model persistence
→ Inference service
→ Backend API
→ Frontend Web

Evaluation nếu phù hợp:
- accuracy
- precision
- recall
- F1
- confusion matrix

KNN phải hỗ trợ cấu hình K.

Ví dụ:

K = 3
K = 5
K = 7

So sánh kết quả để phục vụ phần nghiên cứu.

Nếu KNN không phù hợp với dataset hiện tại, KHÔNG được tạo dữ liệu giả để ép triển khai.

Trong trường hợp đó:
- ghi rõ limitation
- đề xuất label hợp lý
- chỉ triển khai phần có thể chứng minh bằng dữ liệu thật.

==================================================
4. MODEL REGISTRY / MODEL ARTIFACT
   ==================================================

Thiết kế một cơ chế quản lý model/run rõ ràng.

Ví dụ:

HDFS:

/data/ecommerce/ml/
kmeans/
run_id=...
model/
metrics.json
metadata.json
clusters.parquet

    knn/
        run_id=...
            model/
            metrics.json
            metadata.json

và serving:

/data/ecommerce/serving/<run_id>/
ml/
kmeans.json
knn.json
models.json

Không nhất thiết phải đúng tên trên nếu project hiện tại có convention tốt hơn.

Mỗi model phải có metadata tối thiểu:

- model_type
- algorithm
- run_id
- training_time
- dataset
- features
- feature preprocessing
- hyperparameters
- metrics
- model_path
- status

Không được chỉ lưu một file model mà không biết nó được train bằng dữ liệu/features nào.

==================================================
5. BACKEND ML API
   ==================================================

Backend phải đóng vai trò ML serving/inference layer.

Backend KHÔNG train model trong HTTP request.

Backend KHÔNG chạy Spark training mỗi lần user click Predict.

Training là offline/batch pipeline.

Backend load model đã được train/persist hoặc đọc serving artifact/model service phù hợp.

Thiết kế API tối thiểu:

K-MEANS:

GET /api/ml/kmeans/models
GET /api/ml/kmeans/{runId}
GET /api/ml/kmeans/{runId}/clusters

Nếu có prediction/assignment cho một product:

POST /api/ml/kmeans/predict

Request chứa feature input hoặc product identifier.

Backend trả:

- cluster
- cluster distance nếu model hỗ trợ
- model run_id
- model metadata

KNN:

GET /api/ml/knn/models
GET /api/ml/knn/{runId}
GET /api/ml/knn/{runId}/metrics

POST /api/ml/knn/predict

Request:
- feature input hoặc entity identifier

Response:
- predicted label
- confidence/probability nếu model implementation hỗ trợ
- model run_id
- model metadata

Không hard-code prediction result.

Nếu model không load được hoặc chưa có model:
API phải trả trạng thái/error rõ ràng.

==================================================
6. FRONTEND - MACHINE LEARNING WEB
   ==================================================

Frontend phải có một khu vực riêng:

Machine Learning

Có thể chia thành:

/ml/kmeans
/ml/knn

--------------------------------------------------
K-MEANS PAGE
--------------------------------------------------

Hiển thị:

1. Model information
- Model version/run_id
- Dataset
- K
- Features
- Training time

2. Evaluation
- inertia
- silhouette
- cluster count
- cluster distribution

3. Cluster visualization
- cluster size
- feature statistics
- chart nếu phù hợp

4. Product clustering

Cho phép người dùng:

- chọn product
- hoặc nhập feature values
- gọi Backend API
- Backend thực hiện inference
- hiển thị cluster được dự đoán.

Flow:

User
→ React
→ POST /api/ml/kmeans/predict
→ Backend
→ load trained model
→ inference
→ response
→ React visualization

--------------------------------------------------
KNN PAGE
--------------------------------------------------

Hiển thị:

1. Model information
2. Training dataset
3. Features
4. K
5. Accuracy
6. Precision
7. Recall
8. F1
9. Confusion matrix nếu có

Có form:

"KNN Prediction"

User nhập/chọn feature:

Feature 1
Feature 2
...
Feature N

→ Click "Predict"

Frontend gọi:

POST /api/ml/knn/predict

Backend load trained model
→ KNN inference
→ return predicted class
→ Frontend hiển thị prediction.

Nếu prediction cần chọn entity/product/user thì ưu tiên cho phép chọn entity từ dữ liệu thật thay vì bắt người dùng nhập thủ công toàn bộ feature.

==================================================
7. ML TRAINING CONTROL
   ==================================================

Không bắt buộc cho phép frontend trực tiếp trigger training ở phiên bản đầu.

Ưu tiên kiến trúc:

CLI / batch pipeline:
train K-Means
train KNN
→ save model
→ publish artifacts

Backend:
read/load trained model
→ inference

Frontend:
visualize
→ predict

Nếu project đủ ổn định mới cân nhắc API:

POST /api/ml/kmeans/train
POST /api/ml/knn/train

Nhưng nếu triển khai training API thì API phải:
- trigger asynchronous job
- không block HTTP request
- có job_id
- có training status
- có log
- có model artifact sau khi hoàn thành.

TUYỆT ĐỐI không:

POST /train
→ HTTP request giữ connection
→ Backend tự chạy Spark vài phút
→ trả response.

==================================================
8. END-TO-END DEMO FLOW
   ==================================================

Thiết kế hệ thống để khi bảo vệ có thể demo theo flow:

PHẦN 1 - BIG DATA

Kaggle CSV
→ HDFS
→ Hadoop MapReduce
→ Spark
→ Group By
→ MR/Spark parity
→ Benchmark

PHẦN 2 - MACHINE LEARNING

Spark processed data
→ Feature Engineering
→ K-Means training
→ Evaluation
→ Model persistence

và:

Spark processed data
→ Feature Engineering
→ Label
→ KNN training
→ Evaluation
→ Model persistence

PHẦN 3 - WEB

Model artifacts
→ Backend
→ React

Giảng viên có thể:

1. Xem dashboard Big Data.
2. Xem Hadoop vs Spark benchmark.
3. Xem K-Means metrics.
4. Xem các cluster.
5. Chọn một product/entity.
6. Bấm Predict Cluster.
7. Frontend gọi Backend.
8. Backend dùng model đã train.
9. Trả kết quả.
10. Xem KNN model metrics.
11. Nhập/chọn input.
12. Bấm Predict.
13. Backend inference.
14. Frontend hiển thị predicted label.

Đây phải là DEMO FLOW THẬT, không phải mock.

==================================================
9. PHÂN TÁCH TRAINING VÀ INFERENCE
   ==================================================

Đây là nguyên tắc kiến trúc bắt buộc:

OFFLINE / BATCH:

HDFS
→ Spark
→ Feature Engineering
→ Train
→ Evaluate
→ Save Model

ONLINE:

React
→ Backend API
→ Load Model
→ Predict
→ Response
→ React

Không train lại model mỗi lần người dùng gọi API.

Không chạy Group By trên raw dataset trong Backend.

Không để Frontend truy cập trực tiếp HDFS.

==================================================
10. DATABASE / PERSISTENCE
    ==================================================

Nếu project hiện tại có PostgreSQL thì chỉ dùng DB cho metadata/audit/application state nếu thực sự cần.

Có thể lưu:

ml_model_registry
- id
- run_id
- model_type
- algorithm
- dataset
- model_path
- metrics
- status
- created_at

ml_prediction_log
- id
- model_id
- request
- prediction
- created_at

Nhưng không copy toàn bộ Big Data vào PostgreSQL chỉ để phục vụ dashboard.

Big Data vẫn nằm ở HDFS/Parquet.

==================================================
11. KHÔNG ĐƯỢC GIẢ LẬP
    ==================================================

Tuyệt đối không:

- fake model metrics
- fake accuracy
- fake silhouette
- fake cluster
- fake prediction
- hard-code model output
- hard-code benchmark
- hard-code dashboard statistics

Development có thể dùng fixture để dựng UI nhưng phải đánh dấu MOCK và dễ thay thế.

Khi chạy demo cuối cùng:
mọi số liệu phải xuất phát từ pipeline/model thật.

==================================================
12. THỨ TỰ TRIỂN KHAI
    ==================================================

Ưu tiên:

P0:
HDFS
Hadoop MapReduce
Spark
Group By
Parity
Benchmark

P1:
Spark ETL
Feature Engineering
K-Means
KNN
Model evaluation
Model persistence

P1:
Serving artifacts
Backend ML API
Frontend ML dashboard
K-Means inference
KNN inference

P2:
Training job API
Model registry nâng cao
Prediction history
Visualization nâng cao

Không để phần UI hoặc Backend làm ảnh hưởng critical path của Big Data.

==================================================
13. DOCUMENTATION
    ==================================================

Sau khi hoàn thành phải cập nhật docs mô tả rõ:

1. Big Data architecture
2. ML architecture
3. Feature Engineering
4. K-Means methodology
5. KNN methodology
6. Training pipeline
7. Model evaluation
8. Model persistence
9. Backend inference architecture
10. Frontend ML functionality
11. End-to-end demo flow
12. API contract
13. Model artifact contract

Đặc biệt tài liệu phải phân biệt rõ:

Training:
Spark MLlib / offline batch

Inference:
Backend / online API

Visualization:
React Frontend

Không dùng ngôn ngữ marketing hoặc nói quá khả năng hệ thống.

Sau khi rà soát xong, hãy đưa ra implementation plan theo thứ tự ưu tiên, sau đó bắt đầu triển khai từng phần và VERIFY bằng dữ liệu/model thật.

==================================================
PHẢN HỒI CỦA CLAUDE (2026-10-06): đối chiếu với hiện trạng repository
==================================================

Đã đọc toàn bộ yêu cầu ở trên và đối chiếu với code, HDFS và evidence hiện có
(`notebooks/`, `docs/evidence/ml/README.md`, `hdfs dfs -ls -R /data/ecommerce/ml`, plan §9, §18).
Chưa sửa code ML/web nào theo yêu cầu này; chờ người dùng chốt các câu hỏi ở mục D.

A. HIỆN TRẠNG ML (mục 1 của yêu cầu)
--------------------------------------------------

| Hạng mục | Trạng thái | Bằng chứng / khoảng thiếu |
|---|---|---|
| Feature engineering sản phẩm (A7) | DONE-VERIFIED (D2) | `ProductFeaturesJob.java` → HDFS `features/product/run_id=`; chưa chạy trên D3 (cả tháng) |
| K-Means training (Spark MLlib) | DONE-VERIFIED (D2) | `notebooks/kmeans_product.ipynb`: `pyspark.ml` KMeans + StandardScaler, quét K = 2..10 × 3 seed |
| K-Means evaluation | DONE-VERIFIED (D2) | silhouette, inertia, kích thước cụm, 2 baseline, hồ sơ cụm (`kmeans-sweep.csv`, `kmeans-profile.csv`) |
| K-Means persistence | PARTIAL | HDFS chỉ có `model.json` (thứ tự feature, mean/std scaler, tâm cụm), `assignments/` Parquet, `_run.json`. **Chưa** lưu model dạng Spark (`PipelineModel.save`); metadata thiếu một số trường yêu cầu (dataset, status, model_path theo chuẩn chung) |
| K-Means inference ngoài Spark | DONE-VERIFIED | numpy từ `model.json` khớp 100% dự đoán Spark trên 25 084 sản phẩm |
| K cấu hình được | PARTIAL | K quét trong notebook; chưa có tham số chạy (env/CLI) để chạy lại với K chỉ định |
| KNN | DONE-VERIFIED **hướng B** (tìm sản phẩm tương đồng), kết quả âm một phần | `notebooks/knn_product.ipynb`: không có nhãn, đánh giá bằng category agreement@k; **không** vượt baseline "phổ biến". HDFS `ml/knn/` chỉ có `_run.json`, không có model |
| KNN phân loại (có nhãn) | MISSING | Chưa có nhãn, train/test split, accuracy/precision/recall/F1, confusion matrix |
| Serving artifacts `/data/ecommerce/serving/` | MISSING | Đã thiết kế ở plan §18.4 (D11), chưa làm |
| Backend ML API | MISSING | Repo không có backend |
| Frontend ML | MISSING | Repo không có frontend |
| Mock/scaffold ML | Không có | Không có mã mock nào; mọi số trên là từ lần chạy thật trên D2 |

B. NHỮNG ĐIỂM HỢP LÝ, SẼ LÀM THEO
--------------------------------------------------

1. Tách training offline (Spark) và inference online (backend). Không train trong HTTP request. Training API (mục 7) để P2. Trùng với plan §18 (D10, D11).
2. Không fake metric/prediction; fixture UI phải gắn nhãn MOCK. Trùng với nguyên tắc trong CLAUDE.md.
3. Metadata đầy đủ cho mỗi model; registry dạng file `models.json` trong vùng serving; layout HDFS `ml/{kmeans,knn}/run_id=.../` đã đúng convention hiện tại, chỉ cần bổ sung `model/`, `metrics.json`, `metadata.json`.
4. Bộ API `/api/ml/kmeans/*`, `/api/ml/knn/*` và hai trang `/ml/kmeans`, `/ml/knn`. Cho phép chọn sản phẩm thật thay vì nhập tay toàn bộ feature.
5. Thứ tự ưu tiên: P0 Big Data (đang làm giai đoạn 3) trước, ML/web không chen vào critical path.
6. Mô hình công bố nên train trên đặc trưng từ D3 (cả tháng), phù hợp quyết định D9 của plan. D2 lấy mẫu theo dòng làm số đếm mỗi sản phẩm nhỏ đi khoảng 10 lần.

C. NHỮNG ĐIỂM KHÔNG HỢP LÝ HOẶC CẦN SỬA LẠI CHO KHỚP NGỮ CẢNH
--------------------------------------------------

1. **"KNN training bằng Spark MLlib" không làm được:** Spark MLlib không có bộ phân loại KNN. Nó chỉ có `BucketedRandomProjectionLSH` để tìm láng giềng xấp xỉ.
   KNN là *lazy learner*: "train" chỉ là lưu tập huấn luyện đã chuẩn hóa. Việc tính khoảng cách và bỏ phiếu xảy ra lúc dự đoán.
   Đề xuất:
   - Spark (job Java hoặc notebook PySpark) tạo feature, nhãn, chia train/test, fit scaler và ghi tập huấn luyện đã chuẩn hóa thành artifact.
   - Đánh giá KNN (k = 3, 5, 7) trong notebook bằng numpy (chính xác, chia khối) trên mẫu train/test cố định seed. Có thể dùng thêm LSH của Spark để minh họa bản xấp xỉ.
   - Backend nạp artifact và tính k láng giềng lúc predict. Đây là bản chất suy luận của KNN, **không phải** backend train.
   - Báo cáo phải ghi rõ điều này, không viết là "KNN của Spark MLlib".
2. **Dùng accuracy làm chỉ số chính sẽ gây hiểu sai:** nhãn hành vi mua hàng mất cân bằng mạnh. Đoán toàn bộ là lớp "không mua" vẫn đạt accuracy cao.
   Vẫn báo cáo accuracy/precision/recall/F1/confusion matrix như yêu cầu, nhưng chỉ số chính phải là precision/recall/F1 của lớp dương, cộng PR-AUC và balanced accuracy.
   Cần 2 baseline: lớp đa số và Logistic Regression (Spark MLlib).
3. **"confidence/probability" của KNN** chỉ là tỷ lệ phiếu trong k láng giềng, không phải xác suất đã hiệu chỉnh (calibrated). API và giao diện phải ghi đúng tên `vote_share`.
4. **Feature `remove_from_cart` không có trong dữ liệu đang dùng:** tháng 10/2019 có 0 sự kiện `remove_from_cart` (`quality.json` của D2). Không đưa vào feature (đúng với chính yêu cầu "không tự ý thêm feature").
   Feature `revenue` gần như bằng purchases × price nên tương quan mạnh với feature sẵn có. Chỉ thêm nếu có lý do phân tích.
5. **PostgreSQL (mục 10):** repo không có PostgreSQL, và plan đã quyết định không thêm (D7). Theo đúng điều kiện "nếu project hiện tại có" của yêu cầu thì **không thêm**.
   Registry dùng `models.json`. Lịch sử dự đoán (P2) ghi file JSONL trong volume của backend.
6. **"Hadoop vs Spark benchmark" trên dashboard:** số đo là MapReduce LocalJobRunner và Spark `local[n]` trên **một máy**, HDFS 1 DataNode. Dashboard phải ghi rõ phạm vi này. Không được trình bày như so sánh hiệu năng cụm, cũng không kết luận chung "Spark nhanh hơn Hadoop".
7. **Nhãn KNN phải chọn trên dữ liệu có thật, không ép:** dataset không có nhãn sẵn. Phương án hợp lý nhất (chi tiết ở câu hỏi D1) là nhãn theo thời gian ở mức sản phẩm:
   - Đặc trưng lấy từ sự kiện trước mốc `t0`.
   - Nhãn = sản phẩm có ít nhất một purchase trong cửa sổ `[t0, t0+7 ngày)`.
   - Có assertion kiểm tra `max(event_time của feature) < t0`.
   - Chia train/test theo sản phẩm (hash, seed cố định). Thêm một kiểm tra theo mốc thời gian khác để đánh giá độ ổn định.
   Nhãn này cần dữ liệu cả tháng (D3), không dùng được mẫu D2 lấy theo dòng.

D. CÂU HỎI CẦN NGƯỜI DÙNG CHỐT (đã hỏi trực tiếp trong phiên làm việc)
--------------------------------------------------

1. KNN: chuyển sang phân loại có nhãn, mức sản phẩm (khuyến nghị) hay mức user? Giữ hướng B làm phần phụ hay bỏ?
2. Backend: FastAPI (Python + numpy, như D10 đã duyệt) hay Spring Boot (Java, thống nhất với lõi Java)?
3. Thứ tự: hoàn tất giai đoạn 3 (benchmark, đang chạy) rồi mới làm ML/web, hay ưu tiên ML/web ngay?
4. Training: giữ notebook (tham số qua biến môi trường, chạy bằng nbconvert) hay tách thành script Python, còn notebook chỉ để trình bày kết quả?

E. QUYẾT ĐỊNH CỦA NGƯỜI DÙNG (2026-10-06)
--------------------------------------------------

1. KNN = **phân loại mức sản phẩm**:
   - Đặc trưng lấy trước mốc `t0`; nhãn = có purchase trong `[t0, t0+7 ngày)`; dùng dữ liệu D3.
   - Giữ hướng B (tìm sản phẩm tương đồng) làm phần phụ.
2. Backend = **Spring Boot (Java)**:
   - Viết lại phép suy luận K-Means (scaler + tâm gần nhất) và KNN (chuẩn hóa + k láng giềng, bỏ phiếu) bằng Java.
   - Bắt buộc có test đối chiếu khớp với dự đoán của Spark/numpy trên artifact thật.
   - Frontend vẫn là React (theo §18).
3. Thứ tự: hoàn tất giai đoạn 3 (benchmark, D3) trước, sau đó ML (D3) rồi serving/backend/frontend.
4. Training: giữ **notebook**, thêm tham số qua biến môi trường, thực thi bằng nbconvert và lưu output.
5. Kiến trúc tổng thể đã duyệt: xem plan §7.1.
   - MR là nhánh parity/benchmark, không cấp dữ liệu cho feature.
   - Spark có lớp curated Parquet. KNN có job nhãn A8.
   - Một ứng dụng Spring Boot (project Maven riêng) đọc serving artifacts qua volume chỉ đọc.
   - Không tách Analytics API riêng; backend không đọc HDFS trực tiếp.
