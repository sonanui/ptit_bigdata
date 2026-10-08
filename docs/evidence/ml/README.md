# Bằng chứng giai đoạn 4 — K-Means và KNN (notebook Python, Spark MLlib local)

Notebook đã thực thi và **lưu kèm output** (vết toàn bộ tiến trình): `notebooks/kmeans_product.ipynb`, `notebooks/knn_product.ipynb`
(chạy bằng `jupyter nbconvert --to notebook --execute --inplace`, log `*-nbconvert.log`). Ngày 2026-10-06.
Môi trường: PySpark 4.0.4, JDK 21.0.12, Python 3.12.10, `local[2]`, driver 1 GB, HDFS trong Docker.
Đầu vào: đặc trưng A7 do job Java `ProductFeaturesJob` ghi lên HDFS, run `20261006-045824-24861e8-d2`
(D2 = mẫu 10% tháng 10/2019, seed 21; 125 128 sản phẩm, giữ 25 084 sản phẩm có views ≥ 20).

## K-Means sản phẩm — run `20261006-050144-24861e8-d2`

Quy tắc chọn K cố định trước khi chạy: silhouette trung bình (3 seed) cao nhất, bằng nhau thì K nhỏ hơn.

| K | Silhouette (TB 3 seed) | Độ lệch chuẩn | Inertia (TB) | Cụm nhỏ nhất |
|---:|---:|---:|---:|---:|
| 2 | **0.6811** | 0.0009 | 119 778 | 3 691 |
| 3 | 0.4879 | 0.0998 | 100 900 | 1 790 |
| 4 | 0.4587 | 0.0171 | 82 727 | 1 387 |
| 5 | 0.4968 | 0.0003 | 69 638 | 1 059 |
| 6 | 0.4118 | 0.0000 | 59 979 | 944 |
| 7–10 | 0.406–0.419 | — | 55 769 → 43 997 | 469 → 403 |

| So sánh (K = 2) | Silhouette |
|---|---:|
| K-Means (mô hình cuối, seed 1) | **0.6805** |
| Baseline 1: K-Means trên dữ liệu xáo độc lập từng cột | 0.1735 |
| Baseline 2: 2 nhóm theo phân vị giá | 0.1846 |

Hồ sơ cụm (`kmeans-profile.csv`): cụm 1 có 3 729 sản phẩm, views TB 628, cart_rate TB 0.0276, purchase_rate TB 0.0208, 96,1% có purchase;
cụm 0 có 21 355 sản phẩm, views TB 58, cart_rate TB 0.0028, purchase_rate TB 0.0086, 32,2% có purchase. Giá trung vị TB hai cụm gần nhau (228 so với 248).
Diễn giải: hai cụm tách theo **mức độ tương tác/chuyển đổi**, không theo giá hay danh mục. Đây là tương quan, không phải nhân quả.
Hạn chế: K = 2 là phân tách thô; silhouette có xu hướng ưu tiên K nhỏ; `log_views`, `log_distinct_users` tương quan mạnh nên trục "độ phổ biến" chiếm ưu thế;
D2 lấy mẫu theo dòng nên số đếm mỗi sản phẩm bị thu nhỏ khoảng 10 lần.
Suy luận ngoài Spark: `model.json` (thứ tự đặc trưng, mean/std của scaler, tâm cụm) cho kết quả numpy **khớp 100%** dự đoán Spark trên 25 084 sản phẩm.

### E7 — K-Means lặp có/không cache (`e7.csv`)

API RDD `pyspark.mllib` (vì `pyspark.ml.KMeans` tự persist), K = 2, seed 1, 20 vòng lặp cố định (epsilon = 0), 1 warmup + 3 lần đo.

| Chế độ | Fit median (min–max) ms | Thời gian nạp cache (`count`) ms |
|---|---|---|
| Không cache | 4 004 (3 967–5 153) | — |
| `cache()` + `count()` trước | 3 458 (3 417–4 203) | 4 099–4 826 |

Kết luận trong phạm vi đo: cache giảm thời gian fit khoảng 14% (median), nhưng chi phí nạp cache lớn hơn phần tiết kiệm ở 20 vòng lặp với dữ liệu nhỏ
(25 084 dòng × 7 cột, đã nằm trong page cache). Không suy rộng thành "Spark nhanh hơn Hadoop".

## KNN hướng B (sản phẩm tương đồng) — run `20261006-050518-24861e8-d2`

Euclidean trên đặc trưng A7 chuẩn hóa; 1 000 truy vấn (sản phẩm có `category_code`, seed 21); bootstrap 1 000 lần cho khoảng tin cậy 95%.

| Category agreement@k | k = 5 | k = 10 | k = 20 |
|---|---|---|---|
| KNN (mô hình) | 0.0556 [0.0472, 0.0646] | 0.0538 [0.0470, 0.0605] | 0.0530 [0.0467, 0.0597] |
| Baseline ngẫu nhiên | 0.0148 [0.0114, 0.0188] | 0.0129 [0.0108, 0.0152] | 0.0126 [0.0109, 0.0145] |
| Baseline phổ biến (top views) | 0.0760 [0.0600, 0.0940] | 0.0631 [0.0510, 0.0767] | 0.0696 [0.0553, 0.0854] |

**Kết quả âm một phần:** KNN cao hơn ngẫu nhiên khoảng 4 lần nhưng **không vượt** baseline "sản phẩm phổ biến".
Giải thích khả dĩ (chưa kiểm chứng): các sản phẩm phổ biến tập trung ở vài danh mục lớn nên trùng danh mục với nhiều truy vấn;
đặc trưng hành vi tổng hợp (lượt xem, tỷ lệ, giá) mang ít thông tin về danh mục.
Spark `BucketedRandomProjectionLSH` (bucketLength 1.0, 5 bảng băm): recall@10 so với KNN chính xác = 0.998 [0.994, 1.0] trên 50 truy vấn.

---

# D3 — cả tháng 10/2019 (2026-10-07): mô hình công bố

Notebook chạy lại với `ML_TAG=d3` (`jupyter nbconvert --execute --inplace --ExecutePreprocessor.timeout=-1`, log `kmeans-d3-nbconvert.log`,
`knn-classifier-d3-nbconvert.log`); output lưu trong chính notebook. Artifact cục bộ: `20261006-235739-b0376ef-d3/` (K-Means),
`20261007-000421-b0376ef-d3/` (KNN). Trên HDFS: `/data/ecommerce/ml/{kmeans,knn}/run_id=<run>/` gồm model Spark
(`scaler_model/`, `kmeans_model/` hoặc `preprocess_model/`), `model.json`, `metrics.json`, `metadata.json`, `_run.json` và bảng kết quả Parquet.

## K-Means sản phẩm — run `20261006-235739-b0376ef-d3`

Đầu vào: A7 run `20261006-231209-b0376ef-d3`, 92 592 sản phẩm (views ≥ 20). Cùng quy tắc chọn K như D2 (đã cố định trước).

| K | Silhouette TB 3 seed (độ lệch) | Inertia TB | Cụm nhỏ nhất |
|---:|---:|---:|---:|
| 2 | **0.6496** (0.0002) | 441 696 | 17 328 |
| 3 | 0.5027 (0.0000) | 348 895 | 5 466 |
| 4 | 0.4679 (0.0980) | 298 542 | 5 283 |
| 5 | 0.4375 (0.0869) | 260 361 | 3 090 |
| 6–10 | 0.377–0.399 | 229 900 → 164 110 | 3 070 → 447 |

| So sánh (K = 2) | Silhouette |
|---|---:|
| K-Means (mô hình cuối, seed 1; 28 vòng lặp) | **0.6494** |
| Baseline 1: xáo độc lập từng cột | 0.3161 |
| Baseline 2: 2 nhóm theo phân vị giá | 0.1832 |

Hồ sơ cụm (`kmeans-profile.csv`): cụm 0 — 17 356 sản phẩm, views TB 1 912, carts TB 53,1, purchases TB 40,5, cart_rate TB 0,0127, purchase_rate TB 0,0158, 98,5% có purchase;
cụm 1 — 75 236 sản phẩm, views TB 94, carts TB 0,06, purchases TB 0,51, cart_rate TB 0,0007, purchase_rate TB 0,0056, 30,9% có purchase.
Giá trung vị TB 215 so với 188. Giống D2: hai cụm tách theo **mức tương tác/chuyển đổi**, không theo giá; chỉ là tương quan.
Baseline xáo cột trên D3 cao hơn trên D2 (0,316 so với 0,174) nhưng vẫn thấp hơn rõ mô hình.
Suy luận numpy từ `model.json` khớp Spark **100%** trên 92 592 sản phẩm; backend Java khớp 92 592/92 592 (`docs/evidence/webapp/backend-clean-package.log`).

E7 trên D3 (RDD `pyspark.mllib`, K = 2, 20 vòng lặp cố định, 1 warmup + 3 lần đo, `e7.csv`):

| Chế độ | Fit median (min–max) ms | Nạp cache (`count`) ms |
|---|---|---|
| Không cache | 5 770 (5 753–6 954) | — |
| `cache()` + `count()` | 4 849 (4 387–5 932) | 5 701–10 298 |

Cache giảm fit khoảng 16% nhưng tính cả bước nạp cache thì vẫn không có lợi ở 20 vòng lặp và 92 592 × 7 số thực.

## KNN phân loại sản phẩm — run `20261007-000421-b0376ef-d3`

Câu hỏi: từ hành vi 14 ngày trước `t0`, sản phẩm có ≥ 1 purchase trong 7 ngày sau `t0` không? Đầu vào A8 run `20261006-233917-b0376ef-d3`.
Train = ảnh chụp `t0 = 2019-10-15`, test = ảnh chụp `t0 = 2019-10-25` (chia theo thời gian, cửa sổ nhãn không chồng nhau). Kiểm tra rò rỉ
`max(event_time lịch sử) < t0` đạt ở cả hai ảnh chụp. Train chia fit/validation 80/20 theo hash `product_id` (seed 21):
fit 48 721 (28,1% dương), validation 12 155 (27,8%), test 64 254 (25,3%). Scaler (Spark MLlib) chỉ fit trên phần fit.
KNN chính xác bằng numpy (MLlib không có KNN classifier); "mô hình" = 60 876 dòng train đã chuẩn hóa + K + ngưỡng.

Chọn trên validation (ngưỡng vote_share tối ưu F1 cho từng K):

| K | Ngưỡng | Precision | Recall | F1 | PR-AUC |
|---:|---:|---:|---:|---:|---:|
| 3 | 0.667 | 0.630 | 0.527 | 0.574 | 0.558 |
| 5 | 0.400 | 0.534 | 0.682 | 0.599 | 0.616 |
| 7 | 0.429 | 0.595 | 0.630 | 0.612 | 0.644 |
| 9 | 0.444 | 0.634 | 0.599 | 0.616 | 0.662 |
| **15** | 0.400 | 0.638 | 0.616 | **0.627** | 0.686 |

Đánh giá test (dùng một lần):

| Mô hình | Precision | Recall | F1 | PR-AUC | Balanced acc. | Accuracy |
|---|---:|---:|---:|---:|---:|---:|
| **KNN (K = 15, ngưỡng 0,40)** | 0.606 | 0.637 | **0.621** | 0.671 | 0.749 | 0.803 |
| Baseline lớp đa số | 0 | 0 | 0 | 0.253 | 0.500 | 0.747 |
| Baseline luật "đã có purchase trong lịch sử" | 0.456 | 0.768 | 0.573 | 0.409 | 0.729 | 0.709 |
| Logistic Regression (Spark MLlib, ngưỡng 0,320) | 0.606 | 0.671 | **0.637** | **0.718** | 0.762 | 0.806 |

Confusion matrix KNN: TN 41 238, FP 6 738, FN 5 903, TP 10 375. Bootstrap 1 000 lần trên test: F1 KNN [0.615, 0.627], PR-AUC KNN [0.663, 0.678],
F1 KNN − F1 luật lịch sử [0.044, 0.054].

Kết luận trong phạm vi thí nghiệm:
- KNN vượt cả hai baseline đơn giản, khoảng tin cậy của chênh lệch F1 so với luật lịch sử không chứa 0.
- KNN **thua Logistic Regression** ở F1 (0,621 so với 0,637) và PR-AUC (0,671 so với 0,718). Báo cáo đúng như vậy.
- Accuracy của baseline lớp đa số là 0,747 dù không bắt được sản phẩm dương nào: minh họa vì sao không dùng accuracy làm chỉ số chính.
- K tốt nhất (15) nằm ở **biên** lưới K đã cố định trước (3, 5, 7, 9, 15); K lớn hơn có thể tốt hơn trên validation. Chưa thử để giữ quy tắc đã định.
- `vote_share` là tỷ lệ phiếu trong 15 láng giềng, không phải xác suất đã hiệu chỉnh.
- Dự đoán 64 254 sản phẩm test mất 404 918 ms (numpy, brute force); suy luận từ artifact (đọc lại `train_set` từ HDFS + `model.json`)
  khớp đánh giá 100%; backend Java khớp vote_share và nhãn 64 254/64 254.
