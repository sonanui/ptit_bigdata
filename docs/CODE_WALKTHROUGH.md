# Đi qua luồng code: dữ liệu vào và ra ở từng bước

Bản tóm tắt để ôn nhanh trước buổi phản biện. Bản giải thích từng dòng code kèm dữ liệu vào/ra: `docs/LUONG_CODE_CHI_TIET.md`. Mỗi bước nêu: **đầu vào → code xử lý (file:dòng) → đầu ra**,
kèm một ví dụ thật chạy xuyên suốt. Đường dẫn Java tính từ `bigdata/src/main/java/vn/edu/bigdata/revenue/`.
Số liệu lấy từ các run đã commit (`results/`, `serving/20261007-015257-b0376ef-d3/`), không có số ước lượng.

## 0. Ví dụ xuyên suốt: sản phẩm 1002099 (điện thoại Samsung)

Dòng CSV thật trong mẫu D1 (`results/samples/2019-Oct-r0.01-s21.csv`):

```
2019-10-03 00:24:38 UTC,purchase,1002099,2053013555631882655,electronics.smartphone,samsung,370.41,542074160,edf4af5a-...
```

| Bước | Dữ liệu sau bước |
|---|---|
| Parse | `ParsedEvent(event_type="purchase", product_id… category_id="2053013555631882655", price="370.41")` |
| Map (MR V1) | cặp `("2053013555631882655", (37041, 1))` — tiền tính bằng xu (minor units) |
| Reduce (D1) | `2053013555631882655,1565282.93,3387,462.14` (`results/mr/20261006-032500-88fd989-d1/revenue.csv`) |
| Reduce (D3, cả tháng) | `157049623.37, 338018, 464.62` |
| Đặc trưng A7 (D3) | views 18275, carts 0, purchases 153, median_price 370.41, distinct_users 11071 |
| Vector x (log1p, tỷ lệ) | `[9.8133, 0, 5.037, 0, 0.0084, 5.9173, 9.3122]` |
| Chuẩn hóa z = (x−mean)/std | `[3.986, -0.307, 4.232, -0.262, 0.065, 1.109, 4.056]` |
| Khoảng cách² tới 2 tâm | `[24.093, 60.737]` → **cụm 0** (cụm "hot") |
| API `POST /api/ml/kmeans/predict` | cluster 0, `matchesSpark: true` |

## 1. Đọc và làm sạch một dòng CSV (dùng chung cho MR và Spark)

- **Đầu vào:** một dòng văn bản (`Text`) do `TextInputFormat` cắt theo split của HDFS (block 128 MB; `2019-Oct.csv` = 43 block).
- `input/CsvEventParser.java`
  - `fields` (dòng 29–45): bỏ `\r`, giới hạn 64 KiB, bỏ BOM, tách theo RFC 4180 (có dấu nháy), **phải đúng 9 trường**.
  - `parse` (dòng 17–22): chỉ giữ 4 trường cần: `r[1]` event_type, `r[3]` category_id, `r[4]` category_code, `r[6]` price.
- `input/PurchasePreparation.java` `prepare` (dòng 14–42) phân loại:
  - dòng tiêu đề / sai định dạng → `HEADER` / `MALFORMED`;
  - `view`, `cart`, `remove_from_cart` → `NON_PURCHASE` (bỏ qua, có đếm);
  - loại sự kiện lạ → `UNKNOWN_EVENT`;
  - giá không hợp lệ → `INVALID_PRICE` (`domain/Money.parseMinor` dòng 9–17: `BigDecimal.setScale(2, UNNECESSARY)`, không âm, nhân 100);
  - khóa nhóm rỗng/quá 256 byte → `INVALID_GROUP` (`domain/GroupKeyResolver` dòng 6–18);
  - còn lại → `accepted(group, priceMinor)`.
- **Vì sao tiền là `long` xu:** cộng số thực `double` sai lệch tích lũy; `long` cộng chính xác, `Math.addExact` báo tràn.
- **Đầu ra:** `(group, priceMinor)` hoặc một lý do loại; mỗi lý do tăng một counter (`hadoop/shared/MapperSupport.prepare` dòng 22–30, enum `RevenueCounters`). Counters in ra cuối job là bằng chứng kiểm tra chất lượng.

## 2. Hadoop MapReduce — 5 biến thể

Lắp job: `job/JobPlanFactory.build` (dòng 29–81): `TextInputFormat`, chọn mapper/combiner/reducer theo biến thể (switch dòng 45–68), gắn file từ điển vào distributed cache (dòng 69–73), số reducer (dòng 74).
Điều phối: `cli/RevenueTool.run` (dòng 30–144): preflight lại input/output (81–85), build + chạy (86–87), kiểm tra kết quả (110–125), ghi `run-manifest.json` (127). Mã thoát 0 = thành công, 1 = lỗi chạy, 2 = sai tham số.

| Biến thể | Map phát ra | Gom ở phía map | Ghi chú |
|---|---|---|---|
| V1 direct | mỗi lượt mua 1 cặp `(category, (price,1))` — `hadoop/v1/DirectPurchaseMapper` map 22–26, emit dòng 30 | không | mốc so sánh, shuffle nhiều nhất |
| V2 combiner | như V1 | `hadoop/v2/SumCountCombiner` 11–16 cộng (sum,count) sau khi sort/spill | Hadoop **có thể** gọi combiner 0..n lần, nên phép gộp phải kết hợp + giao hoán |
| V3 in-mapper | gom trong `HashMap` có giới hạn | `hadoop/shared/BoundedAccumulator.add` 24–29, đầy thì flush | chắc chắn gộp, bộ nhớ có trần |
| V4 dense | mảng `long[]` đánh chỉ số theo từ điển category | `DenseAggregationMapper` setup 27–39 (đọc từ điển từ cache), map 41–48; `v4/DensePurchaseMapper.cleanup` 9–18 phát 1 lần/category | bỏ chi phí hash |
| V5 batch | gói nhiều category trong một bản ghi theo `id % R` | `v5/BatchPurchaseMapper.cleanup` 16–39 | `v5/BatchPartitioner` 8–12 gửi batch `i` tới reducer `i`; `v5/BatchRevenueReducer` 22–56 mở gói |

**Kiểu dữ liệu trao đổi:** `hadoop/io/SumCountWritable` ghi/đọc đúng 2 `long` (sum, count).
**Shuffle:** cặp được partition theo `hash(key) % R` (V1–V4) hoặc `id % R` (V5), sort theo key, reducer nhận `key → [(s1,c1),(s2,c2)…]`.
**Reduce:** `hadoop/shared/FinalRevenueReducer` 11–16 cộng bằng `AggregateState.merge` (dòng 22–25, `Math.addExact`) rồi `RevenueFormatter` ghi `category\tsum\tcount\tavg`.
**Vì sao không tính trung bình ở combiner:** trung bình của trung bình sai; (sum,count) là một monoid nên gộp ở đâu cũng đúng, chỉ chia `Money.average` (dòng 23–28, HALF_UP) ở cuối.

## 3. Spark (Java) — cùng bài toán, hai API

`spark/RevenueJob.java`:
- `compute` (dòng 72): `textFile` (81) → `flatMapToPair` (83) dùng lại `PurchasePreparation`, phát `(category, SumCount)` → `reduceByKey` (93) — gộp phía map trước shuffle, tương đương combiner → `collect` (95).
- `computeFrame` (dòng 116): DataFrame `groupBy().agg(sum, count)`. Physical plan: `HashAggregate (partial)` → `Exchange hashpartitioning` → `HashAggregate (final)` (plan thật trong `docs/evidence/spark-java/plans/`).

Trên Spark UI (`reports/images/spark-ui-stages-done.png`, D2): job có đúng 2 stage, ranh giới là shuffle. Stage 0 `flatMapToPair at RevenueJob.java:83` đọc 541 MiB và ghi shuffle 40,1 KiB; stage 1 `collect at RevenueJob.java:95` đọc 40,1 KiB đó. Shuffle nhỏ như vậy vì `reduceByKey` gộp ở phía map trước.

Lưu ý khi trả lời: Spark **không** chạy Hadoop Reducer. `reduceByKey`/partial aggregate là phép gộp tương đương về mặt mô hình, thực thi bằng engine của Spark (DAG, stage, shuffle file). HDFS chỉ là nơi đọc/ghi.

**Đối chiếu:** MR V1 = Spark A1 = baseline Python trên D1/D2/D3: 567/567 danh mục, 742 849 lượt mua, khớp từng xu (`analytics/revenue_parity.json`).

## 4. ETL và đặc trưng cho ML

- `spark/EventEtlJob.java`: `parsed` (115) áp schema 9 cột có chủ đích; `curated` (122) lọc `reject_reason is null`, thêm `event_date`, `event_hour`; ghi Parquet `partitionBy("event_date")` (233). Mỗi run ghi vào thư mục `run_id=…` mới (`SparkSupport.requireNew`) nên chạy lại không ghi đè.
- `spark/ProductFeaturesJob.java`:
  - `productStats` (41–56): `groupBy(product_id)` đếm views/carts/purchases, doanh thu, `countDistinct(user_id)`, `percentile_approx` giá trung vị.
  - `withFeatures` (58–68): lọc `views >= 20`, `log1p` cho các đại lượng đếm/giá, `cart_rate`, `purchase_rate`. 7 đặc trưng, khớp `notebooks/ml_common.py` `FEATURES`.

## 5. K-Means (`notebooks/kmeans_product.ipynb`)

1. Đọc Parquet đặc trưng D3: 92 592 sản phẩm.
2. `VectorAssembler` → `StandardScaler(withMean, withStd)`: mỗi đặc trưng về trung bình 0, độ lệch 1 (nếu không, `log_distinct_users` lấn át `cart_rate`).
3. Quét K = 2..10, nhiều seed; luật chọn: silhouette trung bình lớn nhất, hòa thì K nhỏ hơn (`ml/kmeans/metrics.json`).
4. K = 2, seed 1, k-means||, maxIter 50, hội tụ sau 28 vòng: silhouette **0,649**, inertia 441 696.
5. Hồ sơ cụm (`ml/kmeans/profile.csv`) — tên cụm suy ra từ thống kê, không đặt tùy ý:
   - **Cụm 0 "hot"**: 17 356 SP, TB 1 912 lượt xem, 98,5% có lượt mua.
   - **Cụm 1 "thường"**: 75 236 SP, TB 94 lượt xem, 30,9% có lượt mua.
6. Ghi `model.json` (mean, std, tâm cụm), `assignments.csv`.

Inertia = tổng khoảng cách² từ điểm tới tâm của nó (luôn giảm khi K tăng, nên không dùng một mình để chọn K). Silhouette của điểm i = (b−a)/max(a,b), a = khoảng cách TB tới điểm cùng cụm, b = tới cụm gần nhất khác.

## 6. KNN (`notebooks/knn_classifier.ipynb`)

- Câu hỏi: sản phẩm có được mua trong 7 ngày sau mốc t0 không, dựa trên 14 ngày trước t0.
- Chống rò rỉ: train t0 = 2019-10-15, test t0 = 2019-10-25; đặc trưng chỉ lấy trước t0, nhãn chỉ lấy sau t0; scaler fit trên tập train.
- KNN chính xác bằng numpy (`ml_common.knn_vote_share`), khoảng cách Euclid trên dữ liệu chuẩn hóa; chọn K và ngưỡng trên tập validation (K = 15, ngưỡng 0,4).
- Kết quả test: F1 0,621, PR-AUC 0,671; vượt 2 baseline nhưng **thua Logistic Regression** (0,637 / 0,718). Báo cáo đúng như đo.

## 7. Xuất kết quả cho web và suy luận trong backend

- `spark/ServingPublishJob.run` (138): tạo thư mục run mới (`requireNew` 155), ghi CSV/JSON (`analytics/…` 195–229, `ml/kmeans/…` 257–263, `ml/knn/…` 277–278, `ml/models.json` 322) và `manifest.json` chứa sha256 + số dòng từng file.
- Backend `serving/ServingRepository.verified` (148–157): tính lại sha256 trước khi đọc; lệch thì từ chối.
- `api/MlController` `POST /kmeans/predict` (135–140) → `ml/KMeansModel.predict` (35–41): chuẩn hóa bằng mean/std của Spark, tính khoảng cách² tới từng tâm, chọn nhỏ nhất. KNN: `KnnModel.predictStandardized` (65–88), `voteShare >= threshold`.
- Frontend: `KMeansPage.tsx` dòng 38 gọi API; Nginx `location /api/` proxy sang `web-backend:8080` (`webapp/frontend/nginx.conf` 12–13).

## 8. Demo

Kịch bản demo đầy đủ (lệnh, kết quả phải thấy, ảnh dự phòng) ở `docs/DEMO_CASES.md`.

## 9. Câu hỏi thầy dễ hỏi

- *Map phát ra gì, key là gì?* `category_id` (hoặc `category_code` tùy cấu hình), value `(sum xu, count)`.
- *Combiner có chắc được gọi không?* Không; vì vậy V3/V4 gom trong mapper để chắc chắn giảm shuffle.
- *Tại sao số reducer ảnh hưởng?* Mỗi reducer ra một file `part-r-*`; partitioner quyết định key nào về đâu.
- *Spark có phải MapReduce không?* Cùng mô hình map → shuffle → reduce, khác engine và cách thực thi (DAG, in-memory, AQE).
- *Chạy lại có bị trùng không?* Mỗi run một thư mục `run_id` mới; MR từ chối output đã tồn tại.
- *Vì sao K = 2?* Silhouette cao nhất trên nhiều seed; K lớn hơn giảm inertia nhưng tách cụm kém.
- *Có rò rỉ dữ liệu ở KNN không?* Chia theo thời gian, scaler fit trên train, nhãn sau t0.
