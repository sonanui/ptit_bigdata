# Kịch bản demo trước thầy

Demo chạy trên laptop Windows, **Hadoop native** (không Docker), đúng theo các bước cài đặt ở báo cáo mục 3.2.
Thứ tự đi theo luồng dữ liệu: **HDFS → MapReduce → Spark → đối chiếu → ML → web**. Mỗi ca ghi: thao tác, kết quả phải
thấy (số thật đã chạy ngày 08/10/2026), điểm cần nói, câu hỏi dễ gặp. Ảnh dự phòng ở `reports/images/native/`; nếu
máy gặp sự cố khi demo thì mở ảnh. Giải thích code từng dòng: `docs/LUONG_CODE_CHI_TIET.md`.

Tổng thời lượng khoảng 20 phút; các ca có dấu (*) có thể bỏ nếu thầy cần rút ngắn.

## Chuẩn bị

**Trước buổi demo 1 ngày**

- Chạy hết một lần từ ca 1 tới ca 9 trên máy demo. Mọi job đã chạy thì kết quả còn trên HDFS, hôm demo chỉ cần mở lại.
- Build sẵn: `.\scripts\spark-local.ps1 build`, `mvn` backend, `npm run build` frontend (báo cáo bước 7 và 15). Hôm demo không build.
- Đóng các chương trình nặng khác; máy chỉ có 8 GB RAM.

**Trước giờ demo 10 phút** (PowerShell, tại thư mục repo)

```powershell
docker compose stop                       # nhường cổng 8020/9870/8080 nếu Docker đang chạy
.\scripts\native\start-hdfs.ps1           # NameNode + DataNode, chờ "Safe mode is OFF"
.\scripts\native\start-web.ps1            # backend :8080 + frontend :5173
. .\scripts\native\hadoop-env.ps1         # để gõ lệnh hdfs trong cửa sổ demo
```

Mở sẵn các tab trình duyệt: `http://localhost:9870`, `http://localhost:5173`, và file
`bigdata/src/main/java/vn/edu/bigdata/revenue/hadoop/v1/DirectPurchaseMapper.java` trong IntelliJ.

---

## Ca 1: Môi trường và cụm HDFS (2 phút)

| Thao tác | Phải thấy | Ảnh |
|---|---|---|
| `java -version`, `hadoop version` | JDK 21.0.12, Hadoop 3.4.2 | `n01-tools.png` |
| `jps` | `NameNode`, `DataNode` | `n05-start-hdfs.png` |
| `hdfs dfsadmin -report` | Live datanodes (1), Safe mode is OFF | `n05-start-hdfs.png` |
| Tab `localhost:9870` → Overview, Datanodes | 1 DataNode live, dung lượng ổ E | `n05d-namenode-ui.png` |
| Mở `E:\Library\hadoop-native\conf\hdfs-site.xml` | replication 1, blocksize 134217728 | `n03-setup-native.png` |

**Điểm cần nói:** đây là Hadoop chạy trực tiếp trên máy, chế độ giả phân tán: NameNode giữ metadata (tệp gồm những block
nào, block ở đâu), DataNode giữ block thật trên đĩa. Một DataNode nên replication = 1. MapReduce chạy bằng
LocalJobRunner (`mapreduce.framework.name = local`), không có YARN.

**Câu hỏi dễ gặp:** *Vì sao replication = 1?* Chỉ có một DataNode; đặt 3 thì mọi block sẽ ở trạng thái thiếu bản sao.
*Không có YARN thì còn là Hadoop không?* HDFS và mô hình MapReduce (Mapper, Combiner, Partitioner, Reducer, shuffle, sort)
là của Hadoop; LocalJobRunner chỉ thay việc lập lịch task trên nhiều máy bằng chạy trong một JVM.

## Ca 2: Dữ liệu trên HDFS (2 phút)

| Thao tác | Phải thấy | Ảnh |
|---|---|---|
| Tab `localhost:9870` → Utilities → Browse → `/data/ecommerce/raw` | `2019-Oct.csv` 5,28 GB, block 128 MB, replication 1 | `n07b-namenode-browse.png` |
| `hdfs fsck /data/ecommerce/raw/2019-Oct.csv -files -blocks` | 43 block, block cuối 31 468 279 byte, HEALTHY | `n07-fsck.png` |
| `hdfs dfs -head /data/ecommerce/raw/2019-Oct.csv` | header 9 cột + vài dòng | `n07-fsck.png` |
| `.\scripts\native\ingest.ps1 2019-Oct.csv` | `SKIP … đã tồn tại`, không ghi đè | `n06-ingest.png` |

**Điểm cần nói:** 5 668 612 855 / 134 217 728 = 42,2, nên có 42 block đầy và 1 block 31 MB. Mỗi block thành một
split, tức một map task, nên cả tháng có 43 map task. Lần nạp đầu mất 46 giây.

## Ca 3: Tính tay được, MapReduce trên 7 dòng (3 phút), ca quan trọng nhất

```powershell
hdfs dfs -cat /data/ecommerce/fixtures/events.csv
.\scripts\native\mr-native.ps1 /data/ecommerce/fixtures/events.csv demo-<giờ> v1 v2 v3
hdfs dfs -cat /data/ecommerce/mr/demo-<giờ>/v1/part-r-00000
hdfs dfs -cat /data/ecommerce/mr/demo-<giờ>/v1/part-r-00001
```

- Phải thấy: `Run valid` ×3, `Results equal` ×2; `part-r-00000` = `1  60.00  3  20.00`, `part-r-00001` = `2  0.03  3  0.01`.
- Ảnh: `n09-mr-fixture.png`.
- **Mở `DirectPurchaseMapper.java` dòng 22–31** và nói theo bảng:

| Dòng CSV | `map` phát ra |
|---|---|
| purchase, cat 1, 10.00 | `("1", (1000, 1))` |
| purchase, cat 1, 20.00 | `("1", (2000, 1))` |
| purchase, cat 1, 30.00 | `("1", (3000, 1))` |
| **view**, cat 1, 99.00 | không phát (`PurchasePreparation` dòng 18–24) |
| purchase, cat 2, 0.00 / 0.01 / 0.02 | `("2", (0,1))`, `("2", (1,1))`, `("2", (2,1))` |

- Shuffle: `hash("1") = 80`, 80 mod 2 = 0, vào reducer 0; `hash("2") = 81`, mod 2 = 1, vào reducer 1. Đúng hai tệp `part-r` ở trên.
- Reduce (`FinalRevenueReducer` dòng 13–15): cộng `(1000+2000+3000, 3) = (6000, 3)`, sau đó định dạng `60.00  3  20.00`.

**Câu hỏi dễ gặp:** *Vì sao không cộng `double`?* 0,01 + 0,02 trong số thực nhị phân không ra đúng 0,03; nhóm dùng `long`
theo xu (`Money.parseMinor`). *Vì sao không tính trung bình ở combiner?* Trung bình của các trung bình sai khi số phần tử
khác nhau; chỉ cộng `(sum, count)` và chia ở cuối.

## Ca 4: MapReduce trên mẫu 1% và đọc counters (3 phút)

```powershell
py -3 scripts\mr_counters.py results\native\native-d1\meta
```

Job đã chạy sẵn. Nếu thầy muốn xem chạy thật, chạy lại với RUN_ID mới, mất khoảng 1,5 phút:
`.\scripts\native\mr-native.ps1 /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv demo-d1 v1 v2 v3`.

| Counter | V1 | V2 | V3 |
|---|---:|---:|---:|
| Map input records | 423 160 | 423 160 | 423 160 |
| Map output records | 7 505 | 7 505 | 313 |
| Combine output records | 0 | 313 | 0 |
| Reduce shuffle bytes | 285 202 | 11 906 | 11 906 |
| Reduce input records | 7 505 | 313 | 313 |
| Reduce output records | 313 | 313 | 313 |

Ảnh: `n11-mr-d1.png`, `n12-mr-counters.png`.

**Điểm cần nói:**

- 423 160 = 1 header + 415 654 không phải purchase + 7 505 purchase. Không dòng nào bị mất, và chương trình tự kiểm tra
  đẳng thức này (`RevenueTool` dòng 119).
- V2 **không** giảm `Map output records` mà giảm dữ liệu qua shuffle khoảng 24 lần.
- V3 gộp ngay trong mapper nên chỉ phát 313 bản ghi.
- Cả ba ra cùng 313 danh mục.

**Câu hỏi dễ gặp:** *Combiner có chắc được gọi không?* Không; Hadoop có thể gọi 0, 1 hay nhiều lần, vì vậy V3 gộp trong
mapper để chắc chắn. *Còn V4, V5?* V4/V5 dùng distributed cache. Trên Windows, LocalJobRunner cần quyền tạo symbolic link
(lỗi 1314 nếu chưa bật Developer Mode), nên nhóm chạy và đo V4/V5 trong Docker (báo cáo mục 3.4; trên cả tháng V5 nhanh
hơn V1 khoảng 3,1 lần).

## Ca 5: Spark cùng bài toán, xem Spark UI (3 phút)

```powershell
.\scripts\spark-local.ps1 submit revenue --input /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv --tag demo
# trong lúc chạy (khoảng 30 giây): mở http://localhost:4040 → Jobs, Stages
```

- Phải thấy: `A1 xong: … groups=313 valid=7505`. Trên Spark UI có job `collect at RevenueJob.java:95` gồm 2 stage
  (`flatMapToPair at RevenueJob.java:83`, rồi sau shuffle là `collect`). Ảnh: `n13-spark-pipeline.png`, `n13b-sparkui-a1-jobs.png`.
- Mở `RevenueJob.java` dòng 81–95: `flatMapToPair` là hàm Map (dùng lại đúng `PurchasePreparation` của MapReduce).
  `reduceByKey` gộp trong partition trước, giống combiner, rồi shuffle, rồi gộp cuối.

**Câu hỏi dễ gặp:** *Spark có dùng Reducer của Hadoop không?* Không. Cùng mô hình map → shuffle → reduce nhưng Spark tự
thực thi (DAG, stage, file shuffle), chỉ dùng HDFS để đọc và ghi.

## Ca 6: Hai engine ra cùng kết quả (1 phút)

```powershell
py -3 scripts\compare_revenue.py results\native\native-d1\revenue.csv results\spark\20261008-023345-aa12c3c-n1\revenue.csv
py -3 scripts\compare_revenue.py results\native\native-d1\revenue.csv results\native\native-d1\baseline-revenue.csv
```

- Phải thấy: `KHỚP: 313 nhóm, tổng purchase_count=7505` hai lần. Ảnh: `n14-parity.png`.
- Web, trang Tổng quan: "313/313 danh mục có kết quả trùng khớp" (`n20-web-overview.png`). Trên cả tháng: 567/567.

## Ca 7 (*): Làm sạch dữ liệu và Parquet (1 phút)

- `hdfs dfs -ls /data/ecommerce/curated/events/run_id=20261008-023408-aa12c3c-n1` cho thấy các thư mục `event_date=2019-10-01` …
- ETL: 423 160 dòng vào, 423 159 hợp lệ, 1 header bị loại.
- Điểm cần nói: Parquet lưu theo cột, chia theo ngày. Group By cả tháng trên Parquet mất 18 giây so với 227 giây trên CSV (báo cáo Bảng 15).

## Ca 8: K-Means, sản phẩm "bán chạy" và "bình thường" (4 phút)

Web `http://localhost:5173/ml/kmeans`; ô "Bộ kết quả đang xem" đang chọn run chạy lại trên máy (n1).

| Thao tác | Phải thấy | Ảnh |
|---|---|---|
| Phần "Vì sao chọn K = 2?" | silhouette cao nhất ở K = 2 (0,691) | `n21-web-kmeans-clusters.png` |
| Phần "Các cụm khác nhau thế nào?" | Cụm 0: bình thường, 2 688 SP, TB 40 lượt xem, 35,2% có lượt mua. Cụm 1: bán chạy (hot), 474 SP, TB 273 lượt xem, 97,7% có lượt mua | `n21-web-kmeans-clusters.png` |
| Link `/ml/kmeans?product=1004767` | Cụm 1, nhóm bán chạy, trùng Spark | `n21b-web-kmeans-hot.png` |
| Link `/ml/kmeans?product=1002099` | Cụm 0, nhóm bình thường, trùng Spark | `n21c-web-kmeans-thuong.png` |
| Ở ô "Mô hình" chọn mô hình cả tháng `20261006-235739-b0376ef-d3`, tìm `1002099`, bấm "Chọn" | Cụm 0, nhưng lần này là **nhóm bán chạy** (18 275 lượt xem, 153 lượt mua) | `35b-web-kmeans-predict-hot.png` |

**Điểm cần nói:**

- Mỗi sản phẩm là 7 đặc trưng (log lượt xem, giỏ, mua, tỷ lệ, giá, số người xem), được chuẩn hóa `z = (x − mean)/std`
  rồi gán vào tâm gần nhất.
- Tên "bán chạy / bình thường" do nhóm đặt **sau** khi xem bảng số liệu; K-Means chỉ trả số 0/1. Bằng chứng: trên mẫu 1%
  cụm bán chạy là cụm 1, trên cả tháng là cụm 0, nhưng web vẫn gọi đúng vì tên suy ra từ số liệu.
- Cùng sản phẩm 1002099: trên cả tháng thuộc nhóm bán chạy, trên mẫu 1% (187 lượt xem, 0 lượt mua) thuộc nhóm bình
  thường. Cụm phụ thuộc dữ liệu huấn luyện.
- Tính tay ví dụ cả tháng: z = [3,986; …], khoảng cách² tới hai tâm [24,09; 60,74], nên ra cụm 0 (`docs/LUONG_CODE_CHI_TIET.md` §14).

**Câu hỏi dễ gặp:** *Vì sao K = 2?* Luật chọn cố định trước: silhouette trung bình cao nhất qua 3 seed; inertia luôn giảm
khi K tăng nên không dùng một mình. *Silhouette là gì?* Với mỗi điểm, (b − a)/max(a, b), trong đó a là khoảng cách trung
bình tới điểm cùng cụm, b là tới cụm gần nhất khác. Càng gần 1 thì cụm càng tách bạch. *Có chắc không phải ngẫu nhiên?*
Hai baseline (xáo cột, chia theo giá) có silhouette 0,316 và 0,183 trên cả tháng, thấp hơn rõ 0,649.

## Ca 9: KNN (2 phút)

- Link `/ml/knn?product=1004767`: 15/15 láng giềng đã được mua, nên đoán "có". Thực tế cũng có; khớp notebook. Ảnh `n22b-web-knn-predict.png`.
- Bảng đánh giá (`n22-web-knn-metrics.png`): trên mẫu 1%, KNN F1 0,527 > luật lịch sử 0,507 nhưng < Logistic Regression
  0,570. Trên cả tháng: 0,621 so với 0,572 và 0,637. **Nói thẳng: KNN không phải mô hình tốt nhất.**

**Câu hỏi dễ gặp:** *Láng giềng gần nhất của 1004767 lại là chính nó?* Đó là cùng sản phẩm nhưng ở ảnh chụp huấn luyện
(t0 = 15/10, đặc trưng 1–14/10), còn câu hỏi là ảnh chụp kiểm tra (t0 = 25/10, đặc trưng 11–24/10). Hai ảnh chụp có đặc
trưng khác nhau (khoảng cách 0,55, không phải 0), và nhãn của ảnh chụp huấn luyện thuộc 15–21/10, trước t0 kiểm tra. Vì
vậy không rò rỉ nhãn của tập kiểm tra. *Vì sao không dùng accuracy?* 74,7% sản phẩm (cả tháng) không được mua, nên đoán
"không" hết đã được 74,7%; nhóm dùng F1 và PR-AUC.

## Ca 10 (*): Hiệu năng (1 phút, khi được hỏi)

Bảng E2–E7 trong `docs/evidence/bench/SUMMARY.md` và báo cáo mục 3.4 (đo trong Docker). Cả tháng: MR V5 61 giây so với
V1 190 giây; Spark DataFrame trên Parquet 18 giây. Luôn kèm câu: *đo trên một laptop; không kết luận engine nào nhanh hơn
nói chung*.

## Kết thúc

```powershell
.\scripts\native\stop-web.ps1
.\scripts\native\stop-hdfs.ps1
```

## Khi có sự cố trong lúc demo

| Sự cố | Xử lý nhanh |
|---|---|
| `start-hdfs.ps1` báo cổng 8020/9870 bận | `docker compose stop` (HDFS Docker còn chạy) rồi chạy lại |
| Web trắng hoặc lỗi 502 | Backend chưa lên: mở cửa sổ backend trên taskbar xem log; chạy lại `stop-web.ps1` rồi `start-web.ps1` |
| Spark UI không mở | UI chỉ sống khi job đang chạy; dùng ảnh `n13b-sparkui-a1-jobs.png` |
| Máy chậm, treo | Dùng ảnh trong `reports/images/native/` theo đúng thứ tự ca |
