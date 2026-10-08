# Các thuật toán Group By Aggregation và độ phức tạp

Mục 1–9: Hadoop MapReduce V1–V5 (module `bigdata`, package `hadoop/`). Mục 10: Spark (RDD, DataFrame) cho cùng bài toán và phân biệt với MapReduce.

## 1. Bài toán chung

Từ CSV bán hàng, lọc các sự kiện `event_type=purchase`, phân nhóm theo danh mục rồi tính:

```text
SUM(group) = tổng price của purchase thuộc group
COUNT(group) = số purchase hợp lệ thuộc group
AVG(group) = SUM(group) / COUNT(group)
```

Dataset hiện tại không có trường chi nhánh; không biến `brand` thành chi nhánh. Mặc định nhóm theo `category_id`; có thể chọn `category_code` hoặc `category_root`. Một dòng purchase được coi là một đơn vị ghi nhận; không suy diễn quantity hay giá trị cả đơn hàng.

Cả V1–V5 dùng cùng input, policy lọc, cách chọn group, số reducer và định dạng output. Mỗi phiên bản chạy **một job Hadoop MapReduce**. Chúng thay đổi cách tổ chức tính toán, không thay đổi bài toán.

### Trạng thái tổng hợp và tính đúng

Mọi partial giữ `(sum_minor, count)`:

```text
merge((s1,c1), (s2,c2)) = (s1+s2, c1+c2)
```

- Tiền được biểu diễn bằng số nguyên `long`, scale 2; ví dụ `12.34 → 1234`.
- Cộng bằng phép toán kiểm tra overflow; overflow làm run thất bại.
- Không tính trung bình tại mapper/combiner. Chỉ chia và làm tròn `HALF_UP` tại output cuối.
- Ví dụ partial `(10000,100)` và `(3000,10)` phải cho AVG `13000/110`, không phải trung bình của hai AVG partial.
- Tổng hợp có thể chia/ghép theo bất kỳ mapper/reducer nào mà giữ cùng SUM/COUNT, miễn không mất hoặc nhân đôi purchase.
- Parser và `PurchasePreparation` giữ cùng semantics giữa các variant; input sai cấu trúc không được đánh dấu run valid.

## 2. Ký hiệu và cách đọc độ phức tạp

| Ký hiệu | Ý nghĩa |
|---|---|
| `N` | Tổng số dòng sự kiện cần đọc |
| `D` | Tổng số byte input |
| `P` | Số purchase hợp lệ, `P ≤ N` |
| `K` | Số group khác nhau trong purchase hợp lệ |
| `M` | Số mapper/input splits |
| `R` | Số reducer, `R ≥ 1` |
| `B` | Giới hạn số key trong cache V3 |
| `K_m` | Số group thực sự xuất hiện trong mapper m |
| `G` | Tổng partial theo group qua các mapper: `Σ K_m ≤ min(P,MK)` |
| `E` | Tổng records V3 emit sau các lần flush, `G ≤ E ≤ P` |
| `Q` | Tổng batch records V5, `Q ≤ M·min(R,K)` và `Q ≤ G` |

`O(N)` giả định độ dài dòng/key có giới hạn như input contract. Nếu xét độ dài biến thiên, phần scan/parse/hash theo bytes là `O(D)`, không phải mỗi dòng luôn tốn đúng một thao tác.

Trong tài liệu này, sort `T` records được mô hình hóa bằng cận trên `O(T log(T+1))`. Hadoop thực tế sort theo từng mapper, spill, shuffle và merge; đây là mô hình tổng công việc để so các phương án, **không phải công thức chính xác của wall time**. Với nhiều mapper, riêng local sort có thể viết chặt hơn bằng tổng chi phí từng split.

HashMap/dictionary lookup được tính **kỳ vọng O(1)** khi độ dài key có giới hạn; không tuyên bố worst-case O(1). Bộ nhớ bên dưới là phần thuật toán quản lý, chưa gồm Hadoop sort buffer, JVM, parser buffer và network buffer.

Không thuật toán nào bỏ được việc đọc input: cận dưới là `Ω(N)` hoặc `Ω(D)` theo bytes. Tối ưu nhằm giảm records cần sort/shuffle, serialize và allocation; không hứa mọi phiên bản giảm một bậc Big-O độc lập.

## 3. V1 — Direct MapReduce: baseline

### Luồng xử lý

```text
Map(line):
    purchase = parse_and_prepare(line)
    nếu hợp lệ:
        emit(group, (price_minor, 1))

Shuffle:
    partition theo group
    sort/group các records cùng key

Reduce(group, values):
    sum = 0; count = 0
    với mỗi value: cộng sum và count
    emit(group, format(sum,count,sum/count))
```

Ví dụ ba purchases `A:10`, `A:20`, `B:5` làm mapper emit ba records: `A:(1000,1)`, `A:(2000,1)`, `B:(500,1)`.

### Chi phí

- Map scan và prepare: `O(N)`.
- Map output: đúng `P` records.
- Sort/merge model: `O(P log(P+1))`.
- Reduce merge: `O(P)`; output tối đa `K` rows.
- Tổng model: **`O(N + P log(P+1))`**.
- Bộ nhớ mapper: `O(1)`; reducer: `O(1)` vì merge streaming, không giữ toàn bộ values.
- Shuffle records tới reducer: `P`, nếu bỏ qua retry; bytes tỷ lệ với key/value kích thước thực tế.

### Ưu/nhược điểm và source

Dễ hiểu, ít state, thích hợp làm reference để kiểm tra các tối ưu. Bất lợi khi nhiều purchases trùng group: vẫn serialize/sort/shuffle từng purchase.

Source: `hadoop/v1/DirectPurchaseMapper.java`, `hadoop/shared/FinalRevenueReducer.java`, `hadoop/io/SumCountWritable.java`. Mapper dùng `MapperSupport.prepare`; reducer dùng `RevenueFormatter` và `Money`.

## 4. V2 — Hadoop Combiner

### Luồng xử lý

Mapper giữ nguyên V1. Framework có thể chạy combiner trên các records cùng group trong spill/merge:

```text
Combine(group, partials):
    emit(group, merge_tất_cả_sum_count(partials))

Reduce(group, partials):
    merge rồi format như V1
```

Ví dụ `A:(1000,1)` và `A:(2000,1)` có thể được gộp thành `A:(3000,2)` trước khi đi tới reducer.

### Chi phí

- Mapper vẫn emit `P` records; `MAP_OUTPUT_RECORDS` không giảm so V1.
- Map-side collection/sort vẫn xử lý records ban đầu; combiner không loại bỏ chi phí này.
- Gọi `C` là số partial thực tế được shuffle tới reducer. Trong run thành công không tính retry, `C ≤ P`; nếu không gộp được thì `C=P`.
- Reduce merge `O(C)`; shuffle payload giảm khi group lặp lại nhiều.
- **Worst-case vẫn `O(N + P log(P+1))`**. Combiner thêm thao tác merge nhưng có thể tiết kiệm nhiều I/O.
- Bộ nhớ state combiner/reducer: `O(1)` theo group, merge streaming.

Hadoop không bảo đảm combiner chạy đúng một lần, hay chạy ít nhất một lần. Không được coi `C=MK` là bảo đảm. Thuật toán phải đúng khi combiner chạy 0, 1 hoặc nhiều lần.

### Khi có lợi và source

Có lợi khi records cùng group xuất hiện nhiều và shuffle là chi phí đáng kể. Với key gần như unique, có thể không tiết kiệm hoặc chậm hơn vì overhead combiner.

Source: mapper V1 + `hadoop/v2/SumCountCombiner.java`; reducer cuối vẫn là `FinalRevenueReducer`. `JobPlanFactory` chỉ thêm combiner cho V2; V3–V5 hiện không cấu hình combiner.

## 5. V3 — Bounded In-Mapper Aggregation

### Luồng xử lý

Mapper giữ HashMap `group → AggregateState`, tối đa `B` keys:

```text
Map(purchase):
    nếu group mới và cache đã có B keys:
        emit mọi entry trong cache
        clear cache
    cache[group] = merge(cache[group], (price_minor,1))

Cleanup:
    flush cache còn lại

Reduce:
    merge các partial cùng group và format như V1
```

Flush diễn ra **trước khi thêm một key mới vượt giới hạn**. Gặp lại key đã có không gây flush. Các purchases được gom trước `context.write`, nên framework không phải collect/sort từng purchase.

### Chi phí

- Scan và HashMap updates: kỳ vọng `O(N)`.
- Flush tổng cộng `E` entries; `G ≤ E ≤ P`.
- Sort model `O(E log(E+1))`, reduce merge `O(E)`.
- Tổng kỳ vọng: **`O(N + E log(E+1))`**.
- Bộ nhớ mapper: `O(min(B,K))`; reducer: `O(1)`.

Nếu `B ≥ K_m` với mọi mapper thì không có flush giữa chừng: `E=G ≤ min(P,MK)`. Khi đó cận đơn giản là `O(N + MK log(MK+1))`.

Nếu `B` quá nhỏ và các keys liên tục làm đầy cache, `E` có thể gần `P`; worst-case vẫn như baseline. Ví dụ `B=1`, keys xen kẽ `A,B,A,B` không giúp gom đáng kể. Phần overhead HashMap/immutable state cũng có thể làm chậm workload ít lặp.

### Khi có lợi và source

Phù hợp nhiều purchases mỗi split, số groups vừa phải hoặc cần giới hạn RAM. Không cần dictionary/preprocessing riêng.

Source: `hadoop/v3/InMapperPurchaseMapper.java`, `hadoop/shared/BoundedAccumulator.java`; reducer V1. `CACHE_FLUSHES` và `CACHE_PEAK_KEYS` hỗ trợ quan sát; Hadoop cộng peak mỗi mapper nên counter đó không phải peak tối đa toàn cụm.

## 6. V4 — Dense In-Mapper Aggregation

### Chuẩn bị dictionary

Một profile tạo danh sách group sorted và ánh xạ `group → int ID`. Dictionary phải khớp input fingerprint, group mode, policy và checksum. Driver/task validate artifact; task nhận file qua Distributed Cache.

- `DatasetProfiler` giữ count theo group bằng TreeMap: scan/profile `O(N + P log(K+1))`, RAM `O(K)`.
- Tạo danh sách IDs sorted có upper bound `O(K log(K+1))`; artifact/hash/serialization tỷ lệ tổng độ dài keys.
- Chi phí chuẩn bị phải báo riêng và cộng khi đánh giá lần sử dụng đầu. Reuse chỉ hợp lệ nếu provenance vẫn khớp.
- Guard hiện tại: tối đa 100.000 groups. Group runtime không có trong dictionary làm job fail; không âm thầm bỏ purchase.

### Luồng xử lý

```text
Setup:
    load dictionary; tạo index
    sums = long[K]; counts = long[K]

Map(purchase):
    id = index[group]
    sums[id] += price_minor (checked)
    counts[id] += 1 (checked)

Cleanup:
    với từng id:
        nếu counts[id]>0: emit(group_name, (sums[id],counts[id]))

Reduce:
    merge partials cùng group và format như V1
```

Không flush theo `B`, không tạo một aggregate object mới cho mỗi merge vào cache. Pipeline prepare vẫn có object/string allocations; đây **không phải zero-allocation parser**.

### Chi phí job

- Dictionary setup và array init qua M mappers: `O(MK)` theo keys bounded.
- Scan/map: kỳ vọng `O(N)`; index vẫn dùng HashMap, không loại bỏ string lookup.
- Cleanup quét K slots mỗi mapper: `O(MK)`.
- Emit đúng `G=ΣK_m ≤ min(P,MK)` partials.
- Sort/reduce: `O(G log(G+1))` và `O(G)`.
- Tổng kỳ vọng: **`O(N + MK + G log(G+1))`**.
- Bộ nhớ mỗi mapper `O(K)`, reducer streaming `O(1)`.
- Shuffle vẫn dùng Text group + sum/count như V3.

### So với V3

Khi V3 cache đã đủ lớn, V3 cũng emit `G` records. V4 khi đó **không giảm bậc Big-O của sort**, chỉ thay representation và tránh merge allocations. V4 có thể giảm flush khi V3 bị giới hạn `B<K_m`, nhưng dùng nhiều RAM hơn và cần dictionary. Với nhiều groups vắng trong mỗi split, quét K slots còn tốn hơn chỉ duyệt cache V3.

Source: `profile/GroupDictionary.java`, `hadoop/shared/DenseAggregationMapper.java`, `hadoop/v4/DensePurchaseMapper.java`, reducer V1. Đo trên workload hiện tại, V4 chưa nhanh hơn V3; không dùng thứ tự version làm thứ tự tốc độ mặc định.

## 7. V5 — Projected Parser + Packed Batch Aggregation

V5 giữ dense aggregation V4, tối ưu thêm parsing và record layout.

### 7.1. Projected CSV parser

Chỉ tạo giá trị cho bốn trường cần dùng: event_type, category_id, category_code, price. Fast path scan dấu phẩy cho dòng không quote/BOM/CR và kiểm tra đủ chín cột. Dòng có quoting/BOM/CR dùng lại Commons CSV reference parser.

Độ phức tạp parse vẫn `O(độ dài dòng)`; lợi ích là giảm công việc materialize/parse các cột không dùng. Parser còn kiểm tra UTF-8 byte length nên vẫn có byte-buffer allocation. Fallback giữ cùng semantics, không đơn giản hóa input contract để tạo speedup.

### 7.2. Đóng batch theo reducer ownership

```text
Map:
    prepare bằng projected parser
    cộng vào sums[id], counts[id] như V4

Cleanup:
    partition = id % R
    gom các entry có count>0 thành batch[partition]
    mỗi partition có dữ liệu:
        emit(IntWritable(partition), batch(ids,sums,counts))

Partitioner:
    đưa partition p tới đúng reducer p

Reducer p:
    load dictionary; tạo arrays K slots
    với mỗi batch và entry:
        kiểm tra id thuộc p và nằm trong dictionary
        checked merge arrays[id]
    Cleanup:
        emit các group do p sở hữu có count>0
```

Mỗi group luôn thuộc đúng một reducer bằng `id % R`, dù xuất hiện ở nhiều mappers. Một batch chứa nhiều groups, nhưng không làm trộn sum/count giữa groups. Reducer có thể nhận nhiều batches cho cùng partition key; phải merge tất cả.

### Chi phí job

- Dense map/setup `O(N + MK)` kỳ vọng.
- Đóng batch: `O(M(K+R))`; code có mảng kích thước R và quét dictionary slots.
- Số batch records `Q ≤ M·min(R,K)`; mapper không emit batch rỗng.
- Sort model `O(Q log(Q+1))` thay vì sort từng group partial.
- Payload vẫn có `G` aggregate entries, `G≤MK`; serialize và merge entries `O(G)`.
- Mỗi reducer hiện load toàn dictionary và init arrays K slots: tổng setup `O(RK)`. Cleanup chia ownership nên tổng số slots reducer duyệt là `O(K+R)`.
- Tổng kỳ vọng cho implementation hiện tại:
  **`O(N + M(K+R) + RK + Q log(Q+1) + G)`**.
- Bộ nhớ mỗi mapper **`O(K+R)`**, mỗi reducer **`O(K)`**. Tổng bộ nhớ các tasks không đồng nghĩa bộ nhớ một process; còn phụ thuộc số tasks chạy đồng thời.

**Giảm records không đồng nghĩa giảm bytes cùng tỷ lệ.** Hai batches vẫn mang các IDs/sums/counts của G entries. Payload là `O(G)` entries, không phải `O(Q)`. Keys chuyển từ chuỗi group sang integer có thể giảm repeated key bytes, nhưng batch headers/IDs cũng có overhead.

Hadoop vẫn sort/merge các batch keys; không tuyên bố V5 bỏ shuffle hoặc có sort cost bằng 0. Nếu R tăng rất lớn, overhead `MR` và `RK` có thể bất lợi. Batch lớn cũng cần RAM/buffer đủ; cardinality guard giới hạn payload hiện tại.

### Khi có lợi và source

Phù hợp input có nhiều purchase lặp group, K vừa phải, R nhỏ và CSV phần lớn đi fast path. Với K lớn, CSV thường dùng fallback hoặc job quá nhỏ, lợi ích có thể bị overhead che lấp.

Source: `input/ProjectedCsvEventParser.java`, `hadoop/v5/BatchPurchaseMapper.java`, `hadoop/v5/BatchPartitioner.java`, `hadoop/v5/BatchRevenueReducer.java`, `hadoop/io/AggregateBatchWritable.java`; dùng chung dense mapper và dictionary V4. Batch writable defensive-copy arrays, kiểm tra length/IDs/count/sums khi đọc.

## 8. Bảng tổng hợp

Các chi phí dưới đây chưa gồm profile chung/riêng ngoài job và dùng lookup kỳ vọng cùng comparison-sort model.

| Variant | Map output records | Tổng công việc job | RAM mapper | RAM reducer |
|---|---:|---|---|---|
| V1 | `P` | `O(N + P log(P+1))` | `O(1)` | `O(1)` |
| V2 | `P`; combiner giảm records tới reducer | Worst-case như V1 | `O(1)` state | `O(1)` |
| V3 | `E`, `G≤E≤P` | `O(N + E log(E+1))` | `O(min(B,K))` | `O(1)` |
| V4 | `G≤min(P,MK)` | `O(N + MK + G log(G+1))` | `O(K)` | `O(1)` |
| V5 | `Q≤M min(R,K)` batches | `O(N + M(K+R) + RK + Q log(Q+1) + G)` | `O(K+R)` | `O(K)` |

Chuỗi giảm công việc:

```text
V1: sort/shuffle mỗi purchase
V2: gộp bằng framework trước shuffle, vẫn collect/sort map records
V3: gộp trước emit, giảm records framework phải sort
V4: dense representation, không flush; giảm allocations khi merge
V5: projection + packed batches, giảm materialization và batch sort keys
```

Nếu M,K,R cố định và cache đủ nhóm, V3–V5 có thể tiến gần chi phí tuyến tính theo N. Nếu K tăng cùng P hoặc cache liên tục flush, không được coi MK là hằng số và không thể suy ra mọi variant đều O(N).

## 9. Chứng minh bằng test và benchmark

- `VariantsIT`: oracle SUM/COUNT/AVG cho mọi variant, nhiều splits/reducers và empty input.
- `RecordReductionIT`: 600 purchases/3groups/1mapper/2reducers → V1/V2 600 records, V3/V4 3 records, V5 2 batches; cùng output và một job.
- Parser parity, dictionary provenance, writable/partitioning và failure paths có tests riêng.
- Benchmark synthetic 200.000 purchases/32groups/1mapper/2reducers: V1/V2 emit200.000, V3/V4 emit32, V5 emit2batches. Median job V1=1266ms,V2=1158ms,V3=1051ms,V4=1146ms,V5=851ms, cùng completion polling100ms.
- V4 chậm hơn V3 trong phép đo này; V5 median nhanh hơn baseline khoảng1,49×. Chỉ ba lượt đo, không phải bảo đảm cho mọi workload.
- V4/V5 phải cộng profile scan1085ms cho lần dùng đầu; CLI+profile hiện chưa thắng baselineCLI. Không bỏ preprocessing để tạo kết luận tốc độ sai.
- Default local polling5000ms có thể che chênh lệch. So sánh cùng polling và resources, không chỉnh riêng từng variant.

Xem [benchmark và raw evidence](benchmark-report.md), [function từng file](project-structure.md), [hướng dẫn chạy](runbook.md). Kết quả trên dữ liệu Kaggle thật (D1, D2, cả tháng D3, MR trên HDFS) ở `docs/evidence/bench/SUMMARY.md`; YARN, nhiều nút và high-cardinality chưa đo.

## 10. Spark: cùng bài toán A1

Mã nguồn: `bigdata/src/main/java/vn/edu/bigdata/revenue/spark/RevenueJob.java`. Spark đọc **cùng file trên HDFS** và dùng **cùng**
`CsvEventParser`, `PurchasePreparation`, `Money` với MapReduce, nên chính sách lọc và cách tính tiền giống hệt; kết quả được so khớp
chính xác `(group, sum_minor, count, avg)` với MR V1 (D1, D2, D3) và với baseline Python độc lập.

### 10.1 RDD (`--mode rdd-raw`, chế độ đối chứng MR)

```text
textFile(hdfs://.../2019-Oct.csv)                       HadoopRDD: 1 partition / 1 block HDFS (D3: 43 partition)
  -> flatMapToPair(line -> [(category_id, (price_minor, 1))] nếu là purchase hợp lệ, [] nếu không)      ~ Map
  -> reduceByKey(merge (sum, count), 2 partition)       ~ combine phía map + shuffle hash + reduce
  -> collect, chia AVG ở driver (HALF_UP), ghi Parquet
```

Lineage thật trên D3 (`docs/evidence/spark-java/plans/a1-rdd-d3-lineage.txt`):

```text
(2) ShuffledRDD[3] at reduceByKey at RevenueJob.java:93
 +-(43) MapPartitionsRDD[2] at flatMapToPair at RevenueJob.java:83
    |   hdfs://localhost:8020/data/ecommerce/raw/2019-Oct.csv MapPartitionsRDD[1] at textFile at RevenueJob.java:81
    |   hdfs://localhost:8020/data/ecommerce/raw/2019-Oct.csv HadoopRDD[0] at textFile at RevenueJob.java:81
```

Ánh xạ sang mô hình MapReduce (về **mô hình**, không phải cùng cơ chế thực thi):

| Bước | Hadoop MapReduce (V2) | Spark RDD |
|---|---|---|
| Map | `DirectPurchaseMapper.map` phát `(category_id, (price, 1))` | hàm của `flatMapToPair` |
| Gộp trước shuffle | `SumCountCombiner` (Hadoop có thể gọi 0..n lần) | `reduceByKey` luôn gộp phía map (map-side combine) với cùng hàm merge |
| Shuffle | sort + partition `HashPartitioner`, ghi đĩa, reducer kéo về | shuffle hash theo key, ghi file shuffle, task sau đọc |
| Reduce | `FinalRevenueReducer` merge rồi format | phần gộp cuối của `reduceByKey`; format ở driver |

Hàm merge là phép cộng cặp `(sum, count)`: có tính kết hợp và giao hoán, nên gộp bao nhiêu lần, theo thứ tự nào cũng ra cùng kết quả.
Đây là lý do combiner của MR và map-side combine của Spark đều đúng với AVG (không lấy trung bình của các trung bình).

Số đo trên D3 (cả tháng, 42 448 765 dòng):

| | MR V2 (combiner) | Spark RDD `reduceByKey` |
|---|---:|---:|
| Bản ghi phát ra từ map | 742 849 (`MAP_OUTPUT_RECORDS`) | — (gộp ngay trong task) |
| Bản ghi trung gian qua shuffle | 15 511 (`COMBINE_OUTPUT_RECORDS` = `REDUCE_INPUT_RECORDS`) | 15 511 (`shuffleWriteRecords`) |
| Byte shuffle | 589 934 (`REDUCE_SHUFFLE_BYTES`) | 362 451 (`shuffleWriteBytes`) |
| Nhóm đầu ra | 567 | 567 |

Nguồn: `docs/evidence/bench/mr/mr-e2-d3/runs.json`, `docs/evidence/spark-java/d3/20261006-223434-b0376ef-d3/revenue-run.json`.
Nhận định (chưa kiểm chứng riêng): số bản ghi trung gian bằng nhau vì cả hai cùng gộp theo (split, nhóm) và cùng chia input theo block
HDFS 128 MB; khác biệt byte do định dạng serialize (Writable của Hadoop so với serializer của Spark).

### 10.2 DataFrame (`--mode df-raw`, `df-curated`)

`filter(event_type = purchase AND category_id IS NOT NULL) -> groupBy(category_id) -> agg(sum(price_minor), count(*))`.
Catalyst lập kế hoạch hai pha aggregate, thấy rõ trong physical plan thật trên D2 (`docs/evidence/spark-java/plans/a1-df-curated-d2.txt`, rút gọn):

```text
HashAggregate(keys=[category_id], functions=[sum(price_minor), count(1)])                  <- final aggregate (~ reduce)
+- AQEShuffleRead coalesced
   +- Exchange hashpartitioning(category_id, 8)                                              <- shuffle
      +- HashAggregate(keys=[category_id], functions=[partial_sum(price_minor), partial_count(1)])  <- partial aggregate (~ combiner)
         +- Project [category_id, price_minor]
            +- Filter (event_type = purchase AND isnotnull(category_id))
               +- FileScan parquet [event_type, category_id, price_minor] PushedFilters: [EqualTo(event_type,purchase), ...]
```

- `partial_sum/partial_count` trước `Exchange` đóng vai trò combiner; `HashAggregate` sau `Exchange` là phép gộp cuối.
- `Exchange hashpartitioning(category_id, 8)` là shuffle theo `spark.sql.shuffle.partitions` (E4 thử 8/64/200);
  AQE gộp bớt partition rỗng khi chạy.
- Với Parquet curated, Spark chỉ đọc 3 cột cần dùng và đẩy bộ lọc xuống lúc quét file (`PushedFilters`), nên trên D3 thời gian tính
  giảm từ 227 s (RDD trên CSV) xuống 18 s. Con số này **không gồm** chi phí ETL một lần tạo Parquet (khoảng 23 phút trên D3).
- `df-raw` phải parse toàn bộ CSV như ETL nên chậm hơn RDD trên cùng CSV (272 s so với 227 s trên D3).

### 10.3 MapReduce và Spark: phân biệt khi trình bày

- Code MapReduce của dự án là **Hadoop MapReduce thật** (Mapper/Combiner/Partitioner/Reducer, counters của Hadoop, LocalJobRunner).
  Spark **không** chạy trên Hadoop MapReduce; Spark chỉ dùng HDFS để đọc/ghi. `reduceByKey` không phải Hadoop Reducer.
- Spark tối ưu nhờ: gộp phía map luôn bật, DAG nhiều bước trong một job, Parquet dạng cột, Catalyst và AQE; MR tối ưu nhờ các biến thể
  V2–V5 do nhóm tự cài (combiner, in-mapper, dictionary, batch).
- Trên máy nhóm, MR V5 (61 s) nhanh hơn Spark RDD trên CSV (227 s) cho D3, nhưng hai bên chạy ở môi trường I/O khác nhau
  (MR trong container cùng mạng Docker với DataNode, Spark trên host qua cổng chuyển tiếp WSL2). Không kết luận engine nào nhanh hơn
  nói chung; xem E5 trong `docs/evidence/bench/SUMMARY.md`.
- Thuật toán lặp (K-Means) thuộc về Spark: mỗi vòng lặp MR phải đọc lại dữ liệu từ HDFS, còn Spark giữ dữ liệu trong bộ nhớ.
  E7 đo tác động của `cache()` trên D3 (fit 20 vòng: 4 849 ms có cache so với 5 770 ms không cache), xem `docs/ML.md`.
