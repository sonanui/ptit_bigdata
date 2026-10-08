# Luồng code chi tiết: dữ liệu đi qua từng dòng như thế nào

Tài liệu dùng để học và trả lời thầy khi bị hỏi "dòng này làm gì, dữ liệu lúc này trông ra sao". Đi theo đúng thứ tự
chương trình chạy, từ lệnh gõ trên máy tới con số hiện trên web. Mỗi đoạn code được chép kèm **số dòng thật** trong
file, sau đó giải thích từng dòng và ghi rõ **dữ liệu vào / ra** tại đó.

- Đường dẫn Java viết tắt: `revenue/…` = `bigdata/src/main/java/vn/edu/bigdata/revenue/…`.
- Bản tóm tắt ngắn hơn: `docs/CODE_WALKTHROUGH.md`. Cách cài và chạy trên máy: báo cáo mục 3.2 và `scripts/native/`.

## 0. Hai bộ dữ liệu dùng làm ví dụ

**Ví dụ A: tệp 7 dòng** `bigdata/src/test/resources/fixtures/events.csv`. Đủ nhỏ để tính tay, và test tự động
(`VariantsIT`) kiểm tra đúng kết quả này.

```text
dòng 1  event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
dòng 2  2019-10-01 00:00:00 UTC,purchase,1,1,electronics.smartphone,brand,10.00,test,session
dòng 3  2019-10-01 00:00:01 UTC,purchase,1,1,electronics.smartphone,brand,20.00,test,session
dòng 4  2019-10-01 00:00:02 UTC,purchase,1,1,electronics.smartphone,brand,30.00,test,session
dòng 5  2019-10-01 00:00:03 UTC,view,1,1,electronics.smartphone,brand,99.00,test,session
dòng 6  2019-10-01 00:00:04 UTC,purchase,2,2,,brand,0.00,test,session
dòng 7  2019-10-01 00:00:05 UTC,purchase,2,2,,brand,0.01,test,session
dòng 8  2019-10-01 00:00:06 UTC,purchase,2,2,,brand,0.02,test,session
```

Kết quả đúng (tính tay): danh mục `1` có 10 + 20 + 30 = 60,00 với 3 lượt mua, trung bình 20,00 (dòng `view` giá 99 không
được cộng). Danh mục `2` có 0,00 + 0,01 + 0,02 = 0,03 với 3 lượt mua, trung bình 0,01.

**Ví dụ B: dữ liệu thật**, sản phẩm 1002099 (điện thoại Samsung) thuộc danh mục `2053013555631882655`, một dòng trong
mẫu 1% (D1):

```text
2019-10-03 00:24:38 UTC,purchase,1002099,2053013555631882655,electronics.smartphone,samsung,370.41,542074160,edf4af5a-...
```

---

## 1. Điểm bắt đầu: lệnh chạy MapReduce trên máy

```powershell
.\scripts\native\mr-native.ps1 /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv <RUN_ID> v1 v2 v5
```

Script này gọi lần lượt (xem `scripts/native/mr-native.ps1`):

1. `hadoop jar revenue-aggregation.jar vn.edu.bigdata.revenue.cli.DatasetTool preflight …`: quét input trên HDFS, ghi
   `input.json` (danh sách tệp, kích thước, SHA-256, fingerprint) và `preflight.json` (số dòng, số purchase hợp lệ, lỗi).
2. `… DatasetTool profile …`: chỉ khi chạy V4/V5. Tạo `groups.json` là từ điển `category_id → số thứ tự`.
3. `… RevenueTool --variant v1 --manifest … --preflight … --output hdfs://localhost:8020/data/ecommerce/mr/<RUN_ID>/v1 --reducers 2`.
4. `… DatasetTool compare` (so v1 với từng biến thể) và `… DatasetTool export` (ra `revenue.csv`).

`hadoop jar` là lệnh của bản Hadoop cài trên máy. Nó nạp cấu hình từ `HADOOP_CONF_DIR`, gồm
`fs.defaultFS = hdfs://localhost:8020` và `mapreduce.framework.name = local`. Nhờ đó job đọc/ghi HDFS và chạy bằng
**LocalJobRunner**, tức Map, Shuffle và Reduce chạy trong cùng một JVM, không có YARN.

## 2. `RevenueTool`: đọc tham số, kiểm tra input, dựng và chạy job

File `revenue/cli/RevenueTool.java`.

```java
146  public static void main(String[] args) throws Exception {
147    System.exit(ToolRunner.run(new Configuration(), new RevenueTool(), args));
148  }
```

- **146–147**: `ToolRunner` tách các tham số Hadoop chung (`-D key=value`) ra khỏi `args`, nạp chúng vào `Configuration`,
  rồi gọi `run()`. Mã thoát của chương trình là giá trị `run()` trả về.

```java
54      options.variant = Variant.parse(a.required("variant"));
55      options.groupMode = GroupMode.parse(a.get("group-by", "category_id"));
56      Path manifestPath = new Path(a.required("manifest")),
57          reportPath = new Path(a.required("preflight"));
58      options.manifest =
59          JsonArtifacts.read(manifestPath, manifestPath.getFileSystem(conf), InputManifest.class);
60      options.preflight =
61          JsonArtifacts.read(reportPath, reportPath.getFileSystem(conf), PreflightReport.class);
62      options.output = new Path(a.required("output"));
63      options.reducers = a.integer("reducers", 2);
```

- **54**: `"v1"` thành `Variant.V1_DIRECT`.
- **55**: khóa nhóm mặc định là `category_id`.
- **56–61**: đọc hai tệp JSON do bước preflight tạo. `getFileSystem(conf)` chọn hệ tệp theo scheme của đường dẫn: `file:///…`
  là ổ đĩa máy, `hdfs://…` là HDFS.
- **62–63**: thư mục output trên HDFS và số reducer (R = 2).

```java
78      List<Path> inputs = new ArrayList<>();
79      for (var entry : options.manifest.files) inputs.add(new Path(entry.uri));
80      FileSystem inputFs = inputs.get(0).getFileSystem(conf);
81      PreflightReport fresh = DatasetPreflight.inspect(inputs, inputFs, options.groupMode);
82      if (!fresh.isValid()
83          || !fresh.inputFingerprint.equals(options.manifest.fingerprint())
84          || fresh.validPurchaseCount != options.preflight.validPurchaseCount)
85        throw new IllegalArgumentException("Input changed or preflight invalid");
```

- **78–81**: quét lại input ngay trước khi chạy job.
- **82–85**: nếu tệp đã bị sửa (fingerprint khác) hoặc số purchase hợp lệ khác lần preflight thì dừng với mã 2. Đây là
  cách bảo đảm kết quả các phương án so sánh được với nhau: chúng chạy trên **cùng một input**.

```java
86      JobPlan plan = JobPlanFactory.build(conf, options);
87      RunManifest run = JobExecutor.execute(plan);
```

- **86**: dựng đối tượng `Job` của Hadoop (mục 3).
- **87**: chạy job và chờ xong (mục 4).

```java
110      if (!run.validationStatus.equals("failed")) {
111        long actual = run.stages.get(0).counters.getOrDefault(
116                "vn.edu.bigdata.revenue.hadoop.shared.RevenueCounters.VALID_PURCHASE", 0L);
117        ValidationReport validation =
118            ResultValidator.validate(options.output, outputFs, fresh.validPurchaseCount);
119        if (actual != fresh.validPurchaseCount) validation.fail("Stage1 purchase counter mismatch");
...
127      JsonArtifacts.write(new Path(options.output, "run-manifest.json"), outputFs, run);
```

- **111–116**: lấy counter `VALID_PURCHASE` mà các mapper đã đếm.
- **117–118**: `ResultValidator` đọc mọi `part-r-*`. Nó từ chối nếu một khóa xuất hiện hai lần, và kiểm tra tổng `count`
  bằng số purchase hợp lệ của preflight.
- **119**: counter phải bằng con số của preflight. Lệch nghĩa là có dòng bị mất hoặc bị đếm hai lần.
- **127**: ghi `run-manifest.json` vào cạnh output. Tệp chứa mọi counter, thời gian và cấu hình; đây là bằng chứng của
  lần chạy.
- Mã thoát: 0 = hợp lệ, 1 = job lỗi hoặc kết quả sai, 2 = tham số hoặc input sai.

## 3. `JobPlanFactory.build`: nối Mapper, Combiner, Partitioner, Reducer

File `revenue/job/JobPlanFactory.java`.

```java
37    Job job = Job.getInstance(conf, "revenue-" + options.variant);
38    job.setJarByClass(JobPlanFactory.class);
39    job.setInputFormatClass(TextInputFormat.class);
40    for (var entry : options.manifest.files)
41      FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(entry.uri));
42    job.setMapOutputKeyClass(Text.class);
43    job.setMapOutputValueClass(SumCountWritable.class);
44    job.setReducerClass(FinalRevenueReducer.class);
```

- **39**: `TextInputFormat` chia tệp thành các *split*. Mặc định mỗi split bằng một block HDFS (128 MB): cả tháng có 43
  block nên có 43 split, tức 43 map task. Với mỗi dòng, nó gọi `map(offset, dòng)`: khóa là vị trí byte đầu dòng, giá trị
  là nội dung dòng.
- **42–43**: kiểu dữ liệu giữa Map và Reduce là cặp `(Text, SumCountWritable)`, tức `(category_id, (tổng xu, số lượt))`.
- **44**: reducer mặc định cho V1–V4.

```java
45    switch (options.variant) {
46      case V1_DIRECT:   job.setMapperClass(DirectPurchaseMapper.class); break;
49      case V2_COMBINER: job.setMapperClass(DirectPurchaseMapper.class);
51                        job.setCombinerClass(SumCountCombiner.class); break;
53      case V3_IN_MAPPER: job.setMapperClass(InMapperPurchaseMapper.class); break;
56      case V4_DENSE:    job.setMapperClass(DensePurchaseMapper.class); break;
59      case V5_BATCH:    job.setMapperClass(BatchPurchaseMapper.class);
61                        job.setMapOutputKeyClass(IntWritable.class);
62                        job.setMapOutputValueClass(AggregateBatchWritable.class);
63                        job.setPartitionerClass(BatchPartitioner.class);
64                        job.setReducerClass(BatchRevenueReducer.class); break;
```

- **45–64**: năm phương án khác nhau **chỉ** ở các dòng này. Bài toán, input và định dạng output giữ nguyên.
- V2 chỉ thêm combiner (dòng 51). V5 đổi khóa trung gian thành số partition (`IntWritable`) và giá trị thành một gói
  nhiều danh mục.

```java
69    if (options.variant == Variant.V4_DENSE || options.variant == Variant.V5_BATCH) {
70      URI uri = options.dictionaryPath.getFileSystem(conf).makeQualified(options.dictionaryPath).toUri();
72      job.addCacheFile(URI.create(uri + "#group-dictionary.json"));
73    }
74    job.setNumReduceTasks(options.reducers);
77    job.setOutputFormatClass(TextOutputFormat.class);
79    FileOutputFormat.setOutputPath(job, options.output);
```

- **69–72**: *distributed cache*. Tệp từ điển được chép tới mọi task dưới tên `group-dictionary.json` để mapper đọc như
  tệp cục bộ.
- **74**: R = 2, nên output có `part-r-00000` và `part-r-00001`.
- **77**: mỗi dòng output có dạng `khóa<TAB>giá trị`.
- **79**: thư mục output phải chưa tồn tại. Hadoop từ chối ghi đè, nên chạy lại không làm hỏng kết quả cũ.

## 4. `JobExecutor.execute`: chạy và thu counters

File `revenue/job/JobExecutor.java`.

```java
21        stage.success = job.waitForCompletion(false);
22        stage.jobId = String.valueOf(job.getJobID());
23        stage.counters = MetricsCollector.collect(job);
24        if (stage.success
25            && job.getCounters().findCounter(RevenueCounters.MALFORMED_CSV).getValue() > 0) {
26          stage.success = false;
27          stage.error = "Runtime malformed CSV";
28        }
```

- **21**: gửi job và chờ. Với LocalJobRunner, toàn bộ Map → Shuffle → Reduce chạy ở đây.
- **23**: lấy mọi counter. Hadoop có sẵn các counter như `MAP_INPUT_RECORDS`, `MAP_OUTPUT_RECORDS` và
  `REDUCE_SHUFFLE_BYTES`; nhóm tự định nghĩa thêm `HEADERS`, `NON_PURCHASE`, `VALID_PURCHASE`….
- **24–27**: có dòng CSV hỏng thì coi cả lần chạy là thất bại, dù Hadoop báo thành công.

## 5. Pha Map (V1): một dòng CSV thành cặp key-value

### 5.1 `DirectPurchaseMapper.map`

File `revenue/hadoop/v1/DirectPurchaseMapper.java`.

```java
14 public class DirectPurchaseMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {
15   private final MapperSupport support = new MapperSupport();
18   protected void setup(Context context) {
19     mode = GroupMode.parse(context.getConfiguration().get(RunOptions.GROUP_MODE, "CATEGORY_ID"));
20   }
22   protected void map(LongWritable offset, Text line, Context context) … {
24     PreparationResult result = support.prepare(line, context, mode);
25     if (result.valid()) emit(result.group(), result.state(), context);
26   }
28   protected void emit(String group, AggregateState state, Context context) … {
30     context.write(new Text(group), new SumCountWritable(state.sumMinor(), state.purchaseCount()));
31   }
```

- **14**: bốn kiểu dữ liệu. Vào là `(LongWritable offset, Text dòng)`; ra là `(Text category_id, SumCountWritable)`.
- **18–19**: `setup` chạy một lần cho mỗi map task, đọc khóa nhóm từ cấu hình job.
- **22**: `map` chạy **một lần cho mỗi dòng** của split.
- **24**: chuyển dòng cho phần làm sạch (5.2–5.5).
- **25**: chỉ dòng purchase hợp lệ mới được phát ra.
- **30**: phát cặp. Với ví dụ A:

| Dòng | Kết quả của `prepare` | `context.write` phát ra |
|---|---|---|
| 1 (header) | `HEADER` | — |
| 2 | hợp lệ, nhóm `1`, giá 1000 xu | `("1", (1000, 1))` |
| 3 | hợp lệ | `("1", (2000, 1))` |
| 4 | hợp lệ | `("1", (3000, 1))` |
| 5 (`view`) | `NON_PURCHASE` | — |
| 6 | hợp lệ, nhóm `2`, giá 0 xu | `("2", (0, 1))` |
| 7 | hợp lệ | `("2", (1, 1))` |
| 8 | hợp lệ | `("2", (2, 1))` |

Ví dụ B: dòng của sản phẩm 1002099 phát ra `("2053013555631882655", (37041, 1))`.

### 5.2 `MapperSupport.prepare`: làm sạch và đếm

File `revenue/hadoop/shared/MapperSupport.java`.

```java
22   public PreparationResult prepare(Text line, Mapper.Context context, GroupMode mode) {
23     PreparationResult result = PurchasePreparation.prepare(parser.parse(line.toString()), mode);
24     String reason = result.reason().name();
25     if (reason.equals("HEADER")) reason = "HEADERS";
26     context.getCounter(RevenueCounters.valueOf(reason)).increment(1);
27     if (result.missingCategoryCode())
28       context.getCounter(RevenueCounters.MISSING_CATEGORY_CODE).increment(1);
29     return result;
30   }
```

- **23**: `Text` thành `String`, sau đó parse (5.3) và phân loại (5.4).
- **26**: **mỗi dòng tăng đúng một counter** theo lý do. Vì vậy có đẳng thức
  `MAP_INPUT_RECORDS = HEADERS + NON_PURCHASE + VALID_PURCHASE + (các lỗi)`. Trên mẫu D1:
  424 610 = 1 + 417 145 + 7 464.

### 5.3 `CsvEventParser`: tách 9 trường

File `revenue/input/CsvEventParser.java`.

```java
17   public ParseResult parse(String line) {
18     String[] r = fields(line);
19     if (r == null) return ParseResult.malformed();
20     if (isHeader(r)) return ParseResult.header();
21     return ParseResult.event(new ParsedEvent(r[1], r[3], r[4], r[6]));
22   }
29   public static String[] fields(String line) {
30     if (line.endsWith("\r")) line = line.substring(0, line.length() - 1);
31     if (line.getBytes(StandardCharsets.UTF_8).length > MAX_LINE_BYTES || line.indexOf('\n') >= 0)
32       return null;
34     if (line.startsWith("﻿")) line = line.substring(1);
35     try (CSVParser parser = CSVParser.parse(line, CSVFormat.RFC4180)) {
36       List<CSVRecord> rows = parser.getRecords();
37       if (rows.size() != 1 || rows.get(0).size() != 9) return null;
```

- **30**: bỏ `\r` cuối dòng (tệp xuống dòng kiểu Windows).
- **31**: dòng dài quá 64 KiB coi là hỏng, để chặn dòng rác chiếm bộ nhớ.
- **34**: bỏ ký tự BOM ở đầu tệp nếu có.
- **35–37**: tách theo chuẩn CSV RFC 4180, nên hiểu được dấu phẩy nằm trong ngoặc kép. Dòng phải có **đúng 9 trường**.
- **20**: dòng giống hệt header thì là header.
- **21**: chỉ giữ 4 trường cần dùng: `r[1]` event_type, `r[3]` category_id, `r[4]` category_code, `r[6]` price.
  Dòng 2 của ví dụ A thành `ParsedEvent("purchase", "1", "electronics.smartphone", "10.00")`.

### 5.4 `PurchasePreparation.prepare`: phân loại dòng

File `revenue/input/PurchasePreparation.java`.

```java
15     if (parsed.kind == ParseResult.Kind.HEADER) return PreparationResult.skipped(HEADER);
16     if (parsed.kind == ParseResult.Kind.MALFORMED) return PreparationResult.skipped(MALFORMED_CSV);
18     if (!e.eventType.equals("purchase")) {
19       return PreparationResult.skipped(
20           e.eventType.equals("view") || e.eventType.equals("cart") || e.eventType.equals("remove_from_cart")
23               ? NON_PURCHASE : UNKNOWN_EVENT);
25     }
28       price = Money.parseMinor(e.priceText);
30       return PreparationResult.skipped(INVALID_PRICE);
32     Purchase p = new Purchase(e.categoryId, e.categoryCode, price);
34       String group = GroupKeyResolver.resolve(p, mode);
35       if (group.isEmpty() || group.getBytes(StandardCharsets.UTF_8).length > 256)
36         return PreparationResult.skipped(INVALID_GROUP);
37       return PreparationResult.accepted(group, price, mode != GroupMode.CATEGORY_ID && p.categoryCode.isEmpty());
```

- **15–16**: header và dòng hỏng bị loại.
- **18–24**: không phải `purchase` thì loại. Ba loại sự kiện đã biết đếm vào `NON_PURCHASE`; loại lạ đếm vào
  `UNKNOWN_EVENT` để phát hiện dữ liệu bất thường. Dòng 5 của ví dụ A (`view`, giá 99) dừng ở đây, **không bao giờ** tới
  phép cộng.
- **28**: chuyển giá sang số nguyên xu (5.5).
- **34–36**: lấy khóa nhóm. Với `category_id`, khóa phải toàn chữ số (`GroupKeyResolver` dòng 8).
- **37**: `accepted` tạo `AggregateState.single(price)`, tức `(sum = price, count = 1)`.

### 5.5 `Money` và `AggregateState`: vì sao tiền là `long`

```java
// revenue/domain/Money.java
11       BigDecimal value = new BigDecimal(text.trim()).setScale(2, RoundingMode.UNNECESSARY);
12       if (value.signum() < 0) throw new IllegalArgumentException("Negative price");
13       return value.movePointRight(2).longValueExact();
23   public static String average(long sum, long count) {
25     return BigDecimal.valueOf(sum, 2).divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP).toPlainString();
```

- **11**: `"10.00"` thành `BigDecimal 10.00`. `UNNECESSARY` báo lỗi nếu giá có hơn 2 chữ số thập phân, tức không làm tròn
  ngầm.
- **13**: dời dấu phẩy 2 chữ số: 10.00 thành **1000** xu, 370.41 thành **37041** xu.
- **23–25**: trung bình chỉ tính **một lần ở cuối**, làm tròn HALF_UP 2 chữ số.

```java
// revenue/domain/AggregateState.java
22   public AggregateState merge(AggregateState other) {
23     return new AggregateState(
24         Math.addExact(sumMinor, other.sumMinor), Math.addExact(purchaseCount, other.purchaseCount));
```

- **22–24**: phép gộp là cộng từng thành phần. `Math.addExact` ném lỗi khi tràn số thay vì âm thầm ra số sai.
- Vì phép này kết hợp và giao hoán, với phần tử trung hòa `(0, 0)`, gộp ở đâu trước (combiner, trong mapper, reducer) và
  theo thứ tự nào cũng ra cùng kết quả. Đây là lý do **không** gộp bằng trung bình: trung bình của các trung bình sai
  khi các nhóm có số phần tử khác nhau.
- Không dùng `double`: 0,01 + 0,02 trong số thực nhị phân không ra đúng 0,03. Danh mục `2` của ví dụ A là phép thử
  chuyện này.

### 5.6 `SumCountWritable`: dữ liệu được ghi ra đĩa và gửi đi thế nào

File `revenue/hadoop/io/SumCountWritable.java`.

```java
28   public void write(DataOutput out) throws IOException {
29     out.writeLong(sum);
30     out.writeLong(count);
31   }
33   public void readFields(DataInput in) throws IOException {
35       set(in.readLong(), in.readLong());
```

- **28–30**: mỗi giá trị chiếm đúng 16 byte (hai số `long`). Hadoop gọi hàm này khi ghi output của map ra bộ đệm hoặc
  spill xuống đĩa.
- **33–35**: phía reducer đọc lại đúng thứ tự. `set` kiểm tra lại tính hợp lệ (không âm…).

## 6. Shuffle: cặp nào đi tới reducer nào

Không có code của nhóm ở bước này (V1–V4); Hadoop dùng `HashPartitioner`:

```text
partition = (key.hashCode() & Integer.MAX_VALUE) % R
```

`Text.hashCode()` bắt đầu từ 1 và với mỗi byte tính `h = 31*h + byte`:

- khóa `"1"`: 31·1 + 49 = 80, 80 mod 2 = **0**, đi tới reducer 0 (`part-r-00000`);
- khóa `"2"`: 31·1 + 50 = 81, 81 mod 2 = **1**, đi tới reducer 1 (`part-r-00001`).

Đúng như output thật của lần chạy demo: `part-r-00000` chứa `1	60.00	3	20.00`, còn `part-r-00001` chứa `2	0.03	3	0.01`.
Trong mỗi partition, Hadoop **sắp xếp theo khóa** rồi gom các giá trị cùng khóa thành một `Iterable` cho `reduce`. Ví dụ A,
reducer 0 nhận `"1" → [(1000,1), (2000,1), (3000,1)]`.

## 7. Combiner (V2)

File `revenue/hadoop/v2/SumCountCombiner.java`.

```java
11   protected void reduce(Text key, Iterable<SumCountWritable> values, Context context) … {
13     AggregateState total = AggregateState.empty();
14     for (SumCountWritable value : values) total = total.merge(value.toState());
15     context.write(key, new SumCountWritable(total.sumMinor(), total.purchaseCount()));
```

- Chạy **ở phía map**, trên dữ liệu đã sort trong bộ đệm, trước khi gửi qua shuffle.
- Với ví dụ A, `"1" → [(1000,1),(2000,1),(3000,1)]` được gộp thành `("1", (6000,3))`. Ba bản ghi còn một bản ghi.
- Hadoop **có thể gọi combiner 0, 1 hay nhiều lần**. Code vẫn đúng trong mọi trường hợp nhờ phép gộp ở 5.5.
- Counters D1: `Map output records` vẫn là 7 464, vì combiner không đổi số bản ghi map phát ra. `Combine output records`
  là 308, và `Reduce shuffle bytes` giảm từ 283 644 xuống 11 716.

## 8. Pha Reduce và định dạng output

File `revenue/hadoop/shared/FinalRevenueReducer.java` và `revenue/output/RevenueFormatter.java`.

```java
11   protected void reduce(Text key, Iterable<SumCountWritable> values, Context context) … {
13     AggregateState total = AggregateState.empty();
14     for (SumCountWritable value : values) total = total.merge(value.toState());
15     context.write(key, new Text(RevenueFormatter.value(total)));

// RevenueFormatter
10     return Money.formatMinor(state.sumMinor()) + "\t" + state.purchaseCount() + "\t"
14         + Money.average(state.sumMinor(), state.purchaseCount());
```

- **13–14**: duyệt từng giá trị (streaming), không giữ cả danh sách trong RAM. `"1"` cộng ra `(6000, 3)`.
- **15**: `RevenueFormatter` cho `"60.00\t3\t20.00"`. `TextOutputFormat` ghi `1<TAB>60.00<TAB>3<TAB>20.00`.
- Ví dụ B (cả tháng): `2053013555631882655  157049623.37  338018  464.62`.

## 9. V3: gộp ngay trong mapper, có giới hạn bộ nhớ

Files `revenue/hadoop/v3/InMapperPurchaseMapper.java` và `revenue/hadoop/shared/BoundedAccumulator.java`.

```java
// InMapperPurchaseMapper
15     cache = new BoundedAccumulator<>(context.getConfiguration().getInt(RunOptions.MAX_KEYS, 10000));
18   protected void emit(String group, AggregateState value, Context context) … {
20     cache.add(group, value, (k, v) -> super.emit(k, v, context));
23   protected void cleanup(Context context) … {
24     cache.flush((k, v) -> super.emit(k, v, context));

// BoundedAccumulator
24   public void add(K key, AggregateState value, Emitter<K> emitter) … {
26     if (!cache.containsKey(key) && cache.size() == maxKeys) flush(emitter);
27     cache.merge(key, value, AggregateState::merge);
```

- V3 kế thừa V1 nhưng **ghi đè `emit`** (dòng 18–20): thay vì `context.write` ngay, nó cộng dồn vào `HashMap`.
- **BoundedAccumulator 26**: khi gặp khóa mới mà map đã đủ `maxKeys` khóa, phát hết ra rồi xóa. Bộ nhớ không bao giờ vượt
  `maxKeys` khóa.
- **cleanup 24**: hết split thì phát nốt phần còn lại.
- Ví dụ A với `--max-keys 1` (lệnh `demo.sh`): sau `"1"` cache đầy, gặp `"2"` thì phát `("1",(6000,3))`, cuối split phát
  `("2",(3,3))`. Counters thật: `CACHE_FLUSHES = 2`, `MAP_OUTPUT_RECORDS = 2`.

## 10. V4: mảng số theo từ điển

Files `revenue/hadoop/shared/DenseAggregationMapper.java` và `revenue/hadoop/v4/DensePurchaseMapper.java`.

```java
29     dictionary = JsonArtifacts.read(new Path("group-dictionary.json"), FileSystem.getLocal(…), GroupDictionary.class);
34     dictionary.validate(context.getConfiguration().get(RunOptions.FINGERPRINT), mode.name());
35     ids = dictionary.index();
36     sums = new long[dictionary.groups.length];
37     counts = new long[dictionary.groups.length];
44     Integer id = ids.get(result.group());
45     if (id == null) throw new IOException("Group absent from dictionary: " + result.group());
46     sums[id] = Math.addExact(sums[id], result.state().sumMinor());
47     counts[id] = Math.addExact(counts[id], 1);

// DensePurchaseMapper.cleanup
12     for (int id = 0; id < counts.length; id++)
13       if (counts[id] > 0) { key.set(dictionary.groups[id]); value.set(sums[id], counts[id]); context.write(key, value); }
```

- **29–34**: đọc từ điển từ distributed cache. Nếu từ điển không cùng fingerprint input thì dừng, vì không được dùng từ
  điển của dữ liệu khác.
- **36–37**: hai mảng `long`, mỗi danh mục một ô. Không tạo đối tượng mới cho mỗi lần cộng.
- **45**: gặp danh mục không có trong từ điển thì job **thất bại** chứ không bỏ qua dòng.
- **cleanup**: phát mỗi danh mục có dữ liệu đúng một lần.

## 11. V5: gói theo reducer

Files `revenue/hadoop/v5/BatchPurchaseMapper.java`, `BatchPartitioner.java`, `BatchRevenueReducer.java`.

```java
// BatchPurchaseMapper.cleanup
17     int r = context.getNumReduceTasks();
19     for (int id = 0; id < counts.length; id++) if (counts[id] > 0) sizes[id % r]++;
28     for (int id = 0; id < counts.length; id++)
29       if (counts[id] > 0) { int p = id % r, i = sizes[p]++; ids[p][i] = id; totals[p][i] = sums[id]; numbers[p][i] = counts[id]; }
35     for (int p = 0; p < r; p++)
36       if (sizes[p] > 0)
37         context.write(new IntWritable(p), new AggregateBatchWritable(ids[p], totals[p], numbers[p]));

// BatchPartitioner
11     return key.get();

// BatchRevenueReducer.reduce
39     if (key.get() != partition) throw new IOException("Batch routed to wrong reducer");
42         int id = batch.id(i);
43         if (id >= counts.length || id % reducers != partition) throw new IOException("Invalid batch ownership");
45         sums[id] = Math.addExact(sums[id], batch.sum(i));
```

- Danh mục số `id` thuộc reducer `id % R`. Mapper đóng tất cả danh mục của cùng reducer vào **một bản ghi** (dòng
  28–37), nên mỗi mapper phát tối đa R bản ghi.
- Partitioner (dòng 11) gửi gói số `p` tới reducer `p`.
- Reducer kiểm tra gói đến đúng chỗ (dòng 39, 43), cộng vào mảng, rồi ở `cleanup` phát ra từng danh mục thuộc mình.
- Counters D1: `Map output records = 2`, `Reduce input records = 2`, `Reduce output records = 308`, giống V1.

## 12. Spark: cùng bài toán, chạy bằng engine khác

### 12.1 Điểm vào

```powershell
.\scripts\spark-local.ps1 submit revenue --input /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv --tag n1
```

Script gọi `spark-submit --master local[2] --class vn.edu.bigdata.revenue.spark.SparkTool revenue-aggregation-spark.jar revenue …`.

```java
// revenue/spark/SparkTool.java
59     CliArguments a = new CliArguments(Arrays.copyOfRange(args, 1, args.length), FLAGS.get(args[0]));
60     SparkSession spark = SparkSupport.session("ptit-" + args[0]);
63         case "revenue": RevenueJob.run(spark, a); break;

// revenue/spark/SparkSupport.java
45             .setIfMissing("spark.master", "local[2]")
48             .setIfMissing("spark.sql.shuffle.partitions", "8")
50             .setIfMissing("spark.hadoop.fs.defaultFS", hdfsUri())
51             .setIfMissing("spark.hadoop.dfs.client.use.datanode.hostname", "true")
```

- **SparkTool 60**: tạo `SparkSession`. Đây là driver; executor chạy trong cùng JVM vì `local[2]` nghĩa là 2 luồng.
- **SparkSupport 50**: `/data/...` được hiểu là đường dẫn trên `hdfs://localhost:8020`, cùng HDFS mà MapReduce dùng.

### 12.2 `RevenueJob.compute`: map và reduceByKey

File `revenue/spark/RevenueJob.java`.

```java
81     JavaRDD<String> lines = jsc.textFile(SparkSupport.qualify(input));
82     JavaPairRDD<String, long[]> mapped =
83         lines.flatMapToPair(
84             line -> {
85               PreparationResult r = PurchasePreparation.prepare(new CsvEventParser().parse(line), mode);
87               counters.get(r.reason()).add(1);
88               if (!r.valid()) return Collections.emptyIterator();
89               return List.of(new Tuple2<>(r.group(), new long[] {r.state().sumMinor(), 1L})).iterator();
90             });
92     JavaPairRDD<String, long[]> reduced =
93         mapped.reduceByKey(
94             (a, b) -> new long[] {Math.addExact(a[0], b[0]), Math.addExact(a[1], b[1])}, reducers);
95     List<Tuple2<String, long[]>> groups = new ArrayList<>(reduced.collect());
```

- **81**: RDD các dòng; mỗi partition khoảng một block HDFS. Chưa có gì chạy, vì Spark **lười** và chỉ ghi lại kế hoạch.
- **83–89**: đây là **hàm Map**. Nó dùng **lại đúng** `CsvEventParser` và `PurchasePreparation` của MapReduce, nên quy tắc
  lọc giống hệt. Dòng hợp lệ cho một cặp `(category_id, [xu, 1])`; dòng khác cho danh sách rỗng.
- **87**: accumulator đếm theo lý do, tương đương counter của Hadoop.
- **93–94**: `reduceByKey` làm ba việc: gộp các cặp cùng khóa **trong từng partition** trước (giống combiner), ghi ra file
  shuffle, rồi gộp lần cuối ở stage sau (giống reducer). Phép gộp vẫn là cộng từng thành phần như 5.5.
- **95**: `collect()` là *action*: lúc này Spark mới chạy thật. Job có 2 stage, ranh giới là shuffle. Trên Spark UI là
  `flatMapToPair at RevenueJob.java:83` và `collect at RevenueJob.java:95`. Mẫu D2: stage map đọc 541 MiB nhưng chỉ ghi
  40,1 KiB shuffle.
- Sau đó (dòng 98–106) mỗi nhóm thành một hàng `(group_key, sum_minor, count, total, average)`, được ghi Parquet và
  `revenue.csv`, rồi so khớp với output MapReduce.

### 12.3 ETL: CSV thành Parquet đã làm sạch

File `revenue/spark/EventEtlJob.java`.

```java
79   public static Row parse(String line) {
80     String[] f = CsvEventParser.fields(line);
81     if (f == null) return reject("MALFORMED_CSV");
82     if (CsvEventParser.isHeader(f)) return reject("HEADER");
85       time = Timestamp.from(LocalDateTime.parse(f[0], EVENT_TIME).toInstant(ZoneOffset.UTC));
90     if (!EVENT_TYPES.contains(type)) return reject("UNKNOWN_EVENT_TYPE");
95       minor = Money.parseMinor(f[6]);
100    return RowFactory.create(null, time, type, f[2], blankToNull(f[3]), code, code == null ? null : code.split("\\.", 2)[0], …);
115  public static Dataset<Row> parsed(SparkSession spark, String input) {
118        .textFile(SparkSupport.qualify(input))
119        .map((MapFunction<String, Row>) EventEtlJob::parse, Encoders.row(PARSED));
122  public static Dataset<Row> curated(Dataset<Row> parsed) {
124        .filter(col("reject_reason").isNull())
126        .withColumn("event_date", to_date(col("event_time")))
127        .withColumn("event_hour", hour(col("event_time")));
230            curated(parsed).write().mode(SaveMode.ErrorIfExists).partitionBy("event_date").parquet(…);
```

- **79–100**: mỗi dòng thành một hàng có **schema cố định** (12 cột). Dòng lỗi giữ lại kèm `reject_reason` để đếm.
  `category_root` là phần trước dấu chấm của `category_code` (ví dụ `electronics`).
- **119**: áp hàm `parse` cho mọi dòng (pha map của ETL).
- **124–127**: bỏ dòng lỗi, thêm ngày và giờ.
- **230**: ghi Parquet **chia thư mục theo ngày** (`event_date=2019-10-03/…`) và từ chối ghi nếu thư mục đã có. Parquet
  lưu theo cột nên các job sau chỉ đọc đúng cột cần dùng: Group By trên Parquet cả tháng mất 18 giây so với 227 giây
  trên CSV.

### 12.4 Đặc trưng sản phẩm cho ML

File `revenue/spark/ProductFeaturesJob.java`.

```java
41   public static Dataset<Row> productStats(Dataset<Row> events) {
43         .groupBy("product_id")
45             sum(when(col("event_type").equalTo("view"), 1L).otherwise(0L)).as("views"),
46             sum(when(col("event_type").equalTo("cart"), 1L).otherwise(0L)).as("carts"),
47             sum(when(col("event_type").equalTo("purchase"), 1L).otherwise(0L)).as("purchases"),
50             countDistinct(col("user_id")).as("distinct_users"),
51             percentile_approx(col("price").cast("double"), lit(0.5), lit(10000)).as("median_price"),
58   public static Dataset<Row> withFeatures(Dataset<Row> stats, long minViews) {
60         .filter(col("views").geq(minViews))
61         .withColumn("log_views", log1p(col("views")))
64         .withColumn("cart_rate", col("carts").divide(col("views")))
65         .withColumn("purchase_rate", col("purchases").divide(col("views")))
```

- **43–51**: lại là Group By, lần này theo `product_id`. `sum(when(...))` đếm số dòng theo từng loại sự kiện trong một lượt
  quét.
- **60**: chỉ giữ sản phẩm có ít nhất 20 lượt xem, để tỷ lệ có ý nghĩa.
- **61–65**: `log1p(x) = ln(1 + x)` thu hẹp chênh lệch giữa sản phẩm 18 000 lượt xem và 20 lượt xem.
- Sản phẩm 1002099: 18 275 xem, 0 giỏ, 153 mua. Vector là `log1p(18275) = 9,813`, 0, `log1p(153) = 5,037`, 0,
  153/18 275 = 0,0084, `log1p(370,41) = 5,917`, `log1p(11071) = 9,312`.

## 13. Notebook K-Means

File `notebooks/kmeans_product.ipynb`.

```python
# ô 9
2 assembled = VectorAssembler(inputCols=mc.FEATURES, outputCol="raw_features").transform(products)
3 scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True).fit(assembled)
4 scaled = scaler.transform(assembled).cache()
# ô 11
7     for k in range(K_MIN, K_MAX + 1):
8         for seed in SEEDS:
10            model = KMeans(k=k, seed=seed, maxIter=MAX_ITER, initMode="k-means||").fit(scaled)
13            row = {"k": k, "seed": seed, "inertia": model.summary.trainingCost, "silhouette": evaluator.evaluate(pred), …}
# ô 14
1 best_k = int(summary.assign(r=summary.silhouette_mean.round(6)).sort_values(["r"], ascending=False).query("r == r.max()").index.min())
4     model = KMeans(k=best_k, seed=best_seed, maxIter=MAX_ITER, initMode="k-means||").fit(scaled)
```

- **ô 9, dòng 2**: gom 7 cột thành một vector.
- **ô 9, dòng 3**: học trung bình và độ lệch chuẩn của từng cột, rồi chuẩn hóa thành `z = (x − mean)/std`.
- **ô 9, dòng 4**: `cache()` giữ dữ liệu đã chuẩn hóa trong bộ nhớ, vì K-Means lặp nhiều vòng trên cùng dữ liệu.
- **ô 11**: thử K = 2..10, mỗi K 3 seed. Mỗi lần `fit` là một vòng lặp *gán điểm vào tâm gần nhất*, sau đó *tính lại tâm
  bằng trung bình*, cho tới khi hội tụ. `k-means||` là cách chọn tâm ban đầu chạy song song được (Bahmani và cs., 2012).
- **ô 14, dòng 1**: luật chọn K đã cố định trước: silhouette trung bình cao nhất, hòa thì lấy K nhỏ. Kết quả K = 2,
  silhouette 0,649.
- Ô 21 ghi `model.json` gồm `scalerMean`, `scalerStd`, `centers`: đó là tất cả những gì web cần để phân cụm sản phẩm mới.
  Ô 21 dòng 25–30 tự tính lại bằng numpy và yêu cầu trùng Spark 100%.

## 14. Web: từ `model.json` tới câu trả lời "cụm 0"

```java
// webapp/backend/.../serving/ServingRepository.java: verified() tính lại SHA-256 của tệp, so với manifest.json trước khi đọc.
// webapp/backend/.../api/MlController.java
135  @PostMapping("/kmeans/predict")
138    KMeansModel model = registry.kmeans(e);
139    Input input = input(e, req, model.features(), minViews(e));
140    KMeansModel.Prediction p = model.predict(input.vector);
// input(): productId -> lấy 7 đặc trưng đã tính sẵn trong assignments.csv; raw -> tự tính log1p, tỷ lệ (ProductFeatures.transform)

// webapp/backend/.../ml/KMeansModel.java
35   public Prediction predict(double[] x) {
36     double[] z = ProductFeatures.standardize(x, mean, std);
39     for (int c = 0; c < centers.length; c++) {
40       distances[c] = ProductFeatures.squaredDistance(z, centers[c]);
41       if (distances[c] < distances[best]) best = c;

// ProductFeatures
69       z[i] = (x[i] - mean[i]) * scale;          // scale = 1/std
78       double diff = a[j] - b[j]; d += diff * diff;
```

Sản phẩm 1002099 qua đúng các dòng này:

1. `x = [9,813; 0; 5,037; 0; 0,0084; 5,917; 9,312]`.
2. Dòng 69 dùng `mean`/`std` do Spark học được. Ví dụ `log_views`: (9,813 − 4,657)/1,294 = 3,986. Kết quả
   `z = [3,986; −0,307; 4,232; −0,262; 0,065; 1,109; 4,056]`.
3. Dòng 78, khoảng cách bình phương tới hai tâm: [24,09; 60,74].
4. Dòng 41: cụm 0 gần hơn, nên trả về `cluster = 0`. Trên web, `clusterNames()` (`KMeansPage.tsx`) gọi cụm này là
   "nhóm bán chạy (hot)", vì cụm 0 có trung bình lượt xem và tỷ lệ sản phẩm có lượt mua đều cao hơn cụm 1.

Backend còn so với cụm Spark đã gán (`matchesSpark: true`). Nếu Java tính sai, web sẽ báo ngay.

## 15. Tóm tắt một dòng dữ liệu đi qua toàn hệ thống

| Nơi | Dữ liệu |
|---|---|
| `data/raw/2019-Oct.csv` | `2019-10-03 00:24:38 UTC,purchase,1002099,2053013555631882655,…,370.41,…` |
| HDFS `/data/ecommerce/raw/2019-Oct.csv` | cùng dòng, nằm trong 1 trong 43 block 128 MB |
| `CsvEventParser.parse` (dòng 21) | `ParsedEvent("purchase", "2053013555631882655", "electronics.smartphone", "370.41")` |
| `Money.parseMinor` (dòng 13) | `37041` |
| `DirectPurchaseMapper.emit` (dòng 30) | `("2053013555631882655", (37041, 1))` |
| Shuffle | tới reducer `hash % 2` cùng 338 017 cặp khác của danh mục này (cả tháng) |
| `FinalRevenueReducer` (dòng 15) | `2053013555631882655  157049623.37  338018  464.62` |
| Spark `RevenueJob` | cùng con số, khớp từng xu (567/567 nhóm) |
| `ProductFeaturesJob` | 18 275 xem, 153 mua, … |
| Notebook K-Means | cụm 0 |
| `KMeansModel.predict` (web) | cụm 0, `matchesSpark: true`, "nhóm bán chạy (hot)" |

## 16. Câu hỏi hay gặp, trả lời bằng dòng code

| Câu hỏi | Trả lời ngắn, chỉ vào code |
|---|---|
| Key và value của Map là gì? | `DirectPurchaseMapper` dòng 30: key = `category_id` (`Text`), value = `(tổng xu, 1)` (`SumCountWritable`, 16 byte) |
| Dòng `view` đi đâu? | `PurchasePreparation` dòng 18–24: dừng ở đó, chỉ tăng counter `NON_PURCHASE` |
| Vì sao tiền là `long`? | `Money` dòng 11–13: tránh sai số số thực; `Math.addExact` chặn tràn số |
| Combiner có chắc chạy không? | Không. `SumCountCombiner` đúng dù chạy 0, 1 hay nhiều lần, vì phép gộp ở `AggregateState` dòng 22–24 kết hợp và giao hoán |
| Key nào tới reducer nào? | `HashPartitioner`: `"1"` vào reducer 0, `"2"` vào reducer 1 (mục 6); V5 dùng `BatchPartitioner` dòng 11 |
| Làm sao biết không mất dòng? | Counter ở `MapperSupport` dòng 26 và kiểm tra ở `RevenueTool` dòng 119 |
| Spark có dùng Reducer của Hadoop không? | Không. `reduceByKey` (RevenueJob dòng 93) là cơ chế của Spark; chỉ chung mô hình map → shuffle → reduce |
| Chạy lại có ghi đè không? | MR: output phải mới (JobPlanFactory dòng 79). Spark: `SaveMode.ErrorIfExists` (RevenueJob dòng 187, EventEtlJob dòng 232) |
| Web có chạy lại Spark không? | Không. Web đọc `model.json` và tự tính khoảng cách (KMeansModel dòng 35–41) |
