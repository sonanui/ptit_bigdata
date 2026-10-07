package vn.edu.bigdata.revenue.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.domain.Money;
import vn.edu.bigdata.revenue.input.CsvEventParser;
import vn.edu.bigdata.revenue.input.PreparationResult;
import vn.edu.bigdata.revenue.input.PreparationResult.Reason;
import vn.edu.bigdata.revenue.input.PurchasePreparation;

/**
 * A1 trên Spark RDD: doanh thu purchase theo category_id, đối chứng chính xác với Hadoop MR V1.
 *
 * <p>Dùng lại nguyên CsvEventParser + PurchasePreparation + Money của MR nên chính sách lọc trùng
 * khớp theo cấu trúc. Ánh xạ mô hình: flatMapToPair = map (emit category_id -> (sum, 1));
 * reduceByKey gộp phía map trước shuffle (tương tự combiner), shuffle theo HashPartitioner, rồi gộp
 * cuối (tương tự reducer). Đây là cơ chế của Spark, không phải Hadoop Reducer.
 *
 * <p>Thực nghiệm E4 tách tác động của API và định dạng bằng ba chế độ: {@code rdd-raw} (mặc định,
 * đối chứng MR), {@code df-raw} (DataFrame trên CSV, parse như ETL) và {@code df-curated}
 * (DataFrame trên Parquet curated, chỉ đọc 3 cột). Hai chế độ DataFrame dùng chính sách lọc của ETL
 * nên phải được so khớp với {@code rdd-raw}, không mặc định là bằng nhau.
 */
public final class RevenueJob {
  public static final String OUTPUT_ROOT = "/data/ecommerce/agg/revenue_by_category";
  public static final List<String> CSV_HEADER =
      List.of("group_key", "total_revenue", "purchase_count", "average_revenue");
  static final StructType SCHEMA =
      new StructType()
          .add("group_key", DataTypes.StringType, false)
          .add("sum_minor", DataTypes.LongType, false)
          .add("purchase_count", DataTypes.LongType, false)
          .add("total_revenue", DataTypes.StringType, false)
          .add("average_revenue", DataTypes.StringType, false);

  private RevenueJob() {}

  /** Kết quả A1: các nhóm đã sắp theo group_key, số dòng theo lý do, và lineage của RDD. */
  public static final class Result {
    public final List<Row> rows;
    public final Map<String, Long> reasons;
    public final String lineage;

    Result(List<Row> rows, Map<String, Long> reasons, String lineage) {
      this.rows = rows;
      this.reasons = reasons;
      this.lineage = lineage;
    }
  }

  public static Result compute(SparkSession spark, String input, int reducers) {
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    // Accumulator cập nhật trong transformation có thể đếm lặp nếu task chạy lại; job kiểm tra
    // VALID_PURCHASE == tổng purchase_count để phát hiện trường hợp đó.
    EnumMap<Reason, LongAccumulator> counters = new EnumMap<>(Reason.class);
    for (Reason reason : Reason.values())
      counters.put(reason, spark.sparkContext().longAccumulator(reason.name()));
    GroupMode mode = GroupMode.CATEGORY_ID;

    JavaRDD<String> lines = jsc.textFile(SparkSupport.qualify(input));
    JavaPairRDD<String, long[]> mapped =
        lines.flatMapToPair(
            line -> {
              PreparationResult r =
                  PurchasePreparation.prepare(new CsvEventParser().parse(line), mode);
              counters.get(r.reason()).add(1);
              if (!r.valid()) return Collections.emptyIterator();
              return List.of(new Tuple2<>(r.group(), new long[] {r.state().sumMinor(), 1L}))
                  .iterator();
            });
    JavaPairRDD<String, long[]> reduced =
        mapped.reduceByKey(
            (a, b) -> new long[] {Math.addExact(a[0], b[0]), Math.addExact(a[1], b[1])}, reducers);
    List<Tuple2<String, long[]>> groups = new ArrayList<>(reduced.collect());
    groups.sort((x, y) -> x._1().compareTo(y._1()));

    List<Row> rows = new ArrayList<>();
    for (Tuple2<String, long[]> g : groups)
      rows.add(
          RowFactory.create(
              g._1(),
              g._2()[0],
              g._2()[1],
              Money.formatMinor(g._2()[0]),
              Money.average(g._2()[0], g._2()[1])));
    Map<String, Long> reasons = new LinkedHashMap<>();
    for (Reason reason : Reason.values()) {
      long value = counters.get(reason).value();
      if (value > 0) reasons.put(reason.name(), value);
    }
    return new Result(rows, reasons, reduced.toDebugString());
  }

  /** A1 bằng DataFrame: purchase có category_id, groupBy category_id, sum(price_minor), count. */
  public static Result computeFrame(Dataset<Row> events) {
    Dataset<Row> grouped =
        events
            .filter(col("event_type").equalTo("purchase").and(col("category_id").isNotNull()))
            .groupBy("category_id")
            .agg(sum("price_minor").as("sum_minor"), count("*").as("purchase_count"));
    List<Row> collected = new ArrayList<>(grouped.collectAsList());
    collected.sort((x, y) -> x.getString(0).compareTo(y.getString(0)));
    List<Row> rows = new ArrayList<>();
    for (Row g : collected)
      rows.add(
          RowFactory.create(
              g.getString(0),
              g.getLong(1),
              g.getLong(2),
              Money.formatMinor(g.getLong(1)),
              Money.average(g.getLong(1), g.getLong(2))));
    return new Result(
        rows, new LinkedHashMap<>(), grouped.queryExecution().executedPlan().treeString());
  }

  public static List<List<Object>> csvRows(List<Row> rows) {
    List<List<Object>> out = new ArrayList<>();
    for (Row r : rows)
      out.add(List.of(r.getString(0), r.getString(3), r.getLong(2), r.getString(4)));
    return out;
  }

  /**
   * --input /data/... --tag d1 [--mode rdd-raw|df-raw|df-curated --curated-run-id ID --reducers 2
   * --run-id ID]
   */
  static void run(SparkSession spark, vn.edu.bigdata.revenue.cli.CliArguments a) throws Exception {
    String mode = a.get("mode", "rdd-raw");
    if (!List.of("rdd-raw", "df-raw", "df-curated").contains(mode))
      throw new IllegalArgumentException("mode must be rdd-raw, df-raw or df-curated");
    String input =
        mode.equals("df-curated")
            ? EventEtlJob.OUTPUT_ROOT + "/run_id=" + a.required("curated-run-id")
            : a.required("input");
    String runId = a.get("run-id", SparkSupport.runId(a.required("tag")));
    int reducers = a.integer("reducers", 2);
    String out = OUTPUT_ROOT + "/run_id=" + runId;
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("mode", mode);
    params.put("input", input);
    params.put("reducers", reducers);
    params.put("output", out);
    SparkSupport.RunRecord run = new SparkSupport.RunRecord(spark, "revenue", runId, params);
    try {
      SparkSupport.requireNew(spark, out);
      Result result;
      if (mode.equals("rdd-raw"))
        result = run.stage("map_reduceByKey", () -> compute(spark, input, reducers));
      else if (mode.equals("df-raw"))
        result =
            run.stage(
                "df_groupBy",
                () -> computeFrame(EventEtlJob.curated(EventEtlJob.parsed(spark, input))));
      else
        result =
            run.stage(
                "df_groupBy",
                () -> computeFrame(spark.read().parquet(SparkSupport.qualify(input))));
      run.stage(
          "write_parquet",
          () -> {
            spark
                .createDataFrame(result.rows, SCHEMA)
                .coalesce(1)
                .write()
                .mode(SaveMode.ErrorIfExists)
                .parquet(SparkSupport.qualify(out));
            return null;
          });
      long counted = result.rows.stream().mapToLong(r -> r.getLong(2)).sum();
      // Chế độ DataFrame không có accumulator theo lý do; chỉ rdd-raw kiểm tra chéo được.
      long valid = result.reasons.getOrDefault(Reason.VALID_PURCHASE.name(), counted);
      if (valid != counted)
        throw new IllegalStateException("VALID_PURCHASE " + valid + " != sum count " + counted);
      SparkSupport.writeCsv(run.localDir.resolve("revenue.csv"), CSV_HEADER, csvRows(result.rows));
      java.nio.file.Files.writeString(
          run.localDir.resolve(
              mode.equals("rdd-raw") ? "revenue-rdd-lineage.txt" : "revenue-df-plan.txt"),
          result.lineage);
      if (mode.equals("rdd-raw")) {
        run.metrics.put(
            "rowsIn", result.reasons.values().stream().mapToLong(Long::longValue).sum());
        run.metrics.put("reasons", result.reasons);
      }
      run.metrics.put("groups", result.rows.size());
      run.metrics.put("validPurchases", valid);
      run.finish(out + "/_run.json", null);
      System.out.println(
          "A1 xong: " + runId + " groups=" + result.rows.size() + " valid=" + valid + " -> " + out);
    } catch (Exception e) {
      run.finish(null, e);
      throw e;
    }
  }
}
