package vn.edu.bigdata.revenue.spark;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import vn.edu.bigdata.revenue.cli.CliArguments;
import vn.edu.bigdata.revenue.domain.Money;
import vn.edu.bigdata.revenue.input.CsvEventParser;

/**
 * T2.2-T2.3: raw CSV -> curated Parquet (phân vùng event_date) + báo cáo chất lượng dữ liệu.
 *
 * <p>Cấu trúc dòng dùng CsvEventParser.fields (cùng quy tắc với MR), giá dùng Money.parseMinor. Một
 * dòng bị loại khỏi curated khi: HEADER, MALFORMED_CSV, INVALID_EVENT_TIME, UNKNOWN_EVENT_TYPE,
 * MISSING_PRODUCT_ID, MISSING_USER_ID, INVALID_PRICE (giá kiểm tra cho mọi event_type).
 * category_code/brand/user_session rỗng không bị loại; chúng thành NULL và được đếm trong báo cáo.
 */
public final class EventEtlJob {
  public static final String OUTPUT_ROOT = "/data/ecommerce/curated/events";
  static final Set<String> EVENT_TYPES = Set.of("view", "cart", "remove_from_cart", "purchase");
  private static final DateTimeFormatter EVENT_TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'");
  static final StructType PARSED =
      new StructType()
          .add("reject_reason", DataTypes.StringType, true)
          .add("event_time", DataTypes.TimestampType, true)
          .add("event_type", DataTypes.StringType, true)
          .add("product_id", DataTypes.StringType, true)
          .add("category_id", DataTypes.StringType, true)
          .add("category_code", DataTypes.StringType, true)
          .add("category_root", DataTypes.StringType, true)
          .add("brand", DataTypes.StringType, true)
          .add("price", DataTypes.createDecimalType(12, 2), true)
          .add("price_minor", DataTypes.LongType, true)
          .add("user_id", DataTypes.StringType, true)
          .add("user_session", DataTypes.StringType, true);

  private EventEtlJob() {}

  private static Row reject(String reason) {
    Object[] values = new Object[PARSED.size()];
    values[0] = reason;
    return RowFactory.create(values);
  }

  private static String blankToNull(String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  /** Pha "map" của ETL: một dòng vật lý -> một bản ghi đã chuẩn hóa hoặc một lý do loại. */
  public static Row parse(String line) {
    String[] f = CsvEventParser.fields(line);
    if (f == null) return reject("MALFORMED_CSV");
    if (CsvEventParser.isHeader(f)) return reject("HEADER");
    Timestamp time;
    try {
      time = Timestamp.from(LocalDateTime.parse(f[0], EVENT_TIME).toInstant(ZoneOffset.UTC));
    } catch (DateTimeParseException e) {
      return reject("INVALID_EVENT_TIME");
    }
    String type = f[1].trim();
    if (!EVENT_TYPES.contains(type)) return reject("UNKNOWN_EVENT_TYPE");
    if (f[2].isEmpty()) return reject("MISSING_PRODUCT_ID");
    if (f[7].isEmpty()) return reject("MISSING_USER_ID");
    long minor;
    try {
      minor = Money.parseMinor(f[6]);
    } catch (IllegalArgumentException e) {
      return reject("INVALID_PRICE");
    }
    String code = blankToNull(f[4]);
    return RowFactory.create(
        null,
        time,
        type,
        f[2],
        blankToNull(f[3]),
        code,
        code == null ? null : code.split("\\.", 2)[0],
        blankToNull(f[5]),
        BigDecimal.valueOf(minor, 2),
        minor,
        f[7],
        blankToNull(f[8]));
  }

  public static Dataset<Row> parsed(SparkSession spark, String input) {
    return spark
        .read()
        .textFile(SparkSupport.qualify(input))
        .map((MapFunction<String, Row>) EventEtlJob::parse, Encoders.row(PARSED));
  }

  public static Dataset<Row> curated(Dataset<Row> parsed) {
    return parsed
        .filter(col("reject_reason").isNull())
        .drop("reject_reason")
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("event_hour", hour(col("event_time")));
  }

  public static Map<String, Long> reasons(Dataset<Row> parsed) {
    Map<String, Long> out = new LinkedHashMap<>();
    for (Row r :
        parsed
            .groupBy(coalesce(col("reject_reason"), lit("VALID")).as("r"))
            .count()
            .orderBy("r")
            .collectAsList()) out.put(r.getString(0), r.getLong(1));
    return out;
  }

  private static Column nulls(String column) {
    return sum(when(col(column).isNull(), 1).otherwise(0)).as("null_" + column);
  }

  /** Thống kê chất lượng trên curated đã ghi (đọc cột Parquet, không quét lại CSV). */
  public static Map<String, Object> quality(Dataset<Row> events, boolean duplicates) {
    Map<String, Object> report = new LinkedHashMap<>();
    Row s =
        events
            .agg(
                count(lit(1)).as("rows"),
                min("event_time").as("min_event_time"),
                max("event_time").as("max_event_time"),
                nulls("category_id"),
                nulls("category_code"),
                nulls("brand"),
                nulls("user_session"),
                sum(when(col("price_minor").equalTo(0), 1).otherwise(0)).as("zero_price"),
                countDistinct(col("product_id")).as("distinct_products"),
                countDistinct(col("category_id")).as("distinct_category_id"),
                countDistinct(col("category_code")).as("distinct_category_code"),
                countDistinct(col("brand")).as("distinct_brands"),
                countDistinct(col("user_id")).as("distinct_users"),
                countDistinct(col("user_session")).as("distinct_sessions"))
            .first();
    for (String field : s.schema().fieldNames()) {
      Object v = s.getAs(field);
      report.put(field, v instanceof Timestamp ? ((Timestamp) v).toInstant().toString() : v);
    }
    Map<String, Long> types = new LinkedHashMap<>();
    for (Row r : events.groupBy("event_type").count().orderBy("event_type").collectAsList())
      types.put(r.getString(0), r.getLong(1));
    report.put("eventTypes", types);
    report.put(
        "purchasesWithZeroPrice",
        events
            .filter(col("event_type").equalTo("purchase").and(col("price_minor").equalTo(0)))
            .count());
    report.put(
        "categoryIdsWithMultipleCodes",
        events
            .groupBy("category_id")
            .agg(countDistinct(col("category_code")).as("n"))
            .filter(col("n").gt(1))
            .count());
    if (duplicates) {
      // Trùng hoàn toàn trên các trường đã chuẩn hóa; curated vẫn giữ nguyên (no-dedup như MR).
      Row d =
          events
              .groupBy(
                  "event_time",
                  "event_type",
                  "product_id",
                  "category_id",
                  "category_code",
                  "brand",
                  "price_minor",
                  "user_id",
                  "user_session")
              .count()
              .filter(col("count").gt(1))
              .agg(
                  coalesce(sum(col("count").minus(1)), lit(0L)).as("extra"),
                  count(lit(1)).as("groups"))
              .first();
      report.put("exactDuplicateExtraRows", d.getLong(0));
      report.put("exactDuplicateGroups", d.getLong(1));
    }
    return report;
  }

  /** --input /data/... --tag d1 [--duplicates true --run-id ID] */
  static void run(SparkSession spark, CliArguments a) throws Exception {
    String input = a.required("input");
    String runId = a.get("run-id", SparkSupport.runId(a.required("tag")));
    boolean duplicates = Boolean.parseBoolean(a.get("duplicates", "false"));
    String out = OUTPUT_ROOT + "/run_id=" + runId;
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("input", input);
    params.put("duplicates", duplicates);
    params.put("output", out);
    SparkSupport.RunRecord run = new SparkSupport.RunRecord(spark, "etl", runId, params);
    try {
      SparkSupport.requireNew(spark, out);
      Dataset<Row> parsed = parsed(spark, input);
      run.stage(
          "write_curated_parquet",
          () -> {
            // Không persist: file nguồn đã theo thứ tự thời gian nên mỗi split chỉ chạm vài ngày.
            curated(parsed)
                .write()
                .mode(SaveMode.ErrorIfExists)
                .partitionBy("event_date")
                .parquet(SparkSupport.qualify(out));
            return null;
          });
      Map<String, Long> reasons = run.stage("count_reasons", () -> reasons(parsed));
      Dataset<Row> written = spark.read().parquet(SparkSupport.qualify(out));
      long rowsIn = reasons.values().stream().mapToLong(Long::longValue).sum();
      long valid = reasons.getOrDefault("VALID", 0L);
      long readBack = run.stage("verify_readback", written::count);
      if (readBack != valid)
        throw new IllegalStateException("Parquet has " + readBack + " rows, expected " + valid);
      Map<String, Object> quality = run.stage("quality", () -> quality(written, duplicates));
      Map<String, Long> rejected = new LinkedHashMap<>(reasons);
      rejected.remove("VALID");
      run.metrics.put("rowsIn", rowsIn);
      run.metrics.put("rowsValid", valid);
      run.metrics.put("rejected", rejected);
      run.metrics.put("rowsWritten", readBack);
      run.metrics.put("quality", quality);
      SparkSupport.writeJson(run.localDir.resolve("quality.json"), run.metrics);
      run.finish(out + "/_run.json", null);
      System.out.println(
          "ETL xong: "
              + runId
              + " in="
              + rowsIn
              + " valid="
              + valid
              + " rejected="
              + rejected
              + " -> "
              + out);
    } catch (Exception e) {
      run.finish(null, e);
      throw e;
    }
  }
}
