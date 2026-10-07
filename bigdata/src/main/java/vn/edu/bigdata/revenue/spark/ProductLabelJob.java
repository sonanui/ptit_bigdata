package vn.edu.bigdata.revenue.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import vn.edu.bigdata.revenue.cli.CliArguments;

/**
 * A8: đặc trưng sản phẩm trước mốc t0 + nhãn "có purchase trong [t0, t0 + labelDays)" cho KNN phân
 * loại (notebook).
 *
 * <p>1 dòng = 1 product_id tại một ảnh chụp (split). Đặc trưng chỉ dùng sự kiện trong [t0 -
 * featureDays, t0); nhãn chỉ dùng purchase trong [t0, t0 + labelDays). Hai ảnh chụp train/test chia
 * theo thời gian: cửa sổ nhãn của train phải kết thúc trước t0 của test, nên mô hình không thấy bất
 * kỳ sự kiện nào của giai đoạn test. Job ghi lại kiểm tra rò rỉ (max event_time của lịch sử < t0)
 * và độ phủ dữ liệu (curated phải chứa trọn cửa sổ nhãn, nếu không nhãn 0 sẽ sai).
 */
public final class ProductLabelJob {
  public static final String OUTPUT_ROOT = "/data/ecommerce/features/product_label";
  public static final List<String> FEATURES = features();

  private ProductLabelJob() {}

  private static List<String> features() {
    List<String> out = new ArrayList<>(ProductFeaturesJob.FEATURES);
    out.add("recent_view_share");
    return List.copyOf(out);
  }

  static Timestamp at(LocalDate day) {
    return Timestamp.from(day.atStartOfDay(ZoneOffset.UTC).toInstant());
  }

  private static Dataset<Row> between(Dataset<Row> events, LocalDate from, LocalDate until) {
    return events.filter(
        col("event_time").geq(lit(at(from))).and(col("event_time").lt(lit(at(until)))));
  }

  /** Ảnh chụp tại t0: A7 trên lịch sử + recent_view_share (tỷ lệ view trong labelDays cuối). */
  public static Dataset<Row> snapshot(
      Dataset<Row> events,
      LocalDate t0,
      int featureDays,
      int labelDays,
      long minViews,
      String split) {
    Dataset<Row> history = between(events, t0.minusDays(featureDays), t0);
    Dataset<Row> recent =
        between(events, t0.minusDays(labelDays), t0)
            .filter(col("event_type").equalTo("view"))
            .groupBy("product_id")
            .agg(count(lit(1)).as("recent_views"));
    Dataset<Row> stats =
        ProductFeaturesJob.productStats(history)
            .join(recent, "product_id", "left")
            .na()
            .fill(0L, new String[] {"recent_views"});
    Dataset<Row> future =
        between(events, t0, t0.plusDays(labelDays))
            .groupBy("product_id")
            .agg(
                sum(when(col("event_type").equalTo("purchase"), 1L).otherwise(0L))
                    .as("future_purchases"));
    return ProductFeaturesJob.withFeatures(stats, minViews)
        .withColumn("recent_view_share", col("recent_views").divide(col("views")))
        .join(future, "product_id", "left")
        .na()
        .fill(0L, new String[] {"future_purchases"})
        .withColumn("label", when(col("future_purchases").gt(0), 1).otherwise(0))
        .withColumn("split", lit(split))
        .withColumn("t0", lit(t0.toString()));
  }

  /** Kiểm tra rò rỉ và độ phủ cho một ảnh chụp; ném lỗi nếu vi phạm. */
  static Map<String, Object> checks(
      Dataset<Row> events, LocalDate t0, int featureDays, int labelDays) {
    Row h =
        between(events, t0.minusDays(featureDays), t0)
            .agg(min("event_time"), max("event_time"))
            .first();
    Row f =
        between(events, t0, t0.plusDays(labelDays))
            .agg(min("event_time"), max("event_time"))
            .first();
    if (h.isNullAt(1) || f.isNullAt(0))
      throw new IllegalStateException(
          "Không có sự kiện trong cửa sổ lịch sử hoặc cửa sổ nhãn tại t0=" + t0);
    Timestamp historyMax = h.getTimestamp(1),
        futureMin = f.getTimestamp(0),
        futureMax = f.getTimestamp(1);
    if (!historyMax.before(at(t0)) || futureMin.before(at(t0)))
      throw new IllegalStateException("Rò rỉ thời gian tại t0=" + t0);
    // Cửa sổ nhãn phải được dữ liệu phủ tới ngày cuối, nếu không sản phẩm sẽ bị gán nhãn 0 sai.
    if (futureMax.before(at(t0.plusDays(labelDays - 1))))
      throw new IllegalStateException(
          "Dữ liệu chỉ tới "
              + futureMax
              + ", không phủ cửa sổ nhãn ["
              + t0
              + ", +"
              + labelDays
              + "d)");
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("t0", t0.toString());
    out.put("historyMinEventTime", h.getTimestamp(0).toInstant().toString());
    out.put("historyMaxEventTime", historyMax.toInstant().toString());
    out.put("labelMinEventTime", futureMin.toInstant().toString());
    out.put("labelMaxEventTime", futureMax.toInstant().toString());
    out.put("historyBeforeT0", true);
    return out;
  }

  /**
   * --curated-run-id ID --train-t0 2019-10-15 --test-t0 2019-10-25 [--feature-days 14 --label-days
   * 7 --min-views 20 --run-id ID]
   */
  static void run(SparkSession spark, CliArguments a) throws Exception {
    String curatedRun = a.required("curated-run-id");
    LocalDate trainT0 = LocalDate.parse(a.required("train-t0"));
    LocalDate testT0 = LocalDate.parse(a.required("test-t0"));
    int featureDays = a.integer("feature-days", 14);
    int labelDays = a.integer("label-days", 7);
    long minViews = a.number("min-views", 20);
    if (testT0.isBefore(trainT0.plusDays(labelDays)))
      throw new IllegalArgumentException(
          "test-t0 phải >= train-t0 + label-days (tránh chồng cửa sổ nhãn)");
    String tag = curatedRun.substring(curatedRun.lastIndexOf('-') + 1);
    String runId = a.get("run-id", SparkSupport.runId(tag));
    String out = OUTPUT_ROOT + "/run_id=" + runId;
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("curatedRunId", curatedRun);
    params.put("trainT0", trainT0.toString());
    params.put("testT0", testT0.toString());
    params.put("featureDays", featureDays);
    params.put("labelDays", labelDays);
    params.put("minViews", minViews);
    params.put("output", out);
    SparkSupport.RunRecord run = new SparkSupport.RunRecord(spark, "labels", runId, params);
    try {
      SparkSupport.requireNew(spark, out);
      Dataset<Row> events =
          spark
              .read()
              .parquet(SparkSupport.qualify(EventEtlJob.OUTPUT_ROOT + "/run_id=" + curatedRun));
      Map<String, Object> leakage = new LinkedHashMap<>();
      leakage.put(
          "train", run.stage("check_train", () -> checks(events, trainT0, featureDays, labelDays)));
      leakage.put(
          "test", run.stage("check_test", () -> checks(events, testT0, featureDays, labelDays)));
      run.stage(
          "write_labels",
          () -> {
            snapshot(events, trainT0, featureDays, labelDays, minViews, "train")
                .unionByName(snapshot(events, testT0, featureDays, labelDays, minViews, "test"))
                .write()
                .partitionBy("split")
                .mode(SaveMode.ErrorIfExists)
                .parquet(SparkSupport.qualify(out));
            return null;
          });
      Dataset<Row> written = spark.read().parquet(SparkSupport.qualify(out));
      Map<String, Object> splits = new LinkedHashMap<>();
      for (Row r :
          written
              .groupBy("split")
              .agg(count(lit(1)).as("rows"), sum("label").as("positives"))
              .orderBy("split")
              .collectAsList()) {
        Map<String, Object> s = new LinkedHashMap<>();
        long rows = r.getLong(1), positives = r.getLong(2);
        s.put("rows", rows);
        s.put("positives", positives);
        s.put("positiveRate", rows == 0 ? null : (double) positives / rows);
        splits.put(r.getString(0), s);
      }
      run.metrics.put("features", FEATURES);
      run.metrics.put("label", "future_purchases > 0 in [t0, t0 + labelDays)");
      run.metrics.put("splits", splits);
      run.metrics.put("leakageChecks", leakage);
      SparkSupport.writeJson(run.localDir.resolve("labels-summary.json"), run.metrics);
      run.finish(out + "/_run.json", null);
      System.out.println("Labels xong: " + runId + " " + splits);
    } catch (Exception e) {
      run.finish(null, e);
      throw e;
    }
  }
}
