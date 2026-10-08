package vn.edu.bigdata.revenue.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.log1p;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.percentile_approx;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import vn.edu.bigdata.revenue.cli.CliArguments;

/**
 * T4.1 (A7): Group By product_id trên curated -> đặc trưng hành vi cho K-Means/KNN (notebook).
 *
 * <p>1 dòng = 1 product_id trong cửa sổ thời gian của curated run. Không dùng category làm đặc
 * trưng (để diễn giải cụm và đánh giá KNN độc lập). Lọc views >= min-views để các tỷ lệ ổn định.
 */
public final class ProductFeaturesJob {
  public static final String OUTPUT_ROOT = "/data/ecommerce/features/product";
  public static final List<String> FEATURES =
      List.of(
          "log_views",
          "log_carts",
          "log_purchases",
          "cart_rate",
          "purchase_rate",
          "log_median_price",
          "log_distinct_users");

  private ProductFeaturesJob() {}

  public static Dataset<Row> productStats(Dataset<Row> events) {
    return events
        .groupBy("product_id")
        .agg(
            sum(when(col("event_type").equalTo("view"), 1L).otherwise(0L)).as("views"),
            sum(when(col("event_type").equalTo("cart"), 1L).otherwise(0L)).as("carts"),
            sum(when(col("event_type").equalTo("purchase"), 1L).otherwise(0L)).as("purchases"),
            sum(when(col("event_type").equalTo("purchase"), col("price_minor")).otherwise(0L))
                .as("revenue_minor"),
            countDistinct(col("user_id")).as("distinct_users"),
            percentile_approx(col("price").cast("double"), lit(0.5), lit(10000)).as("median_price"),
            max("category_id").as("category_id"),
            max("category_code").as("category_code"),
            max("category_root").as("category_root"),
            max("brand").as("brand"));
  }

  public static Dataset<Row> withFeatures(Dataset<Row> stats, long minViews) {
    return stats
        .filter(col("views").geq(minViews))
        .withColumn("log_views", log1p(col("views")))
        .withColumn("log_carts", log1p(col("carts")))
        .withColumn("log_purchases", log1p(col("purchases")))
        .withColumn("cart_rate", col("carts").divide(col("views")))
        .withColumn("purchase_rate", col("purchases").divide(col("views")))
        .withColumn("log_median_price", log1p(col("median_price")))
        .withColumn("log_distinct_users", log1p(col("distinct_users")));
  }

  /** --curated-run-id ID [--min-views 20 --run-id ID] */
  static void run(SparkSession spark, CliArguments a) throws Exception {
    String curatedRun = a.required("curated-run-id");
    long minViews = a.number("min-views", 20);
    String tag = curatedRun.substring(curatedRun.lastIndexOf('-') + 1);
    String runId = a.get("run-id", SparkSupport.runId(tag));
    String out = OUTPUT_ROOT + "/run_id=" + runId;
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("curatedRunId", curatedRun);
    params.put("minViews", minViews);
    params.put("output", out);
    SparkSupport.RunRecord run = new SparkSupport.RunRecord(spark, "features", runId, params);
    try {
      SparkSupport.requireNew(spark, out);
      Dataset<Row> events =
          spark
              .read()
              .parquet(SparkSupport.qualify(EventEtlJob.OUTPUT_ROOT + "/run_id=" + curatedRun));
      Dataset<Row> stats = productStats(events).cache();
      long products = run.stage("product_stats", stats::count);
      double[] quantiles =
          stats.stat().approxQuantile("views", new double[] {0.5, 0.75, 0.9, 0.95, 0.99}, 0.001);
      run.stage(
          "write_features",
          () -> {
            withFeatures(stats, minViews)
                .write()
                .mode(SaveMode.ErrorIfExists)
                .parquet(SparkSupport.qualify(out));
            return null;
          });
      Dataset<Row> written = spark.read().parquet(SparkSupport.qualify(out));
      long kept = written.count();
      Map<String, Object> describe = new LinkedHashMap<>();
      for (Row r : written.describe(FEATURES.toArray(new String[0])).collectAsList()) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (String f : FEATURES) values.put(f, r.getAs(f));
        describe.put(r.getString(0), values);
      }
      run.metrics.put("products", products);
      run.metrics.put("minViews", minViews);
      run.metrics.put("kept", kept);
      run.metrics.put("excluded", products - kept);
      run.metrics.put("viewsQuantiles_p50_p75_p90_p95_p99", quantiles);
      run.metrics.put(
          "keptWithCategoryCode", written.filter(col("category_code").isNotNull()).count());
      run.metrics.put("keptWithPurchase", written.filter(col("purchases").gt(0)).count());
      run.metrics.put("features", FEATURES);
      run.metrics.put("describe", describe);
      SparkSupport.writeJson(run.localDir.resolve("features-summary.json"), run.metrics);
      run.finish(out + "/_run.json", null);
      System.out.println(
          "Features xong: "
              + runId
              + " products="
              + products
              + " kept="
              + kept
              + " (views>="
              + minViews
              + ")");
    } catch (Exception e) {
      run.finish(null, e);
      throw e;
    }
  }
}
