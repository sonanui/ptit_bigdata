package vn.edu.bigdata.revenue.spark;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import vn.edu.bigdata.revenue.cli.CliArguments;
import vn.edu.bigdata.revenue.domain.Money;

/**
 * T2.5: Group By A2-A5 trên curated Parquet.
 *
 * <ul>
 *   <li>A2 funnel_by_category: khóa category_id; số view/cart/remove_from_cart/purchase, doanh thu.
 *   <li>A3 conversion (cột trong A2, A5): purchase/view, purchase/cart theo SỰ KIỆN, không theo
 *       phiên; mẫu số 0 thì NULL.
 *   <li>A4 trend_by_hour: khóa (event_date, event_hour) UTC.
 *   <li>A5 funnel_by_brand: khóa brand, NULL -> "__UNKNOWN__".
 * </ul>
 *
 * Kiểm tra: tổng events mỗi bảng = số dòng curated; khi có --revenue-run-id, tổng purchase của A2
 * trên category_id hợp lệ ([0-9]+) = tổng purchase_count của A1.
 */
public final class MetricsJob {
  public static final String AGG_ROOT = "/data/ecommerce/agg";

  private MetricsJob() {}

  private static Column isType(String type) {
    return when(col("event_type").equalTo(type), 1L).otherwise(0L);
  }

  /** Các hàm tổng hợp có tính kết hợp, nên Spark gộp được một phần trước shuffle. */
  static Column[] counts() {
    return new Column[] {
      count(lit(1)).as("events"),
      sum(isType("view")).as("views"),
      sum(isType("cart")).as("carts"),
      sum(isType("remove_from_cart")).as("removes"),
      sum(isType("purchase")).as("purchases"),
      sum(when(col("event_type").equalTo("purchase"), col("price_minor")).otherwise(0L))
          .as("revenue_minor")
    };
  }

  static Dataset<Row> withConversion(Dataset<Row> df) {
    return df.withColumn(
            "view_to_purchase",
            when(col("views").gt(0), round(col("purchases").divide(col("views")), 6)))
        .withColumn(
            "cart_to_purchase",
            when(col("carts").gt(0), round(col("purchases").divide(col("carts")), 6)));
  }

  public static Dataset<Row> funnelByCategory(Dataset<Row> events) {
    Column[] aggs =
        append(
            counts(),
            max("category_code").as("category_code"),
            countDistinct(col("category_code")).as("distinct_codes"));
    return withConversion(events.groupBy("category_id").agg(aggs[0], tail(aggs)));
  }

  public static Dataset<Row> trendByHour(Dataset<Row> events) {
    Column[] aggs = append(counts(), countDistinct(col("user_id")).as("distinct_users"));
    return events.groupBy("event_date", "event_hour").agg(aggs[0], tail(aggs));
  }

  public static Dataset<Row> funnelByBrand(Dataset<Row> events) {
    Column[] aggs = counts();
    return withConversion(
        events
            .withColumn("brand", coalesce(col("brand"), lit("__UNKNOWN__")))
            .groupBy("brand")
            .agg(aggs[0], tail(aggs)));
  }

  private static Column[] append(Column[] base, Column... extra) {
    Column[] out = java.util.Arrays.copyOf(base, base.length + extra.length);
    System.arraycopy(extra, 0, out, base.length, extra.length);
    return out;
  }

  private static Column[] tail(Column[] columns) {
    return java.util.Arrays.copyOfRange(columns, 1, columns.length);
  }

  /** Bảng nhỏ đã tổng hợp -> CSV cục bộ; cột *_minor được kèm dạng tiền định dạng như MR. */
  static void exportCsv(Dataset<Row> table, String[] order, java.nio.file.Path file)
      throws java.io.IOException {
    List<String> header = new ArrayList<>(List.of(table.columns()));
    boolean money = header.contains("revenue_minor");
    if (money) header.add(header.indexOf("revenue_minor") + 1, "revenue");
    List<List<Object>> rows = new ArrayList<>();
    for (Row r :
        table
            .orderBy(order[0], java.util.Arrays.copyOfRange(order, 1, order.length))
            .collectAsList()) {
      List<Object> values = new ArrayList<>();
      for (String c : table.columns()) {
        Object v = r.getAs(c);
        values.add(v == null ? "" : v);
        if (money && c.equals("revenue_minor")) values.add(Money.formatMinor((Long) v));
      }
      rows.add(values);
    }
    SparkSupport.writeCsv(file, header, rows);
  }

  /** --curated-run-id ID [--revenue-run-id A1_ID --run-id ID] */
  static void run(SparkSession spark, CliArguments a) throws Exception {
    String curatedRun = a.required("curated-run-id");
    String revenueRun = a.get("revenue-run-id", null);
    String tag = curatedRun.substring(curatedRun.lastIndexOf('-') + 1);
    String runId = a.get("run-id", SparkSupport.runId(tag));
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("curatedRunId", curatedRun);
    params.put("revenueRunId", revenueRun);
    SparkSupport.RunRecord run = new SparkSupport.RunRecord(spark, "metrics", runId, params);
    try {
      Dataset<Row> events =
          spark
              .read()
              .parquet(SparkSupport.qualify(EventEtlJob.OUTPUT_ROOT + "/run_id=" + curatedRun))
              .cache();
      long total = events.count();
      Map<String, Object> tables = new LinkedHashMap<>();
      Object[][] specs = {
        {"funnel_by_category", funnelByCategory(events), new String[] {"category_id"}},
        {"trend_by_hour", trendByHour(events), new String[] {"event_date", "event_hour"}},
        {"funnel_by_brand", funnelByBrand(events), new String[] {"brand"}}
      };
      for (Object[] spec : specs) {
        String name = (String) spec[0];
        @SuppressWarnings("unchecked")
        Dataset<Row> table = (Dataset<Row>) spec[1];
        String out = AGG_ROOT + "/" + name + "/run_id=" + runId;
        SparkSupport.requireNew(spark, out);
        Map<String, Object> m =
            run.stage(
                name,
                () -> {
                  table.write().mode(SaveMode.ErrorIfExists).parquet(SparkSupport.qualify(out));
                  Dataset<Row> written = spark.read().parquet(SparkSupport.qualify(out));
                  exportCsv(written, (String[]) spec[2], run.localDir.resolve(name + ".csv"));
                  Row t = written.agg(count(lit(1)), sum("events"), sum("purchases")).first();
                  Map<String, Object> stats = new LinkedHashMap<>();
                  stats.put("rows", t.getLong(0));
                  stats.put("events", t.getLong(1));
                  stats.put("purchases", t.getLong(2));
                  return stats;
                });
        if ((Long) m.get("events") != total)
          throw new IllegalStateException(
              name + " events " + m.get("events") + " != curated " + total);
        tables.put(name, m);
      }
      Map<String, Object> checks = new LinkedHashMap<>();
      checks.put("curatedRows", total);
      if (revenueRun != null) {
        long a1 =
            spark
                .read()
                .parquet(SparkSupport.qualify(RevenueJob.OUTPUT_ROOT + "/run_id=" + revenueRun))
                .agg(sum("purchase_count"))
                .first()
                .getLong(0);
        long a2 =
            events
                .filter(
                    col("event_type").equalTo("purchase").and(col("category_id").rlike("^[0-9]+$")))
                .count();
        checks.put("a1PurchaseCount", a1);
        checks.put("a2PurchasesValidCategory", a2);
        checks.put("a1EqualsA2", a1 == a2);
        if (a1 != a2)
          throw new IllegalStateException("A1/A2 cross-check failed: " + a1 + " != " + a2);
      }
      run.metrics.put("tables", tables);
      run.metrics.put("checks", checks);
      run.finish(AGG_ROOT + "/funnel_by_category/run_id=" + runId + "/_run.json", null);
      System.out.println("A2-A5 xong: " + runId + " " + tables + " " + checks);
    } catch (Exception e) {
      run.finish(null, e);
      throw e;
    }
  }
}
