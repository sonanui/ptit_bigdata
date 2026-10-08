package vn.edu.bigdata.revenue.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.edu.bigdata.revenue.input.CsvEventParser;

/**
 * Spark local trên file cục bộ: A1 khớp oracle tính tay của MR, ETL bảo toàn số dòng, A2-A5, A7.
 */
class SparkJobsTest {
  private static final Path FIXTURES = Paths.get("src", "test", "resources", "fixtures");
  private static SparkSession spark;

  @BeforeAll
  static void start() {
    System.setProperty("spark.master", "local[1]");
    System.setProperty("spark.sql.shuffle.partitions", "2");
    System.setProperty("spark.ui.enabled", "false");
    System.setProperty("spark.hadoop.fs.defaultFS", "file:///");
    spark = SparkSupport.session("spark-jobs-test");
  }

  @AfterAll
  static void stop() {
    spark.stop();
  }

  /** URI file:/// dạng thô; Path.toUri() mã hóa dấu cách thành %20 mà Hadoop không giải mã. */
  private static String uri(Path path) {
    return "file:///" + path.toAbsolutePath().toString().replace('\\', '/').replaceFirst("^/", "");
  }

  private static String csv(Path dir, String... lines) throws Exception {
    List<String> all = new ArrayList<>();
    all.add(CsvEventParser.HEADER);
    all.addAll(List.of(lines));
    Path file = dir.resolve("events.csv");
    Files.write(file, all, StandardCharsets.UTF_8);
    return uri(file);
  }

  @Test
  void revenueOnFixtureMatchesHandComputedOracle() throws Exception {
    RevenueJob.Result result = RevenueJob.compute(spark, uri(FIXTURES.resolve("events.csv")), 2);
    List<String> actual = new ArrayList<>();
    for (List<Object> row : RevenueJob.csvRows(result.rows))
      actual.add(row.get(0) + "\t" + row.get(1) + "\t" + row.get(2) + "\t" + row.get(3));
    assertEquals(
        Files.readAllLines(FIXTURES.resolve("expected-category-id.tsv"), StandardCharsets.UTF_8),
        actual);
    assertEquals(Map.of("VALID_PURCHASE", 6L, "NON_PURCHASE", 1L, "HEADER", 1L), result.reasons);
    assertTrue(result.lineage.contains("ShuffledRDD"), result.lineage);
  }

  @Test
  void revenueDataFrameOnCsvAndParquetMatchesOracle(@TempDir Path dir) throws Exception {
    List<String> expected =
        Files.readAllLines(FIXTURES.resolve("expected-category-id.tsv"), StandardCharsets.UTF_8);
    Dataset<Row> events =
        EventEtlJob.curated(EventEtlJob.parsed(spark, uri(FIXTURES.resolve("events.csv"))));
    String parquet = uri(dir.resolve("curated"));
    events.write().partitionBy("event_date").parquet(parquet);
    for (RevenueJob.Result result :
        List.of(
            RevenueJob.computeFrame(events),
            RevenueJob.computeFrame(spark.read().parquet(parquet)))) {
      List<String> actual = new ArrayList<>();
      for (List<Object> row : RevenueJob.csvRows(result.rows))
        actual.add(row.get(0) + "\t" + row.get(1) + "\t" + row.get(2) + "\t" + row.get(3));
      assertEquals(expected, actual);
      assertTrue(result.lineage.contains("HashAggregate"), result.lineage);
    }
  }

  @Test
  void revenueClassifiesLikePurchasePreparation(@TempDir Path dir) throws Exception {
    String input =
        csv(
            dir,
            "t,view,1,1,a,b,NaN,u,s",
            "t,other,1,1,a,b,1,u,s",
            "t,purchase,1,,a,b,NaN,u,s",
            "t,purchase,1,,a,b,1,u,s",
            "t,purchase,1,1,a,b,0,u,s",
            "t,purchase,1,1,a,\"x,y\",10.00,u,",
            "t,purchase,1,1,a,b,1,u",
            "t,purchase,1,1,a,b,1,u,s,extra");
    RevenueJob.Result result = RevenueJob.compute(spark, input, 2);
    Map<String, Long> expected = new HashMap<>();
    expected.put("HEADER", 1L);
    expected.put("NON_PURCHASE", 1L);
    expected.put("UNKNOWN_EVENT", 1L);
    expected.put("INVALID_PRICE", 1L);
    expected.put("INVALID_GROUP", 1L);
    expected.put("VALID_PURCHASE", 2L);
    expected.put("MALFORMED_CSV", 2L);
    assertEquals(expected, result.reasons);
    assertEquals("1", result.rows.get(0).getString(0));
    assertEquals(1000L, result.rows.get(0).getLong(1));
  }

  @Test
  void etlConservesRowsAndRecordsRejectReasons(@TempDir Path dir) throws Exception {
    String input =
        csv(
            dir,
            "2019-10-01 00:00:00 UTC,view,1,10,electronics.phone,apple,5.00,u1,s1",
            "2019-10-01 01:00:00 UTC,cart,1,10,,apple,5.00,u1,s1",
            "2019-10-01 01:00:00 UTC,purchase,1,10,electronics.phone,,5.00,u1,",
            "not-a-time,view,1,10,a,b,1.00,u1,s1",
            "2019-10-01 02:00:00 UTC,click,1,10,a,b,1.00,u1,s1",
            "2019-10-01 02:00:00 UTC,view,,10,a,b,1.00,u1,s1",
            "2019-10-01 02:00:00 UTC,view,1,10,a,b,1.234,u1,s1",
            "2019-10-01 02:00:00 UTC,view,1,10,a,b,1.00,u1",
            "2019-10-01 00:00:00 UTC,view,1,10,electronics.phone,apple,5.00,u1,s1");
    Dataset<Row> parsed = EventEtlJob.parsed(spark, input);
    Map<String, Long> reasons = EventEtlJob.reasons(parsed);
    Map<String, Long> expected = new HashMap<>();
    expected.put("VALID", 4L);
    expected.put("HEADER", 1L);
    expected.put("INVALID_EVENT_TIME", 1L);
    expected.put("UNKNOWN_EVENT_TYPE", 1L);
    expected.put("MISSING_PRODUCT_ID", 1L);
    expected.put("INVALID_PRICE", 1L);
    expected.put("MALFORMED_CSV", 1L);
    assertEquals(expected, reasons);
    assertEquals(10L, reasons.values().stream().mapToLong(Long::longValue).sum());

    Dataset<Row> curated = EventEtlJob.curated(parsed);
    Map<String, Object> q = EventEtlJob.quality(curated, true);
    assertEquals(4L, q.get("rows"));
    assertEquals(1L, q.get("null_category_code"));
    assertEquals(1L, q.get("null_brand"));
    assertEquals(1L, q.get("null_user_session"));
    assertEquals(1L, q.get("exactDuplicateExtraRows"));
    assertEquals(Map.of("cart", 1L, "purchase", 1L, "view", 2L), q.get("eventTypes"));
    Row purchase = curated.filter("event_type = 'purchase'").first();
    assertEquals(new BigDecimal("5.00"), purchase.getAs("price"));
    assertEquals("electronics", purchase.getAs("category_root"));
    assertEquals(1, (int) purchase.getAs("event_hour"));
  }

  @Test
  void metricsOnFixture() {
    Dataset<Row> events =
        EventEtlJob.curated(EventEtlJob.parsed(spark, uri(FIXTURES.resolve("events.csv"))));
    Map<String, Row> funnel = new HashMap<>();
    for (Row r : MetricsJob.funnelByCategory(events).collectAsList())
      funnel.put(r.getAs("category_id"), r);
    assertEquals(1L, (long) funnel.get("1").getAs("views"));
    assertEquals(3L, (long) funnel.get("1").getAs("purchases"));
    assertEquals(6000L, (long) funnel.get("1").getAs("revenue_minor"));
    assertEquals(3.0, (double) funnel.get("1").getAs("view_to_purchase"));
    assertNull(funnel.get("1").getAs("cart_to_purchase"));
    assertEquals(3L, (long) funnel.get("2").getAs("revenue_minor"));
    assertNull(funnel.get("2").getAs("view_to_purchase"));

    List<Row> trend = MetricsJob.trendByHour(events).collectAsList();
    assertEquals(1, trend.size());
    assertEquals(7L, (long) trend.get(0).getAs("events"));
    assertEquals(1L, (long) trend.get(0).getAs("distinct_users"));

    Row brand = MetricsJob.funnelByBrand(events).first();
    assertEquals("brand", brand.getAs("brand"));
    assertEquals(6L, (long) brand.getAs("purchases"));
    assertEquals(6003L, (long) brand.getAs("revenue_minor"));
  }

  @Test
  void productLabelsUseOnlyHistoryBeforeT0(@TempDir Path dir) throws Exception {
    String input =
        csv(
            dir,
            "2019-10-01 10:00:00 UTC,view,p1,10,a.b,x,10.00,u1,s1",
            "2019-10-02 10:00:00 UTC,view,p1,10,a.b,x,10.00,u2,s2",
            "2019-10-03 00:00:00 UTC,purchase,p1,10,a.b,x,10.00,u2,s2",
            "2019-10-01 11:00:00 UTC,view,p2,10,a.b,x,5.00,u1,s1",
            "2019-10-02 11:00:00 UTC,view,p2,10,a.b,x,5.00,u1,s1",
            "2019-10-03 12:00:00 UTC,view,p2,10,a.b,x,5.00,u3,s3",
            "2019-10-04 09:00:00 UTC,view,p2,10,a.b,x,5.00,u3,s3",
            "2019-10-05 08:00:00 UTC,purchase,p2,10,a.b,x,5.00,u3,s3");
    Dataset<Row> events = EventEtlJob.curated(EventEtlJob.parsed(spark, input));
    java.time.LocalDate train = java.time.LocalDate.parse("2019-10-03");
    java.time.LocalDate test = java.time.LocalDate.parse("2019-10-05");

    Map<String, Row> rows = new HashMap<>();
    for (Row r : ProductLabelJob.snapshot(events, train, 2, 1, 2, "train").collectAsList())
      rows.put(r.getAs("product_id"), r);
    // Purchase lúc đúng 00:00 của t0 thuộc cửa sổ nhãn, không thuộc lịch sử.
    assertEquals(0L, (long) rows.get("p1").getAs("purchases"));
    assertEquals(1, (int) rows.get("p1").getAs("label"));
    assertEquals(0, (int) rows.get("p2").getAs("label"));
    assertEquals(0.5, (double) rows.get("p1").getAs("recent_view_share"), 1e-12);

    List<Row> testRows = ProductLabelJob.snapshot(events, test, 2, 1, 2, "test").collectAsList();
    assertEquals(1, testRows.size());
    assertEquals("p2", testRows.get(0).getAs("product_id"));
    assertEquals(1, (int) testRows.get(0).getAs("label"));

    assertEquals(true, ProductLabelJob.checks(events, train, 2, 1).get("historyBeforeT0"));
    // Cửa sổ nhãn vượt quá dữ liệu: phải từ chối thay vì gán nhãn 0.
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalStateException.class,
        () -> ProductLabelJob.checks(events, java.time.LocalDate.parse("2019-10-05"), 2, 3));
  }

  @Test
  void productFeaturesFilterAndTransform(@TempDir Path dir) throws Exception {
    String input =
        csv(
            dir,
            "2019-10-01 00:00:00 UTC,view,p1,10,a.b,x,10.00,u1,s1",
            "2019-10-01 00:00:01 UTC,view,p1,10,a.b,x,10.00,u2,s2",
            "2019-10-01 00:00:02 UTC,cart,p1,10,a.b,x,10.00,u2,s2",
            "2019-10-01 00:00:03 UTC,purchase,p1,10,a.b,x,10.00,u2,s2",
            "2019-10-01 00:00:04 UTC,view,p2,10,a.b,x,99.00,u1,s1");
    Dataset<Row> events = EventEtlJob.curated(EventEtlJob.parsed(spark, input));
    List<Row> rows =
        ProductFeaturesJob.withFeatures(ProductFeaturesJob.productStats(events), 2).collectAsList();
    assertEquals(1, rows.size());
    Row p1 = rows.get(0);
    assertEquals("p1", p1.getAs("product_id"));
    assertEquals(0.5, (double) p1.getAs("cart_rate"), 1e-12);
    assertEquals(0.5, (double) p1.getAs("purchase_rate"), 1e-12);
    assertEquals(Math.log1p(2), (double) p1.getAs("log_views"), 1e-12);
    assertEquals(Math.log1p(10.0), (double) p1.getAs("log_median_price"), 1e-12);
    assertEquals(Math.log1p(2), (double) p1.getAs("log_distinct_users"), 1e-12);
  }
}
