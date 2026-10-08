package vn.edu.bigdata.revenue.spark;

import static org.apache.spark.sql.functions.col;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import vn.edu.bigdata.revenue.cli.CliArguments;
import vn.edu.bigdata.revenue.output.OutputRow;
import vn.edu.bigdata.revenue.output.ResultValidator;

/**
 * Publish serving artifacts (plan §18.4): gom kết quả đã có trên HDFS thành JSON/CSV nhỏ cho
 * backend, kèm manifest.json (sha256, số dòng từng file). Ghi một lần vào thư mục run mới; manifest
 * ghi cuối cùng nên run thiếu manifest là run hỏng. Không tính toán lại phép tổng hợp nào, chỉ đọc,
 * đối chiếu MR với Spark (A1) và chuyển định dạng.
 *
 * <pre>
 * publish --tag d3 --revenue-run-id A1 --mr-output hdfs://…/mr/…/v1 --etl-run-id ETL --metrics-run-id M
 *         [--kmeans-run-id K --knn-run-id N --benchmarks-dir docs\evidence\bench\serving --run-id ID]
 * </pre>
 */
public final class ServingPublishJob {
  public static final String OUTPUT_ROOT = "/data/ecommerce/serving";
  private static final ObjectMapper JSON = new ObjectMapper();

  private ServingPublishJob() {}

  /** Bộ ghi file nhỏ lên HDFS, ghi lại sha256/kích thước/số dòng cho manifest. */
  static final class Writer {
    private final FileSystem fs;
    private final String root;
    final ArrayNode files = JSON.createArrayNode();

    Writer(FileSystem fs, String root) {
      this.fs = fs;
      this.root = root;
    }

    void bytes(String relative, byte[] content, Long rows) throws Exception {
      try (OutputStream out = fs.create(new Path(root + "/" + relative), false)) {
        out.write(content);
      }
      ObjectNode f = files.addObject();
      f.put("path", relative);
      f.put(
          "sha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content)));
      f.put("bytes", content.length);
      if (rows != null) f.put("rows", rows);
    }

    void json(String relative, JsonNode value) throws Exception {
      bytes(relative, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(value), null);
    }

    /** CSV RFC 4180, LF; số thực in bằng Double.toString (khôi phục chính xác giá trị double). */
    void csv(String relative, Dataset<Row> table) throws Exception {
      List<String> header = new ArrayList<>();
      for (StructField f : table.schema().fields()) header.add(f.name());
      StringWriter text = new StringWriter();
      long rows = 0;
      try (CSVPrinter printer =
          new CSVPrinter(text, CSVFormat.RFC4180.builder().setRecordSeparator('\n').build())) {
        printer.printRecord(header);
        for (Row r : table.collectAsList()) {
          List<Object> values = new ArrayList<>();
          for (int i = 0; i < r.size(); i++) values.add(r.isNullAt(i) ? "" : r.get(i));
          printer.printRecord(values);
          rows++;
        }
      }
      bytes(relative, text.toString().getBytes(StandardCharsets.UTF_8), rows);
    }
  }

  static JsonNode readJson(FileSystem fs, String path) throws IOException {
    try (InputStream in = fs.open(new Path(path))) {
      return JSON.readTree(in);
    }
  }

  /** So khớp chính xác (sum_minor, count, average) giữa MR V1 (part-r-*) và Spark A1 (Parquet). */
  static ObjectNode parity(Map<String, OutputRow> mr, List<Row> spark) {
    Map<String, Row> sparkRows = new TreeMap<>();
    for (Row r : spark) sparkRows.put(r.getString(0), r);
    ArrayNode mismatches = JSON.createArrayNode();
    long purchases = 0;
    for (var e : mr.entrySet()) {
      Row s = sparkRows.get(e.getKey());
      OutputRow m = e.getValue();
      purchases += m.purchaseCount;
      if (s == null
          || s.getLong(1) != m.sumMinor
          || s.getLong(2) != m.purchaseCount
          || !s.getString(4).equals(m.averageText))
        mismatches
            .addObject()
            .put("group", e.getKey())
            .put("reason", s == null ? "only MR" : "values differ");
    }
    for (String key : sparkRows.keySet())
      if (!mr.containsKey(key))
        mismatches.addObject().put("group", key).put("reason", "only Spark");
    ObjectNode out = JSON.createObjectNode();
    out.put("matched", mismatches.isEmpty());
    out.put("mrGroups", mr.size());
    out.put("sparkGroups", sparkRows.size());
    out.put("mrPurchaseCount", purchases);
    out.put("mismatchCount", mismatches.size());
    ArrayNode first = out.putArray("mismatches");
    for (int i = 0; i < Math.min(50, mismatches.size()); i++) first.add(mismatches.get(i));
    return out;
  }

  static void run(SparkSession spark, CliArguments a) throws Exception {
    String tag = a.required("tag");
    String runId = a.get("run-id", SparkSupport.runId(tag));
    String out = OUTPUT_ROOT + "/" + runId;
    Map<String, Object> params = new LinkedHashMap<>();
    for (String key :
        List.of(
            "revenue-run-id",
            "mr-output",
            "etl-run-id",
            "metrics-run-id",
            "kmeans-run-id",
            "knn-run-id",
            "benchmarks-dir")) params.put(key, a.get(key, null));
    params.put("output", out);
    SparkSupport.RunRecord run = new SparkSupport.RunRecord(spark, "publish", runId, params);
    try {
      SparkSupport.requireNew(spark, out);
      FileSystem fs = SparkSupport.fs(spark, out);
      String root = SparkSupport.qualify(out);
      Writer w = new Writer(fs, root);
      ObjectNode manifest = JSON.createObjectNode();
      manifest.put("schemaVersion", 1);
      manifest.put("runId", runId);
      manifest.put("createdAt", Instant.now().toString());
      manifest.put("gitSha", System.getenv().getOrDefault("PTIT_GIT_SHA", "nogit"));
      ObjectNode sources = manifest.putObject("sources");

      // --- Group By và parity --------------------------------------------------------------
      String revenueRun = a.required("revenue-run-id");
      String etlRun = a.required("etl-run-id");
      String metricsRun = a.required("metrics-run-id");
      String mrOutput = a.required("mr-output");
      sources.put("sparkRevenueRunId", revenueRun).put("etlRunId", etlRun);
      sources.put("metricsRunId", metricsRun).put("mrOutput", mrOutput);
      JsonNode etl =
          readJson(
              fs,
              SparkSupport.qualify(EventEtlJob.OUTPUT_ROOT + "/run_id=" + etlRun + "/_run.json"));
      manifest.set(
          "dataset",
          JSON.createObjectNode()
              .put("tag", tag)
              .put("input", etl.path("params").path("input").asText()));
      run.stage(
          "analytics",
          () -> {
            Dataset<Row> revenue =
                spark
                    .read()
                    .parquet(
                        SparkSupport.qualify(RevenueJob.OUTPUT_ROOT + "/run_id=" + revenueRun));
            List<Row> sparkRows = revenue.orderBy("group_key").collectAsList();
            Map<String, OutputRow> mr =
                ResultValidator.read(new Path(mrOutput), SparkSupport.fs(spark, mrOutput));
            ObjectNode parity = parity(mr, sparkRows);
            parity.put("mrOutput", mrOutput).put("sparkRevenueRunId", revenueRun);
            w.json("analytics/revenue_parity.json", parity);
            String agg = MetricsJob.AGG_ROOT;
            Dataset<Row> codes =
                spark
                    .read()
                    .parquet(SparkSupport.qualify(agg + "/funnel_by_category/run_id=" + metricsRun))
                    .select(col("category_id").as("group_key"), col("category_code"));
            w.csv(
                "analytics/revenue_by_category.csv",
                revenue
                    .join(codes, "group_key", "left")
                    .select(
                        "group_key",
                        "category_code",
                        "total_revenue",
                        "purchase_count",
                        "average_revenue")
                    .orderBy("group_key"));
            Map<String, String[]> tables = new LinkedHashMap<>();
            tables.put("funnel_by_category", new String[] {"category_id"});
            tables.put("funnel_by_brand", new String[] {"brand"});
            tables.put("trend_by_hour", new String[] {"event_date", "event_hour"});
            for (var t : tables.entrySet()) {
              Dataset<Row> table =
                  spark
                      .read()
                      .parquet(
                          SparkSupport.qualify(agg + "/" + t.getKey() + "/run_id=" + metricsRun));
              w.csv(
                  "analytics/" + t.getKey() + ".csv",
                  table.orderBy(
                      t.getValue()[0],
                      java.util.Arrays.copyOfRange(t.getValue(), 1, t.getValue().length)));
            }
            w.json("analytics/quality.json", etl.path("metrics"));
            run.metrics.put("parityMatched", parity.path("matched").asBoolean());
            return null;
          });

      // --- Benchmark (bảng tổng hợp đã đối chiếu, tạo từ docs/evidence/bench) -----------------
      String bench = a.get("benchmarks-dir", null);
      if (bench != null)
        run.stage(
            "benchmarks",
            () -> {
              try (var files = Files.list(java.nio.file.Path.of(bench))) {
                for (java.nio.file.Path f :
                    files.filter(p -> p.toString().endsWith(".json")).sorted().toList())
                  w.bytes("benchmarks/" + f.getFileName(), Files.readAllBytes(f), null);
              }
              return null;
            });

      // --- Mô hình ML ----------------------------------------------------------------------
      ArrayNode models = JSON.createArrayNode();
      String kmeansRun = a.get("kmeans-run-id", null);
      if (kmeansRun != null)
        run.stage(
            "kmeans",
            () -> {
              String dir = SparkSupport.qualify("/data/ecommerce/ml/kmeans/run_id=" + kmeansRun);
              for (String f : List.of("model.json", "metrics.json", "metadata.json"))
                w.json("ml/kmeans/" + f, readJson(fs, dir + "/" + f));
              for (String f : List.of("sweep.csv", "profile.csv"))
                try (InputStream in = fs.open(new Path(dir + "/" + f))) {
                  byte[] content = in.readAllBytes();
                  w.bytes("ml/kmeans/" + f, content, lines(content) - 1);
                }
              w.csv(
                  "ml/kmeans/assignments.csv",
                  spark.read().parquet(dir + "/assignments").orderBy("product_id"));
              models.add(readJson(fs, dir + "/metadata.json"));
              return null;
            });
      String knnRun = a.get("knn-run-id", null);
      if (knnRun != null)
        run.stage(
            "knn",
            () -> {
              String dir = SparkSupport.qualify("/data/ecommerce/ml/knn/run_id=" + knnRun);
              JsonNode model = readJson(fs, dir + "/model.json");
              for (String f : List.of("model.json", "metrics.json", "metadata.json"))
                w.json("ml/knn/" + f, readJson(fs, dir + "/" + f));
              w.csv(
                  "ml/knn/train_set.csv", spark.read().parquet(dir + "/train_set").orderBy("row"));
              // Sản phẩm test để demo: đặc trưng + số đếm thô tại t0 của test, nhãn thật, kết quả
              // notebook.
              Dataset<Row> labels =
                  spark
                      .read()
                      .parquet(
                          SparkSupport.qualify(
                              ProductLabelJob.OUTPUT_ROOT
                                  + "/run_id="
                                  + model.path("labelsRunId").asText()))
                      .where("split = 'test'")
                      .drop("label");
              Dataset<Row> predictions = spark.read().parquet(dir + "/test_predictions");
              List<String> columns =
                  new ArrayList<>(
                      List.of(
                          "product_id",
                          "label",
                          "vote_share",
                          "prediction",
                          "t0",
                          "category_code",
                          "brand",
                          "views",
                          "carts",
                          "purchases",
                          "median_price",
                          "distinct_users",
                          "recent_views"));
              columns.addAll(ProductLabelJob.FEATURES);
              w.csv(
                  "ml/knn/products.csv",
                  predictions
                      .join(labels, "product_id")
                      .select(
                          columns.stream()
                              .map(c -> col(c))
                              .toArray(org.apache.spark.sql.Column[]::new))
                      .orderBy("product_id"));
              models.add(readJson(fs, dir + "/metadata.json"));
              return null;
            });
      w.json("ml/models.json", models);

      manifest.set("files", w.files);
      // manifest ghi cuối: backend chỉ đọc run có manifest và chỉ đọc file có trong manifest.
      try (OutputStream o = fs.create(new Path(root + "/manifest.json"), false)) {
        o.write(JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest));
      }
      run.metrics.put("files", w.files.size());
      run.finish(out + "/_run.json", null);
      System.out.println("Publish xong: " + runId + " files=" + w.files.size() + " -> " + out);
    } catch (Exception e) {
      run.finish(null, e);
      throw e;
    }
  }

  private static long lines(byte[] content) {
    long n = 0;
    for (byte b : content) if (b == '\n') n++;
    return n;
  }
}
