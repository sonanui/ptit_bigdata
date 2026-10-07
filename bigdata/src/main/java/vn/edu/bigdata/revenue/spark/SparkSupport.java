package vn.edu.bigdata.revenue.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;
import vn.edu.bigdata.revenue.input.JsonArtifacts;

/** SparkSession local (host) đọc/ghi HDFS trong Docker, cùng tiện ích lưu vết lần chạy. */
public final class SparkSupport {
  public static final String DEFAULT_HDFS = "hdfs://localhost:8020";
  public static final java.nio.file.Path RESULTS = java.nio.file.Paths.get("results", "spark");

  private SparkSupport() {}

  /**
   * Giá trị đặt qua spark-submit (--master, --conf) được giữ nguyên; chỉ điền mặc định khi thiếu.
   * DataNode trong Docker khai báo hostname localhost và publish cổng 9866, nên client trên host
   * phải dùng hostname của DataNode. Cụm có 1 DataNode nên replication phía client là 1.
   */
  public static SparkSession session(String app) {
    SparkConf conf =
        new SparkConf()
            .setIfMissing("spark.master", "local[2]")
            .setIfMissing("spark.app.name", app)
            .setIfMissing("spark.sql.session.timeZone", "UTC")
            .setIfMissing("spark.sql.shuffle.partitions", "8")
            .setIfMissing("spark.ui.showConsoleProgress", "false")
            .setIfMissing("spark.hadoop.fs.defaultFS", hdfsUri())
            .setIfMissing("spark.hadoop.dfs.client.use.datanode.hostname", "true")
            .setIfMissing("spark.hadoop.dfs.replication", "1");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel(System.getenv().getOrDefault("SPARK_LOG_LEVEL", "WARN"));
    return spark;
  }

  public static String hdfsUri() {
    return System.getenv().getOrDefault("HDFS_URI", DEFAULT_HDFS);
  }

  /** /data/... là đường dẫn HDFS tuyệt đối; URI có scheme (hdfs://, file://) giữ nguyên. */
  public static String qualify(String path) {
    return path.startsWith("/") ? hdfsUri() + path : path;
  }

  public static FileSystem fs(SparkSession spark, String path) throws IOException {
    return new Path(qualify(path)).getFileSystem(spark.sparkContext().hadoopConfiguration());
  }

  public static void requireNew(SparkSession spark, String path) throws IOException {
    if (fs(spark, path).exists(new Path(qualify(path))))
      throw new IllegalArgumentException("Output already exists: " + qualify(path));
  }

  public static String runId(String tag) {
    String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
    return time + "-" + System.getenv().getOrDefault("PTIT_GIT_SHA", "nogit") + "-" + tag;
  }

  public static void writeCsv(java.nio.file.Path file, List<String> header, List<List<Object>> rows)
      throws IOException {
    Files.createDirectories(file.getParent());
    try (Writer writer =
            Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        CSVPrinter printer =
            new CSVPrinter(writer, CSVFormat.RFC4180.builder().setRecordSeparator('\n').build())) {
      printer.printRecord(header);
      for (List<Object> row : rows) printer.printRecord(row);
    }
  }

  public static void writeJson(java.nio.file.Path file, Object value) throws IOException {
    Files.createDirectories(file.getParent());
    try (Writer writer =
        Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
      new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(writer, value);
    }
  }

  /** Cộng dồn task metrics của mọi task kết thúc trong phiên (input, shuffle) cho thực nghiệm. */
  static final class TaskTotals extends SparkListener {
    static final List<String> NAMES =
        List.of(
            "tasks",
            "inputBytesRead",
            "inputRecordsRead",
            "shuffleReadBytes",
            "shuffleReadRecords",
            "shuffleWriteBytes",
            "shuffleWriteRecords",
            "executorRunTimeMillis",
            "jvmGcTimeMillis");
    private final AtomicLongArray totals = new AtomicLongArray(NAMES.size());

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      TaskMetrics m = taskEnd.taskMetrics();
      if (m == null) return;
      long[] values = {
        1,
        m.inputMetrics().bytesRead(),
        m.inputMetrics().recordsRead(),
        m.shuffleReadMetrics().totalBytesRead(),
        m.shuffleReadMetrics().recordsRead(),
        m.shuffleWriteMetrics().bytesWritten(),
        m.shuffleWriteMetrics().recordsWritten(),
        m.executorRunTime(),
        m.jvmGCTime()
      };
      for (int i = 0; i < values.length; i++) totals.addAndGet(i, values[i]);
    }

    Map<String, Long> snapshot() {
      Map<String, Long> out = new LinkedHashMap<>();
      for (int i = 0; i < NAMES.size(); i++) out.put(NAMES.get(i), totals.get(i));
      return out;
    }
  }

  /** Lưu vết một lần chạy: tham số, thời gian từng bước, số liệu, phiên bản, lỗi. */
  public static final class RunRecord {
    private final SparkSession spark;
    private final TaskTotals taskTotals = new TaskTotals();
    private final long started = System.nanoTime();
    private final List<Map<String, Object>> stages = new ArrayList<>();
    public final Map<String, Object> record = new LinkedHashMap<>();
    public final Map<String, Object> metrics = new LinkedHashMap<>();
    public final String runId;
    public final java.nio.file.Path localDir;

    public RunRecord(SparkSession spark, String job, String runId, Map<String, Object> params) {
      this.spark = spark;
      this.runId = runId;
      this.localDir = RESULTS.resolve(runId);
      record.put("schemaVersion", 1);
      record.put("job", job);
      record.put("runId", runId);
      record.put("params", params);
      record.put("startedAt", Instant.now().toString());
      record.put("gitSha", System.getenv().getOrDefault("PTIT_GIT_SHA", "nogit"));
      record.put("sparkVersion", spark.version());
      record.put("javaVersion", System.getProperty("java.version"));
      record.put("hadoopClientVersion", VersionInfo.getVersion());
      record.put("master", spark.sparkContext().master());
      Map<String, String> config = new LinkedHashMap<>();
      for (String key :
          List.of(
              "spark.driver.memory",
              "spark.sql.shuffle.partitions",
              "spark.sql.ansi.enabled",
              "spark.sql.session.timeZone",
              "spark.hadoop.fs.defaultFS")) {
        scala.Option<String> value = spark.conf().getOption(key);
        config.put(key, value.isDefined() ? value.get() : "default");
      }
      record.put("configuration", config);
      record.put("stages", stages);
      record.put("metrics", metrics);
      spark.sparkContext().addSparkListener(taskTotals);
    }

    public <T> T stage(String name, Callable<T> body) throws Exception {
      long t0 = System.nanoTime();
      try {
        return body.call();
      } finally {
        Map<String, Object> stage = new LinkedHashMap<>();
        stage.put("name", name);
        stage.put("elapsedMillis", (System.nanoTime() - t0) / 1_000_000);
        stages.add(stage);
      }
    }

    /** Ghi JSON cục bộ; nếu thành công thì ghi thêm bản create-only lên HDFS cạnh output. */
    public void finish(String hdfsPath, Throwable error) throws IOException {
      record.put("status", error == null ? "succeeded" : "failed");
      record.put("error", error == null ? null : error.toString());
      record.put("endToEndMillis", (System.nanoTime() - started) / 1_000_000);
      // Listener chạy bất đồng bộ: chờ hàng đợi sự kiện rỗng trước khi đọc tổng.
      // Hết thời gian chờ thì vẫn ghi tổng hiện có nhưng đánh dấu là có thể thiếu.
      boolean complete = true;
      try {
        spark.sparkContext().listenerBus().waitUntilEmpty(10_000);
      } catch (java.util.concurrent.TimeoutException e) {
        complete = false;
      }
      record.put("taskMetrics", taskTotals.snapshot());
      record.put("taskMetricsComplete", complete);
      writeJson(localDir.resolve(record.get("job") + "-run.json"), record);
      if (error == null && hdfsPath != null)
        JsonArtifacts.write(new Path(qualify(hdfsPath)), fs(spark, hdfsPath), record);
    }
  }
}
