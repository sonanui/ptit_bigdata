package vn.edu.bigdata.revenue.spark;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import vn.edu.bigdata.revenue.cli.CliArguments;

/**
 * Điểm chạy các job Spark (Java) của pipeline:
 *
 * <pre>
 * spark-submit --class vn.edu.bigdata.revenue.spark.SparkTool revenue-aggregation-spark.jar JOB --name value ...
 *   revenue  --input /data/... --tag d1 [--reducers 2]                   A1 (RDD reduceByKey), đối chứng MR
 *            [--mode rdd-raw|df-raw|df-curated --curated-run-id ID]  E4: DataFrame trên CSV/Parquet
 *   etl      --input /data/... --tag d1 [--duplicates true]              raw -> curated Parquet + chất lượng
 *   metrics  --curated-run-id ID [--revenue-run-id A1_ID]                A2-A5
 *   features --curated-run-id ID [--min-views 20]                        A7 (đầu vào notebook K-Means/KNN)
 *   labels   --curated-run-id ID --train-t0 D --test-t0 D [--feature-days 14 --label-days 7]  A8 (KNN phân loại)
 *   publish  --tag d3 --revenue-run-id … --mr-output … --etl-run-id … --metrics-run-id … [--kmeans-run-id … --knn-run-id …]  serving
 * </pre>
 */
public final class SparkTool {
  private static final Map<String, Set<String>> FLAGS =
      Map.of(
          "revenue", Set.of("input", "tag", "reducers", "run-id", "mode", "curated-run-id"),
          "etl", Set.of("input", "tag", "duplicates", "run-id"),
          "metrics", Set.of("curated-run-id", "revenue-run-id", "run-id"),
          "features", Set.of("curated-run-id", "min-views", "run-id"),
          "labels",
              Set.of(
                  "curated-run-id",
                  "train-t0",
                  "test-t0",
                  "feature-days",
                  "label-days",
                  "min-views",
                  "run-id"),
          "publish",
              Set.of(
                  "tag",
                  "revenue-run-id",
                  "mr-output",
                  "etl-run-id",
                  "metrics-run-id",
                  "kmeans-run-id",
                  "knn-run-id",
                  "benchmarks-dir",
                  "run-id"));

  private SparkTool() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || !FLAGS.containsKey(args[0])) {
      System.err.println(
          "Usage: SparkTool {revenue|etl|metrics|features|labels|publish} --name value ...");
      System.exit(2);
    }
    CliArguments a = new CliArguments(Arrays.copyOfRange(args, 1, args.length), FLAGS.get(args[0]));
    SparkSession spark = SparkSupport.session("ptit-" + args[0]);
    try {
      switch (args[0]) {
        case "revenue":
          RevenueJob.run(spark, a);
          break;
        case "etl":
          EventEtlJob.run(spark, a);
          break;
        case "metrics":
          MetricsJob.run(spark, a);
          break;
        case "labels":
          ProductLabelJob.run(spark, a);
          break;
        case "publish":
          ServingPublishJob.run(spark, a);
          break;
        default:
          ProductFeaturesJob.run(spark, a);
      }
    } finally {
      spark.stop();
    }
  }
}
