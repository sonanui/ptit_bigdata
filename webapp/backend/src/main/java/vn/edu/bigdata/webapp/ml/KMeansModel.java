package vn.edu.bigdata.webapp.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;

/**
 * Mô hình K-Means từ {@code ml/kmeans/model.json} (notebook K-Means): thứ tự đặc trưng, mean/std
 * của StandardScaler, tâm cụm trong không gian đã chuẩn hóa. Suy luận = tâm gần nhất.
 */
public record KMeansModel(
    String runId, int k, List<String> features, double[] mean, double[] std, double[][] centers) {

  public static KMeansModel from(JsonNode json) {
    List<String> features = new ArrayList<>();
    json.path("features").forEach(f -> features.add(f.asText()));
    double[][] centers = new double[json.path("centers").size()][];
    for (int i = 0; i < centers.length; i++) centers[i] = Json.doubles(json.path("centers").get(i));
    KMeansModel model =
        new KMeansModel(
            json.path("runId").asText(),
            json.path("k").asInt(),
            List.copyOf(features),
            Json.doubles(json.path("scalerMean")),
            Json.doubles(json.path("scalerStd")),
            centers);
    if (model.k != centers.length
        || model.mean.length != features.size()
        || model.std.length != features.size())
      throw new IllegalStateException("model.json K-Means không nhất quán: " + model.runId);
    return model;
  }

  /** Cụm gần nhất (bằng nhau thì chỉ số nhỏ hơn, giống argmin của Spark/numpy) và khoảng cách. */
  public Prediction predict(double[] x) {
    double[] z = ProductFeatures.standardize(x, mean, std);
    double[] distances = new double[centers.length];
    int best = 0;
    for (int c = 0; c < centers.length; c++) {
      distances[c] = ProductFeatures.squaredDistance(z, centers[c]);
      if (distances[c] < distances[best]) best = c;
    }
    return new Prediction(best, distances, z);
  }

  /** {@code squaredDistances}: tới từng tâm, trong không gian đã chuẩn hóa. */
  public record Prediction(int cluster, double[] squaredDistances, double[] standardized) {}
}
