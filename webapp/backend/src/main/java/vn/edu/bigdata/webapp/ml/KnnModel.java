package vn.edu.bigdata.webapp.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * KNN phân loại từ {@code ml/knn/model.json} + {@code ml/knn/train_set.csv} (notebook KNN): tập
 * huấn luyện đã chuẩn hóa bằng StandardScaler của Spark, K và ngưỡng vote_share chọn trên validation.
 *
 * <p>Đây là suy luận (tìm k láng giềng, bỏ phiếu), không phải huấn luyện: KNN là lazy learner nên
 * "mô hình" chính là tập huấn luyện đã lưu. Quy tắc giống {@code ml_common.knn_vote_share}: Euclidean
 * bình phương cộng theo thứ tự cột, hòa ở ranh giới thì dòng train có chỉ số nhỏ hơn được chọn.
 */
public record KnnModel(
    String runId,
    int k,
    double threshold,
    List<String> features,
    double[] mean,
    double[] std,
    String[] trainProductIds,
    int[] trainLabels,
    double[][] trainZ) {

  public static KnnModel from(JsonNode json, List<Map<String, String>> trainSet) {
    List<String> features = new ArrayList<>();
    json.path("features").forEach(f -> features.add(f.asText()));
    int n = trainSet.size();
    String[] ids = new String[n];
    int[] labels = new int[n];
    double[][] z = new double[n][features.size()];
    for (Map<String, String> row : trainSet) {
      int i = Integer.parseInt(row.get("row"));
      if (i < 0 || i >= n || ids[i] != null)
        throw new IllegalStateException("train_set.csv: cột row không liên tục tại " + i);
      ids[i] = row.get("product_id");
      labels[i] = Integer.parseInt(row.get("label"));
      for (int j = 0; j < features.size(); j++)
        z[i][j] = Double.parseDouble(row.get("z_" + features.get(j)));
    }
    KnnModel model =
        new KnnModel(
            json.path("runId").asText(),
            json.path("k").asInt(),
            json.path("threshold").asDouble(),
            List.copyOf(features),
            Json.doubles(json.path("scalerMean")),
            Json.doubles(json.path("scalerStd")),
            ids,
            labels,
            z);
    if (n != json.path("trainRows").asInt() || model.k < 1 || model.k > n)
      throw new IllegalStateException("model.json KNN không khớp train_set.csv: " + model.runId);
    return model;
  }

  public Prediction predict(double[] x) {
    return predictStandardized(ProductFeatures.standardize(x, mean, std));
  }

  public Prediction predictStandardized(double[] z) {
    // Max-heap theo (khoảng cách, chỉ số): giữ k phần tử nhỏ nhất theo thứ tự (d, i) tăng dần.
    Comparator<double[]> order =
        Comparator.<double[]>comparingDouble(e -> e[0]).thenComparingDouble(e -> e[1]);
    PriorityQueue<double[]> heap = new PriorityQueue<>(k, order.reversed());
    for (int i = 0; i < trainZ.length; i++) {
      double d = ProductFeatures.squaredDistance(z, trainZ[i]);
      if (heap.size() < k) heap.add(new double[] {d, i});
      else if (order.compare(new double[] {d, i}, heap.peek()) < 0) {
        heap.poll();
        heap.add(new double[] {d, i});
      }
    }
    List<double[]> nearest = new ArrayList<>(heap);
    nearest.sort(order);
    List<Neighbour> neighbours = new ArrayList<>();
    int positives = 0;
    for (double[] e : nearest) {
      int i = (int) e[1];
      positives += trainLabels[i];
      neighbours.add(new Neighbour(trainProductIds[i], trainLabels[i], e[0]));
    }
    double voteShare = (double) positives / k;
    return new Prediction(voteShare >= threshold ? 1 : 0, voteShare, neighbours);
  }

  public record Neighbour(String productId, int label, double squaredDistance) {}

  /** {@code voteShare}: tỷ lệ láng giềng nhãn 1, không phải xác suất đã hiệu chỉnh. */
  public record Prediction(int label, double voteShare, List<Neighbour> neighbours) {}
}
