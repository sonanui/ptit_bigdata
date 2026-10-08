package vn.edu.bigdata.webapp.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vn.edu.bigdata.webapp.ml.KMeansModel;
import vn.edu.bigdata.webapp.ml.KnnModel;
import vn.edu.bigdata.webapp.ml.ModelRegistry;
import vn.edu.bigdata.webapp.ml.ModelRegistry.Entry;
import vn.edu.bigdata.webapp.ml.ProductFeatures;
import vn.edu.bigdata.webapp.serving.ArtifactNotFoundException;

/**
 * Suy luận online trên mô hình đã huấn luyện offline (notebook Spark MLlib). Không có endpoint
 * huấn luyện. Đầu vào: product_id có trong artifact, hoặc số đếm thô của một sản phẩm.
 */
@RestController
@RequestMapping("/api/ml")
class MlController {
  private final ModelRegistry registry;

  MlController(ModelRegistry registry) {
    this.registry = registry;
  }

  /** Số đếm thô; recentViews chỉ cần cho KNN (A8). */
  record RawInput(
      @PositiveOrZero double views,
      @PositiveOrZero double carts,
      @PositiveOrZero double purchases,
      @PositiveOrZero double medianPrice,
      @PositiveOrZero double distinctUsers,
      @PositiveOrZero Double recentViews) {
    ProductFeatures.Raw toRaw() {
      return new ProductFeatures.Raw(views, carts, purchases, medianPrice, distinctUsers, recentViews);
    }
  }

  record PredictRequest(String runId, String productId, @Valid RawInput raw) {}

  // ---------------------------------------------------------------- danh mục và chi tiết mô hình

  @GetMapping("/{type:kmeans|knn}/models")
  List<Map<String, Object>> models(@PathVariable String type) {
    List<Map<String, Object>> out = new ArrayList<>();
    for (Entry e : registry.list(type)) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("runId", e.modelRunId());
      row.put("servingRunId", e.servingRunId());
      row.put("metadata", e.metadata());
      out.add(row);
    }
    return out;
  }

  @GetMapping("/{type:kmeans|knn}/{runId}")
  Map<String, Object> model(@PathVariable String type, @PathVariable String runId) {
    Entry e = registry.find(type, runId);
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("runId", e.modelRunId());
    out.put("servingRunId", e.servingRunId());
    out.put("metadata", e.metadata());
    out.put("metrics", registry.artifact(e, "metrics.json"));
    return out;
  }

  @GetMapping("/{type:kmeans|knn}/{runId}/metrics")
  JsonNode metrics(@PathVariable String type, @PathVariable String runId) {
    return registry.artifact(registry.find(type, runId), "metrics.json");
  }

  @GetMapping("/kmeans/{runId}/clusters")
  Map<String, Object> clusters(@PathVariable String runId) {
    Entry e = registry.find(ModelRegistry.KMEANS, runId);
    KMeansModel model = registry.kmeans(e);
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("runId", e.modelRunId());
    out.put("k", model.k());
    out.put("features", model.features());
    out.put("centers", model.centers());
    out.put("sizes", registry.artifact(e, "metrics.json").path("clusterSizes"));
    out.put("profile", registry.table(e, "profile.csv"));
    return out;
  }

  /** Tìm sản phẩm thật để demo: theo product_id, brand hoặc category_code. */
  @GetMapping("/{type:kmeans|knn}/{runId}/products")
  List<Map<String, String>> products(
      @PathVariable String type,
      @PathVariable String runId,
      @RequestParam(required = false) String q,
      @RequestParam(defaultValue = "20") int limit) {
    if (limit < 1 || limit > 200) throw new IllegalArgumentException("limit trong [1, 200]");
    String needle = q == null ? "" : q.toLowerCase(Locale.ROOT).trim();
    List<Map<String, String>> out = new ArrayList<>();
    for (Map<String, String> row : catalog(registry.find(type, runId))) {
      if (needle.isEmpty()
          || row.get("product_id").contains(needle)
          || contains(row.get("brand"), needle)
          || contains(row.get("category_code"), needle)) out.add(row);
      if (out.size() == limit) break;
    }
    return out;
  }

  private static boolean contains(String value, String needle) {
    return value != null && value.toLowerCase(Locale.ROOT).contains(needle);
  }

  private List<Map<String, String>> catalog(Entry e) {
    return registry.table(e, ModelRegistry.KMEANS.equals(e.type()) ? "assignments.csv" : "products.csv");
  }

  private Map<String, String> product(Entry e, String productId) {
    for (Map<String, String> row : catalog(e))
      if (productId.equals(row.get("product_id"))) return row;
    throw new ArtifactNotFoundException(
        "Sản phẩm " + productId + " không có trong tập " + e.type() + " của run " + e.modelRunId());
  }

  // ------------------------------------------------------------------------------- suy luận

  @PostMapping("/kmeans/predict")
  Map<String, Object> predictCluster(@Valid @RequestBody PredictRequest req) {
    Entry e = registry.find(ModelRegistry.KMEANS, req.runId());
    KMeansModel model = registry.kmeans(e);
    Input input = input(e, req, model.features(), minViews(e));
    KMeansModel.Prediction p = model.predict(input.vector);
    Map<String, Object> out = response(e, input);
    out.put("cluster", p.cluster());
    out.put("squaredDistances", p.squaredDistances());
    out.put("standardizedFeatures", p.standardized());
    if (input.row != null) {
      int spark = Integer.parseInt(input.row.get("cluster"));
      out.put("sparkCluster", spark);
      out.put("matchesSpark", spark == p.cluster());
    }
    return out;
  }

  @PostMapping("/knn/predict")
  Map<String, Object> predictLabel(@Valid @RequestBody PredictRequest req) {
    Entry e = registry.find(ModelRegistry.KNN, req.runId());
    KnnModel model = registry.knn(e);
    if (req.raw() != null && req.raw().recentViews() == null)
      throw new IllegalArgumentException("Thiếu lượt xem trong 7 ngày gần nhất (raw.recentViews), KNN cần thông tin này");
    Input input = input(e, req, model.features(), minViews(e));
    KnnModel.Prediction p = model.predict(input.vector);
    Map<String, Object> out = response(e, input);
    out.put("label", p.label());
    out.put("labelMeaning", "1 = dự đoán có purchase trong 7 ngày sau t0");
    out.put("voteShare", p.voteShare());
    out.put("voteShareNote", "tỷ lệ láng giềng nhãn 1, không phải xác suất đã hiệu chỉnh");
    out.put("k", model.k());
    out.put("threshold", model.threshold());
    out.put("neighbours", p.neighbours());
    if (input.row != null) {
      out.put("actualLabel", Integer.parseInt(input.row.get("label")));
      out.put("notebookVoteShare", Double.parseDouble(input.row.get("vote_share")));
    }
    return out;
  }

  private record Input(double[] vector, Map<String, String> row, Map<String, Double> features, List<String> warnings) {}

  private Input input(Entry e, PredictRequest req, List<String> order, long minViews) {
    if ((req.productId() == null) == (req.raw() == null))
      throw new IllegalArgumentException("Cần đúng một trong hai: productId hoặc raw");
    if (req.productId() != null) {
      Map<String, String> row = product(e, req.productId());
      Map<String, Double> f = new LinkedHashMap<>();
      for (String name : order) f.put(name, Double.parseDouble(row.get(name)));
      return new Input(ProductFeatures.vector(f, order), row, f, List.of());
    }
    ProductFeatures.Raw raw = req.raw().toRaw();
    if (raw.views() <= 0) throw new IllegalArgumentException("Lượt xem phải lớn hơn 0 (các tỷ lệ được tính trên lượt xem)");
    Map<String, Double> f = ProductFeatures.transform(raw);
    return new Input(ProductFeatures.vector(f, order), null, f, ProductFeatures.domainWarnings(raw, minViews));
  }

  private static long minViews(Entry e) {
    JsonNode d = e.metadata().path("dataset");
    return d.path("minViews").asLong(20);
  }

  private static Map<String, Object> response(Entry e, Input input) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("modelRunId", e.modelRunId());
    out.put("algorithm", e.metadata().path("algorithm").asText());
    out.put("features", input.features);
    out.put("outOfDomain", !input.warnings.isEmpty());
    out.put("warnings", input.warnings);
    return out;
  }
}
