package vn.edu.bigdata.webapp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import vn.edu.bigdata.webapp.ml.KMeansModel;
import vn.edu.bigdata.webapp.ml.KnnModel;
import vn.edu.bigdata.webapp.ml.ModelRegistry;
import vn.edu.bigdata.webapp.ml.ProductFeatures;
import vn.edu.bigdata.webapp.serving.ServingRepository;

/**
 * Suy luận Java khớp kết quả của pipeline trên artifact thật (thư mục serving đã sync, mặc định
 * ../../serving): K-Means khớp cụm Spark gán cho mọi sản phẩm; KNN khớp vote_share của notebook cho
 * mọi sản phẩm test. Bỏ qua (không fail) khi máy chưa có serving run.
 */
class ServingParityTest {
  private static ModelRegistry registry() {
    Path dir = Path.of(System.getenv().getOrDefault("SERVING_DIR", "../../serving"));
    Assumptions.assumeTrue(Files.isDirectory(dir), "Chưa có thư mục serving " + dir.toAbsolutePath());
    ServingRepository serving = new ServingRepository(dir.toString(), new ObjectMapper());
    Assumptions.assumeFalse(serving.runIds().isEmpty(), "Chưa có serving run");
    return new ModelRegistry(serving);
  }

  @Test
  void kmeansMatchesSparkAssignments() {
    ModelRegistry registry = registry();
    List<ModelRegistry.Entry> models = registry.list(ModelRegistry.KMEANS);
    Assumptions.assumeFalse(models.isEmpty(), "Chưa publish K-Means");
    ModelRegistry.Entry e = models.get(0);
    KMeansModel model = registry.kmeans(e);
    List<Map<String, String>> rows = registry.table(e, "assignments.csv");
    int mismatches = 0;
    for (Map<String, String> row : rows) {
      double[] x = new double[model.features().size()];
      for (int j = 0; j < x.length; j++) x[j] = Double.parseDouble(row.get(model.features().get(j)));
      if (model.predict(x).cluster() != Integer.parseInt(row.get("cluster"))) mismatches++;
    }
    System.out.printf("K-Means parity: %d/%d khớp (run %s)%n", rows.size() - mismatches, rows.size(), e.modelRunId());
    assertEquals(0, mismatches);
  }

  @Test
  void kmeansRecomputesFeaturesFromRawCounts() {
    ModelRegistry registry = registry();
    List<ModelRegistry.Entry> models = registry.list(ModelRegistry.KMEANS);
    Assumptions.assumeFalse(models.isEmpty(), "Chưa publish K-Means");
    ModelRegistry.Entry e = models.get(0);
    KMeansModel model = registry.kmeans(e);
    double worst = 0;
    for (Map<String, String> row : registry.table(e, "assignments.csv")) {
      Map<String, Double> f =
          ProductFeatures.transform(
              new ProductFeatures.Raw(
                  Double.parseDouble(row.get("views")),
                  Double.parseDouble(row.get("carts")),
                  Double.parseDouble(row.get("purchases")),
                  Double.parseDouble(row.get("median_price")),
                  Double.parseDouble(row.get("distinct_users")),
                  null));
      for (String name : model.features())
        worst = Math.max(worst, Math.abs(f.get(name) - Double.parseDouble(row.get(name))));
    }
    System.out.println("Sai lệch lớn nhất đặc trưng tính lại từ số đếm thô: " + worst);
    assertTrue(worst < 1e-12, "sai lệch " + worst);
  }

  @Test
  void knnMatchesNotebookVoteShares() {
    ModelRegistry registry = registry();
    List<ModelRegistry.Entry> models = registry.list(ModelRegistry.KNN);
    Assumptions.assumeFalse(models.isEmpty(), "Chưa publish KNN");
    ModelRegistry.Entry e = models.get(0);
    KnnModel model = registry.knn(e);
    List<Map<String, String>> rows = registry.table(e, "products.csv");
    int mismatches = 0;
    for (Map<String, String> row : rows) {
      double[] x = new double[model.features().size()];
      for (int j = 0; j < x.length; j++) x[j] = Double.parseDouble(row.get(model.features().get(j)));
      KnnModel.Prediction p = model.predict(x);
      if (p.voteShare() != Double.parseDouble(row.get("vote_share"))
          || p.label() != Integer.parseInt(row.get("prediction"))) mismatches++;
    }
    System.out.printf("KNN parity: %d/%d khớp (run %s)%n", rows.size() - mismatches, rows.size(), e.modelRunId());
    assertEquals(0, mismatches);
  }
}
