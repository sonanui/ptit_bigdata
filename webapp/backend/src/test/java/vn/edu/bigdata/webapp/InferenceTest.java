package vn.edu.bigdata.webapp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import vn.edu.bigdata.webapp.ml.KMeansModel;
import vn.edu.bigdata.webapp.ml.KnnModel;
import vn.edu.bigdata.webapp.ml.ProductFeatures;

/** Phép tính suy luận trên ví dụ tính tay; đối chiếu với artifact thật nằm ở ServingParityTest. */
class InferenceTest {
  @Test
  void transformMatchesFeatureJobDefinitions() {
    Map<String, Double> f =
        ProductFeatures.transform(new ProductFeatures.Raw(2, 1, 1, 10.0, 2, 1.0));
    assertEquals(StrictMath.log1p(2), f.get("log_views"));
    assertEquals(0.5, f.get("cart_rate"));
    assertEquals(0.5, f.get("purchase_rate"));
    assertEquals(StrictMath.log1p(10.0), f.get("log_median_price"));
    assertEquals(0.5, f.get("recent_view_share"));
    assertEquals(
        List.of("lượt xem dưới 20, trong khi mô hình chỉ học từ sản phẩm có từ 20 lượt xem trở lên"),
        ProductFeatures.domainWarnings(new ProductFeatures.Raw(2, 1, 1, 10, 2, null), 20));
  }

  @Test
  void standardizeMultipliesByReciprocalLikeSpark() {
    assertArrayEquals(
        new double[] {(3.0 - 1.0) * (1.0 / 3.0), 0.0},
        ProductFeatures.standardize(new double[] {3, 5}, new double[] {1, 2}, new double[] {3, 0}));
  }

  @Test
  void kmeansPicksNearestCenterAndLowerIndexOnTie() {
    KMeansModel m =
        new KMeansModel(
            "r", 2, List.of("a"), new double[] {0}, new double[] {1}, new double[][] {{-1}, {1}});
    assertEquals(1, m.predict(new double[] {0.9}).cluster());
    assertEquals(0, m.predict(new double[] {0.0}).cluster());
    assertArrayEquals(new double[] {1.0, 1.0}, m.predict(new double[] {0.0}).squaredDistances());
  }

  @Test
  void knnVotesOverKNearestWithLowerRowOnTie() {
    // 6 điểm train trùng nhau: 3 láng giềng là dòng 0, 1, 2 -> nhãn 1, 1, 0 (giống ml_common).
    List<Map<String, String>> rows = new ArrayList<>();
    int[] labels = {1, 1, 0, 0, 0, 0};
    for (int i = 0; i < labels.length; i++) {
      Map<String, String> r = new LinkedHashMap<>();
      r.put("row", Integer.toString(i));
      r.put("product_id", "p" + i);
      r.put("label", Integer.toString(labels[i]));
      r.put("z_a", "0.0");
      rows.add(r);
    }
    var json =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .createObjectNode()
            .put("runId", "r")
            .put("k", 3)
            .put("threshold", 0.6)
            .put("trainRows", 6);
    json.putArray("features").add("a");
    json.putArray("scalerMean").add(0.0);
    json.putArray("scalerStd").add(1.0);
    KnnModel model = KnnModel.from(json, rows);
    KnnModel.Prediction p = model.predict(new double[] {0.0});
    assertEquals(2.0 / 3.0, p.voteShare());
    assertEquals(1, p.label());
    assertEquals(List.of("p0", "p1", "p2"), p.neighbours().stream().map(KnnModel.Neighbour::productId).toList());

    json.put("trainRows", 5);
    assertThrows(IllegalStateException.class, () -> KnnModel.from(json, rows));
  }
}
