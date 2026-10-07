package vn.edu.bigdata.webapp.ml;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tái hiện phép biến đổi của ProductFeaturesJob (A7) và ProductLabelJob (A8) từ số đếm thô sang đặc
 * trưng, để người dùng nhập số đếm thay vì đặc trưng đã log. Spark dùng {@code StrictMath.log1p} cho
 * hàm log1p và chia double cho tỷ lệ; ở đây làm đúng như vậy.
 */
public final class ProductFeatures {
  /** Số đếm thô; {@code recentViews} chỉ dùng cho A8 (KNN). */
  public record Raw(
      double views,
      double carts,
      double purchases,
      double medianPrice,
      double distinctUsers,
      Double recentViews) {}

  private ProductFeatures() {}

  public static Map<String, Double> transform(Raw raw) {
    Map<String, Double> f = new LinkedHashMap<>();
    f.put("log_views", StrictMath.log1p(raw.views()));
    f.put("log_carts", StrictMath.log1p(raw.carts()));
    f.put("log_purchases", StrictMath.log1p(raw.purchases()));
    f.put("cart_rate", raw.carts() / raw.views());
    f.put("purchase_rate", raw.purchases() / raw.views());
    f.put("log_median_price", StrictMath.log1p(raw.medianPrice()));
    f.put("log_distinct_users", StrictMath.log1p(raw.distinctUsers()));
    if (raw.recentViews() != null) f.put("recent_view_share", raw.recentViews() / raw.views());
    return f;
  }

  /** Lý do đầu vào nằm ngoài miền mà mô hình được huấn luyện (vẫn dự đoán, nhưng phải cảnh báo). */
  public static List<String> domainWarnings(Raw raw, long minViews) {
    List<String> out = new java.util.ArrayList<>();
    if (raw.views() < minViews)
      out.add("lượt xem dưới " + minViews + ", trong khi mô hình chỉ học từ sản phẩm có từ " + minViews + " lượt xem trở lên");
    if (raw.carts() > raw.views() || raw.purchases() > raw.views())
      out.add("lượt thêm giỏ hoặc lượt mua nhiều hơn lượt xem, trường hợp rất hiếm trong dữ liệu huấn luyện");
    if (raw.recentViews() != null && raw.recentViews() > raw.views())
      out.add("lượt xem 7 ngày gần nhất nhiều hơn tổng lượt xem");
    return out;
  }

  /** Vector theo đúng thứ tự đặc trưng của mô hình; thiếu đặc trưng nào thì báo lỗi. */
  public static double[] vector(Map<String, Double> features, List<String> order) {
    double[] x = new double[order.size()];
    for (int i = 0; i < x.length; i++) {
      Double v = features.get(order.get(i));
      if (v == null || v.isNaN() || v.isInfinite())
        throw new IllegalArgumentException("Thiếu hoặc không hợp lệ đặc trưng " + order.get(i));
      x[i] = v;
    }
    return x;
  }

  /**
   * StandardScalerModel của Spark (withMean, withStd): (x - mean) * (1 / std), std = 0 thì 0.
   * Nhân với nghịch đảo (không chia) để khớp phép tính của Spark.
   */
  public static double[] standardize(double[] x, double[] mean, double[] std) {
    double[] z = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      double scale = std[i] == 0.0 ? 0.0 : 1.0 / std[i];
      z[i] = (x[i] - mean[i]) * scale;
    }
    return z;
  }

  /** Euclidean bình phương, cộng theo thứ tự cột (giống ml_common.knn_vote_share). */
  public static double squaredDistance(double[] a, double[] b) {
    double d = 0.0;
    for (int j = 0; j < a.length; j++) {
      double diff = a[j] - b[j];
      d += diff * diff;
    }
    return d;
  }
}
