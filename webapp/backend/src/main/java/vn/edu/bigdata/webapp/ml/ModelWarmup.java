package vn.edu.bigdata.webapp.ml;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import vn.edu.bigdata.webapp.serving.ServingRepository;

/**
 * Nạp sẵn mô hình và bảng sản phẩm của serving run mặc định ngay sau khi ứng dụng sẵn sàng, ở luồng
 * nền: lần dự đoán đầu không phải chờ đọc + kiểm sha256 các CSV lớn (6–9 s đo được trên D3). Chỉ đọc
 * artifact, không huấn luyện. Lỗi chỉ ghi log; API vẫn báo lỗi rõ ràng khi được gọi.
 */
@Component
class ModelWarmup {
  private static final Logger LOG = LoggerFactory.getLogger(ModelWarmup.class);

  private final ServingRepository serving;
  private final ModelRegistry registry;
  private final boolean enabled;

  ModelWarmup(ServingRepository serving, ModelRegistry registry, @Value("${serving.warmup:true}") boolean enabled) {
    this.serving = serving;
    this.registry = registry;
    this.enabled = enabled;
  }

  @EventListener(ApplicationReadyEvent.class)
  void onReady() {
    if (!enabled || serving.runIds().isEmpty()) return;
    Thread thread = new Thread(this::load, "model-warmup");
    thread.setDaemon(true);
    thread.start();
  }

  void load() {
    for (String type : List.of(ModelRegistry.KMEANS, ModelRegistry.KNN)) {
      long started = System.nanoTime();
      try {
        ModelRegistry.Entry entry = registry.find(type, null);
        if (ModelRegistry.KMEANS.equals(type)) {
          registry.kmeans(entry);
          registry.table(entry, "assignments.csv");
        } else {
          registry.knn(entry);
          registry.table(entry, "products.csv");
        }
        LOG.info("Đã nạp sẵn mô hình {} run {} ({} ms)", type, entry.modelRunId(), (System.nanoTime() - started) / 1_000_000);
      } catch (RuntimeException e) {
        LOG.warn("Không nạp sẵn được mô hình {}: {}", type, e.getMessage());
      }
    }
  }
}
