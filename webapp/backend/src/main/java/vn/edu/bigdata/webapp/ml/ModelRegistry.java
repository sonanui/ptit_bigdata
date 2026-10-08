package vn.edu.bigdata.webapp.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;
import vn.edu.bigdata.webapp.serving.ArtifactException;
import vn.edu.bigdata.webapp.serving.ArtifactNotFoundException;
import vn.edu.bigdata.webapp.serving.ServingRepository;

/**
 * Danh mục mô hình đã publish: mỗi serving run có tối đa một mô hình mỗi loại tại {@code
 * ml/<type>/}. Mô hình được định danh bằng run_id huấn luyện (trong metadata.json), không phải
 * run_id serving. Nạp lười và cache; không bao giờ huấn luyện.
 */
@Service
public class ModelRegistry {
  public static final String KMEANS = "kmeans";
  public static final String KNN = "knn";

  private final ServingRepository serving;
  private final Map<String, Object> loaded = new ConcurrentHashMap<>();

  public ModelRegistry(ServingRepository serving) {
    this.serving = serving;
  }

  /**
   * Mô hình đã publish của một loại, mới nhất trước. Một mô hình có thể được publish lại trong nhiều
   * serving run; chỉ giữ bản trong serving run mới nhất để mỗi run huấn luyện xuất hiện một lần.
   */
  public List<Entry> list(String type) {
    List<Entry> out = new ArrayList<>();
    java.util.Set<String> seen = new java.util.HashSet<>();
    for (String runId : serving.runIds()) {
      String path = "ml/" + type + "/metadata.json";
      if (serving.has(runId, path)) {
        JsonNode meta = serving.json(runId, path);
        String modelRunId = meta.path("run_id").asText();
        if (seen.add(modelRunId)) out.add(new Entry(modelRunId, runId, type, meta));
      }
    }
    return out;
  }

  /** {@code modelRunId} rỗng = mô hình trong serving run mặc định (_LATEST). */
  public Entry find(String type, String modelRunId) {
    if (modelRunId == null || modelRunId.isBlank()) {
      String latest = serving.latestRunId();
      return list(type).stream()
          .filter(e -> e.servingRunId().equals(latest))
          .findFirst()
          .orElseThrow(
              () -> new ArtifactNotFoundException("Run " + latest + " chưa publish mô hình " + type));
    }
    return list(type).stream()
        .filter(e -> e.modelRunId().equals(modelRunId))
        .findFirst()
        .orElseThrow(
            () -> new ArtifactNotFoundException("Không có mô hình " + type + " run " + modelRunId));
  }

  public JsonNode artifact(Entry entry, String file) {
    return serving.json(entry.servingRunId(), "ml/" + entry.type() + "/" + file);
  }

  public List<Map<String, String>> table(Entry entry, String file) {
    return serving.csv(entry.servingRunId(), "ml/" + entry.type() + "/" + file);
  }

  public KMeansModel kmeans(Entry entry) {
    return (KMeansModel)
        loaded.computeIfAbsent(
            KMEANS + entry.servingRunId(),
            k -> load(entry, () -> KMeansModel.from(artifact(entry, "model.json"))));
  }

  public KnnModel knn(Entry entry) {
    return (KnnModel)
        loaded.computeIfAbsent(
            KNN + entry.servingRunId(),
            k ->
                load(
                    entry,
                    () -> KnnModel.from(artifact(entry, "model.json"), table(entry, "train_set.csv"))));
  }

  /** Artifact mô hình sai cấu trúc là lỗi phía dữ liệu đã publish (503), không phải lỗi request. */
  private static Object load(Entry entry, java.util.function.Supplier<Object> loader) {
    try {
      return loader.get();
    } catch (IllegalStateException | IllegalArgumentException | NullPointerException e) {
      throw new ArtifactException(
          "Artifact mô hình " + entry.type() + " của run " + entry.servingRunId() + " lỗi: " + e.getMessage(), e);
    }
  }

  public record Entry(String modelRunId, String servingRunId, String type, JsonNode metadata) {}
}
