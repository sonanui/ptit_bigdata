package vn.edu.bigdata.webapp.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vn.edu.bigdata.webapp.serving.ArtifactNotFoundException;
import vn.edu.bigdata.webapp.serving.ServingRepository;

/**
 * Kết quả Big Data đã publish: run, Group By (A1–A5), parity MR/Spark, chất lượng dữ liệu,
 * benchmark. Chỉ đọc artifact; mọi con số đều từ file của pipeline.
 */
@RestController
@RequestMapping("/api")
class AnalyticsController {
  /** Bảng analytics được phép truy vấn qua {@code /tables/{name}} (tên file không đuôi). */
  private static final List<String> TABLES =
      List.of("revenue_by_category", "funnel_by_category", "funnel_by_brand", "trend_by_hour");

  private final ServingRepository serving;

  AnalyticsController(ServingRepository serving) {
    this.serving = serving;
  }

  @GetMapping("/health")
  Map<String, Object> health() {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("servingDir", serving.root().toString());
    List<String> runs = serving.runIds();
    out.put("runs", runs.size());
    out.put("defaultRun", runs.isEmpty() ? null : serving.latestRunId());
    out.put("status", runs.isEmpty() ? "NO_DATA" : "UP");
    return out;
  }

  @GetMapping("/runs")
  List<Map<String, Object>> runs() {
    String latest = serving.runIds().isEmpty() ? null : serving.latestRunId();
    List<Map<String, Object>> out = new ArrayList<>();
    for (String id : serving.runIds()) {
      JsonNode m = serving.manifest(id);
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("runId", id);
      row.put("default", id.equals(latest));
      row.put("createdAt", m.path("createdAt").asText(null));
      row.put("dataset", m.path("dataset"));
      out.add(row);
    }
    return out;
  }

  @GetMapping("/runs/{runId}")
  JsonNode manifest(@PathVariable String runId) {
    return serving.manifest(runId);
  }

  @GetMapping("/analytics/{runId}/tables/{name}")
  Map<String, Object> table(
      @PathVariable String runId,
      @PathVariable String name,
      @RequestParam(required = false) String sort,
      @RequestParam(defaultValue = "desc") String order,
      @RequestParam(required = false) String q,
      @RequestParam(defaultValue = "50") int limit,
      @RequestParam(defaultValue = "0") int offset) {
    if (!TABLES.contains(name)) throw new ArtifactNotFoundException("Không có bảng " + name);
    if (limit < 1 || limit > 5000 || offset < 0)
      throw new IllegalArgumentException("limit trong [1, 5000], offset >= 0");
    List<Map<String, String>> rows = new ArrayList<>(serving.csv(runId, "analytics/" + name + ".csv"));
    if (q != null && !q.isBlank()) {
      String needle = q.toLowerCase(Locale.ROOT);
      rows.removeIf(
          r -> r.values().stream().noneMatch(v -> v != null && v.toLowerCase(Locale.ROOT).contains(needle)));
    }
    if (sort != null && !sort.isBlank()) {
      if (!rows.isEmpty() && !rows.get(0).containsKey(sort))
        throw new IllegalArgumentException("Không có cột " + sort);
      Comparator<Map<String, String>> cmp = Comparator.comparing(r -> sortKey(r.get(sort)));
      rows.sort("asc".equals(order) ? cmp : cmp.reversed());
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("runId", runId);
    out.put("table", name);
    out.put("total", rows.size());
    out.put("rows", rows.subList(Math.min(offset, rows.size()), Math.min(offset + limit, rows.size())));
    return out;
  }

  /** Số so sánh như số (chuỗi rỗng xếp cuối), còn lại so sánh chuỗi. */
  private static SortKey sortKey(String v) {
    if (v == null || v.isEmpty()) return new SortKey(Double.NEGATIVE_INFINITY, "");
    try {
      return new SortKey(Double.parseDouble(v), v);
    } catch (NumberFormatException e) {
      return new SortKey(Double.NaN, v);
    }
  }

  private record SortKey(double number, String text) implements Comparable<SortKey> {
    @Override
    public int compareTo(SortKey o) {
      if (!Double.isNaN(number) && !Double.isNaN(o.number)) return Double.compare(number, o.number);
      return text.compareTo(o.text);
    }
  }

  @GetMapping("/analytics/{runId}/parity")
  JsonNode parity(@PathVariable String runId) {
    return serving.json(runId, "analytics/revenue_parity.json");
  }

  @GetMapping("/analytics/{runId}/quality")
  JsonNode quality(@PathVariable String runId) {
    return serving.json(runId, "analytics/quality.json");
  }

  @GetMapping("/analytics/{runId}/benchmarks")
  List<String> benchmarks(@PathVariable String runId) {
    List<String> out = new ArrayList<>();
    for (JsonNode f : serving.manifest(runId).path("files")) {
      String path = f.path("path").asText();
      if (path.startsWith("benchmarks/") && path.endsWith(".json"))
        out.add(path.substring("benchmarks/".length(), path.length() - ".json".length()));
    }
    return out;
  }

  @GetMapping("/analytics/{runId}/benchmarks/{name}")
  JsonNode benchmark(@PathVariable String runId, @PathVariable String name) {
    if (!name.matches("[0-9A-Za-z._-]+")) throw new ArtifactNotFoundException("Không có benchmark " + name);
    return serving.json(runId, "benchmarks/" + name + ".json");
  }
}
