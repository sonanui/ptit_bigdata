package vn.edu.bigdata.webapp.serving;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * Đọc serving artifacts theo hợp đồng ở plan §18.4: {@code <serving.dir>/<runId>/manifest.json}
 * liệt kê mọi file kèm sha256. Mỗi file chỉ được đọc nếu có trong manifest và checksum khớp (kiểm
 * một lần rồi cache nội dung). Không ghi gì vào thư mục serving.
 */
@Repository
public class ServingRepository {
  private static final Pattern RUN_ID = Pattern.compile("[0-9A-Za-z._-]{1,128}");
  private final Path root;
  private final ObjectMapper mapper;
  // Hai map riêng: nạp một artifact (computeIfAbsent trên cache) cần đọc manifest; dùng chung một
  // ConcurrentHashMap sẽ thành computeIfAbsent lồng nhau, có thể ném "Recursive update".
  private final Map<String, JsonNode> manifests = new ConcurrentHashMap<>();
  private final Map<String, Object> cache = new ConcurrentHashMap<>();

  public ServingRepository(@Value("${serving.dir}") String dir, ObjectMapper mapper) {
    this.root = Path.of(dir).toAbsolutePath().normalize();
    this.mapper = mapper;
  }

  public Path root() {
    return root;
  }

  /** Các run có manifest.json, mới nhất trước (run_id bắt đầu bằng thời gian). */
  public List<String> runIds() {
    if (!Files.isDirectory(root)) return List.of();
    try (Stream<Path> dirs = Files.list(root)) {
      return dirs.filter(d -> Files.isRegularFile(d.resolve("manifest.json")))
          .map(d -> d.getFileName().toString())
          .filter(id -> RUN_ID.matcher(id).matches())
          .sorted((a, b) -> b.compareTo(a))
          .toList();
    } catch (IOException e) {
      throw new ArtifactException("Không đọc được thư mục serving " + root, e);
    }
  }

  /** run trong {@code _LATEST}; nếu không có thì run mới nhất. */
  public String latestRunId() {
    Path latest = root.resolve("_LATEST");
    try {
      if (Files.isRegularFile(latest)) {
        String id = Files.readString(latest, StandardCharsets.UTF_8).trim();
        if (RUN_ID.matcher(id).matches() && runIds().contains(id)) return id;
      }
    } catch (IOException e) {
      throw new ArtifactException("Không đọc được " + latest, e);
    }
    List<String> ids = runIds();
    if (ids.isEmpty()) throw new ArtifactNotFoundException("Chưa có serving run nào trong " + root);
    return ids.get(0);
  }

  public JsonNode manifest(String runId) {
    return manifests.computeIfAbsent(
            runId,
            k -> {
              try {
                return mapper.readTree(runDir(runId).resolve("manifest.json").toFile());
              } catch (IOException e) {
                throw new ArtifactException("manifest.json lỗi trong run " + runId, e);
              }
            });
  }

  public boolean has(String runId, String relative) {
    return entry(runId, relative) != null;
  }

  public JsonNode json(String runId, String relative) {
    return (JsonNode)
        cache.computeIfAbsent(
            runId + "\u0000" + relative,
            k -> {
              try (InputStream in = Files.newInputStream(verified(runId, relative))) {
                return mapper.readTree(in);
              } catch (IOException e) {
                throw new ArtifactException("Không đọc được " + relative, e);
              }
            });
  }

  /** CSV có header; mỗi dòng là map cột -> giá trị chuỗi, giữ thứ tự cột. */
  @SuppressWarnings("unchecked")
  public List<Map<String, String>> csv(String runId, String relative) {
    return (List<Map<String, String>>)
        cache.computeIfAbsent(
            runId + "\u0000" + relative,
            k -> {
              CSVFormat format =
                  CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true).get();
              try (Reader reader =
                      Files.newBufferedReader(verified(runId, relative), StandardCharsets.UTF_8);
                  CSVParser parser = CSVParser.parse(reader, format)) {
                List<Map<String, String>> rows = new ArrayList<>();
                for (CSVRecord r : parser) rows.add(new LinkedHashMap<>(r.toMap()));
                return List.copyOf(rows);
              } catch (IOException e) {
                throw new ArtifactException("Không đọc được " + relative, e);
              }
            });
  }

  private Path runDir(String runId) {
    if (!RUN_ID.matcher(runId).matches())
      throw new ArtifactNotFoundException("run_id không hợp lệ: " + runId);
    Path dir = root.resolve(runId).normalize();
    if (!dir.startsWith(root) || !Files.isRegularFile(dir.resolve("manifest.json")))
      throw new ArtifactNotFoundException("Không có serving run " + runId);
    return dir;
  }

  private JsonNode entry(String runId, String relative) {
    for (JsonNode f : manifest(runId).path("files"))
      if (relative.equals(f.path("path").asText())) return f;
    return null;
  }

  /** Đường dẫn file đã kiểm: có trong manifest, nằm trong thư mục run, sha256 khớp. */
  private Path verified(String runId, String relative) {
    JsonNode entry = entry(runId, relative);
    if (entry == null)
      throw new ArtifactNotFoundException("Run " + runId + " không có artifact " + relative);
    Path dir = runDir(runId);
    Path file = dir.resolve(relative).normalize();
    if (!file.startsWith(dir) || !Files.isRegularFile(file))
      throw new ArtifactException("Thiếu file " + relative + " trong run " + runId, null);
    String expected = entry.path("sha256").asText();
    String actual = sha256(file);
    if (!actual.equals(expected))
      throw new ArtifactException(
          "Checksum sai cho " + relative + ": manifest " + expected + ", thực tế " + actual, null);
    return file;
  }

  static String sha256(Path file) {
    try (InputStream in =
        new DigestInputStream(Files.newInputStream(file), MessageDigest.getInstance("SHA-256"))) {
      in.transferTo(java.io.OutputStream.nullOutputStream());
      return HexFormat.of().formatHex(((DigestInputStream) in).getMessageDigest().digest());
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new ArtifactException("Không tính được sha256 của " + file, e);
    }
  }
}
