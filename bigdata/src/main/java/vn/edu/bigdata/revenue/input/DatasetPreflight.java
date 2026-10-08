package vn.edu.bigdata.revenue.input;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import vn.edu.bigdata.revenue.domain.GroupMode;

public final class DatasetPreflight {
  private DatasetPreflight() {}

  public static PreflightReport inspect(List<Path> paths, FileSystem fs, GroupMode mode)
      throws IOException {
    return inspect(paths, fs, mode, (offset, line) -> {});
  }

  public static PreflightReport inspect(
      List<Path> paths, FileSystem fs, GroupMode mode, BoundedLineReader.Visitor observer)
      throws IOException {
    if (paths.isEmpty()) throw new IllegalArgumentException("No input files");
    PreflightReport report = new PreflightReport();
    report.groupMode = mode.name();
    long start = System.nanoTime();
    Set<String> seen = new HashSet<>();
    CsvEventParser parser = new CsvEventParser();
    for (Path path : paths) {
      String uri = fs.makeQualified(path).toUri().normalize().toString();
      if (!seen.add(uri)) throw new IllegalArgumentException("Duplicate input: " + uri);
      if (!fs.isFile(path) || !path.getName().endsWith(".csv"))
        throw new IllegalArgumentException("Input must be uncompressed .csv: " + path);
      boolean[] hasHeader = {false};
      InputManifest.Entry entry =
          BoundedLineReader.scan(
              path,
              fs,
              (offset, line) -> {
                report.lines++;
                ParseResult parsed = parser.parse(line);
                if (offset == 0) {
                  hasHeader[0] = parsed.kind == ParseResult.Kind.HEADER;
                  if (!hasHeader[0]) error(report, "Missing/wrong header: " + path);
                }
                if (parsed.kind == ParseResult.Kind.MALFORMED)
                  error(report, "Malformed physical CSV line: " + path + "@" + offset);
                if (parsed.kind == ParseResult.Kind.EVENT) {
                  String rawGroup =
                      mode == GroupMode.CATEGORY_ID
                          ? parsed.event.categoryId
                          : parsed.event.categoryCode;
                  if (rawGroup.getBytes(StandardCharsets.UTF_8).length > 256)
                    error(report, "Group field exceeds 256 bytes: " + path + "@" + offset);
                }
                PreparationResult prepared = PurchasePreparation.prepare(parsed, mode);
                if (prepared.valid())
                  report.validPurchaseCount = Math.addExact(report.validPurchaseCount, 1);
                observer.accept(offset, line);
              });
      if (!hasHeader[0]) error(report, "No header in file: " + path);
      report.manifest.files.add(entry);
    }
    report.inputFingerprint = report.manifest.fingerprint();
    report.elapsedMillis = (System.nanoTime() - start) / 1000000;
    return report;
  }

  private static void error(PreflightReport report, String error) {
    if (report.errors.size() < 20) report.errors.add(error);
  }

  public static List<Path> listCsv(Path input, FileSystem fs) throws IOException {
    List<Path> paths = new ArrayList<>();
    if (fs.isFile(input)) paths.add(input);
    else
      for (FileStatus status : fs.listStatus(input))
        if (status.isFile() && status.getPath().getName().endsWith(".csv"))
          paths.add(status.getPath());
    paths.sort(Comparator.comparing(Path::toString));
    if (paths.isEmpty()) throw new IOException("No .csv files: " + input);
    return paths;
  }
}
