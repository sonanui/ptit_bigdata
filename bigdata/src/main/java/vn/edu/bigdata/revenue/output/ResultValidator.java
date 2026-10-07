package vn.edu.bigdata.revenue.output;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class ResultValidator {
  private ResultValidator() {}

  /** Rechecks provenance after jobs; counts alone cannot detect a changed price. */
  public static ValidationReport validateInput(
      vn.edu.bigdata.revenue.input.InputManifest manifest, FileSystem fs) {
    ValidationReport report = new ValidationReport();
    try {
      for (var entry : manifest.files) {
        var current =
            vn.edu.bigdata.revenue.input.BoundedLineReader.scan(
                new Path(entry.uri), fs, (offset, line) -> {});
        if (current.bytes != entry.bytes || !current.sha256.equals(entry.sha256))
          report.fail("Input changed during execution: " + entry.uri);
      }
    } catch (Exception e) {
      report.fail(e.toString());
    }
    return report;
  }

  public static Map<String, OutputRow> read(Path output, FileSystem fs) throws IOException {
    Map<String, OutputRow> rows = new TreeMap<>();
    FileStatus[] parts = fs.globStatus(new Path(output, "part-r-*"));
    if (parts == null || parts.length == 0) throw new IOException("Missing output parts");
    for (FileStatus part : parts)
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(fs.open(part.getPath()), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          OutputRow row = OutputRow.parseTsv(line);
          if (rows.putIfAbsent(row.groupKey, row) != null)
            throw new IllegalArgumentException("Duplicate output key: " + row.groupKey);
          if (rows.size() > 100000)
            throw new IOException("Output cardinality exceeds 100000 guard");
        }
      }
    return rows;
  }

  public static ValidationReport validate(Path output, FileSystem fs, long expectedCount) {
    ValidationReport report = new ValidationReport();
    try {
      Map<String, OutputRow> rows = read(output, fs);
      report.groupCount = rows.size();
      for (OutputRow row : rows.values()) {
        report.totalCount = Math.addExact(report.totalCount, row.purchaseCount);
        report.totalSumMinor = Math.addExact(report.totalSumMinor, row.sumMinor);
      }
      if (report.totalCount != expectedCount)
        report.fail("Purchase count mismatch: " + report.totalCount + " != " + expectedCount);
    } catch (Exception e) {
      report.fail(e.toString());
    }
    return report;
  }

  public static ValidationReport compare(Path a, Path b, FileSystem fs) {
    ValidationReport report = new ValidationReport();
    try {
      Map<String, OutputRow> left = read(a, fs), right = read(b, fs);
      if (!left.keySet().equals(right.keySet())) report.fail("Group keys differ");
      for (String key : left.keySet()) {
        OutputRow x = left.get(key), y = right.get(key);
        if (y == null || x.sumMinor != y.sumMinor || x.purchaseCount != y.purchaseCount)
          report.fail("Aggregate differs: " + key);
      }
    } catch (Exception e) {
      report.fail(e.toString());
    }
    return report;
  }
}
