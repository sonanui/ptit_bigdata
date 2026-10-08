package vn.edu.bigdata.revenue.profile;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.CsvEventParser;
import vn.edu.bigdata.revenue.input.DatasetPreflight;
import vn.edu.bigdata.revenue.input.InputManifest;
import vn.edu.bigdata.revenue.input.PreparationResult;
import vn.edu.bigdata.revenue.input.PurchasePreparation;

public final class DatasetProfiler {
  private DatasetProfiler() {}

  public static ProfileReport profile(List<Path> paths, FileSystem fs, GroupMode mode, int maxKeys)
      throws IOException {
    if (maxKeys < 1) throw new IllegalArgumentException("maxProfileKeys must be positive");
    ProfileReport report = new ProfileReport();
    CsvEventParser parser = new CsvEventParser();
    report.preflight =
        DatasetPreflight.inspect(
            paths,
            fs,
            mode,
            (offset, line) -> {
              PreparationResult p = PurchasePreparation.prepare(parser.parse(line), mode);
              report.counters.merge(p.reason().name(), 1L, Math::addExact);
              if (p.valid()) {
                if (!report.countByGroup.containsKey(p.group())
                    && report.countByGroup.size() >= maxKeys)
                  throw new IOException("Profile cardinality exceeds guard");
                report.countByGroup.merge(p.group(), 1L, Math::addExact);
              }
            });
    if (!report.preflight.isValid())
      throw new IOException("Preflight failed: " + report.preflight.errors);
    report.groupMode = mode.name();
    report.validPurchaseCount = report.preflight.validPurchaseCount;
    report.inputFingerprint = report.preflight.inputFingerprint;
    report.policyHash = InputManifest.policyHash();
    report.elapsedMillis = report.preflight.elapsedMillis;
    return report;
  }
}
