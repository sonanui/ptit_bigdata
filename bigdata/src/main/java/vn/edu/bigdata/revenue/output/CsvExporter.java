package vn.edu.bigdata.revenue.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import vn.edu.bigdata.revenue.domain.Money;

public final class CsvExporter {
  private CsvExporter() {}

  public static void export(Path parts, Path csv, FileSystem fs) throws IOException {
    var rows = ResultValidator.read(parts, fs);
    if (fs.exists(csv)) throw new IOException("Export exists: " + csv);
    try (CSVPrinter printer =
        new CSVPrinter(
            new OutputStreamWriter(fs.create(csv, false), StandardCharsets.UTF_8),
            CSVFormat.RFC4180)) {
      printer.printRecord("group_key", "total_revenue", "purchase_count", "average_revenue");
      for (OutputRow row : rows.values())
        printer.printRecord(
            row.groupKey, Money.formatMinor(row.sumMinor), row.purchaseCount, row.averageText);
    }
  }
}
