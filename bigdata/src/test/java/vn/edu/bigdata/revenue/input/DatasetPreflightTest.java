package vn.edu.bigdata.revenue.input;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.edu.bigdata.revenue.domain.GroupMode;

class DatasetPreflightTest {
  @TempDir java.nio.file.Path temp;

  @Test
  void rejectsQuotedCarriageReturnBeforeHadoopSplitsIt() throws Exception {
    java.nio.file.Path csv = temp.resolve("cr.csv");
    Files.writeString(csv, CsvEventParser.HEADER + "\nt,purchase,1,1,a,\"two\rlines\",1,u,s\n");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    assertFalse(
        DatasetPreflight.inspect(List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_ID)
            .isValid());
  }

  @Test
  void detectsMutatedInputAndRefusesMultilineAndDuplicates() throws Exception {
    java.nio.file.Path csv = temp.resolve("events.csv");
    Files.writeString(csv, CsvEventParser.HEADER + "\nt,purchase,1,1,a,b,1,u,s\n");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path path = new Path(csv.toUri());
    PreflightReport report = DatasetPreflight.inspect(List.of(path), fs, GroupMode.CATEGORY_ID);
    assertTrue(report.isValid());
    assertEquals(1, report.validPurchaseCount);
    String hash = report.manifest.fingerprint();
    Files.writeString(csv, CsvEventParser.HEADER + "\nt,purchase,1,1,a,b,2,u,s\n");
    assertNotEquals(
        hash,
        DatasetPreflight.inspect(List.of(path), fs, GroupMode.CATEGORY_ID).manifest.fingerprint());
    assertThrows(
        IllegalArgumentException.class,
        () -> DatasetPreflight.inspect(List.of(path, path), fs, GroupMode.CATEGORY_ID));
    Files.writeString(csv, CsvEventParser.HEADER + "\nt,purchase,1,1,a,\"two\nlines\",2,u,s\n");
    assertFalse(DatasetPreflight.inspect(List.of(path), fs, GroupMode.CATEGORY_ID).isValid());
  }

  @Test
  void refusesMissingHeaderAndLongKeys() throws Exception {
    java.nio.file.Path csv = temp.resolve("bad.csv");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Files.writeString(csv, "t,purchase,1,1,a,b,1,u,s\n");
    assertFalse(
        DatasetPreflight.inspect(List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_ID)
            .isValid());
    Files.writeString(
        csv, CsvEventParser.HEADER + "\nt,purchase,1,1," + "a".repeat(257) + ",b,1,u,s\n");
    assertFalse(
        DatasetPreflight.inspect(List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_CODE)
            .isValid());
  }
}
