package vn.edu.bigdata.revenue.output;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResultValidatorTest {
  @TempDir java.nio.file.Path dir;

  @Test
  void rejectsInputPriceMutationEvenWhenPurchaseCountIsUnchanged() throws Exception {
    java.nio.file.Path csv = dir.resolve("source.csv");
    String header = vn.edu.bigdata.revenue.input.CsvEventParser.HEADER;
    Files.writeString(csv, header + "\nt,purchase,1,1,a,b,1,u,s\n");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    var original =
        vn.edu.bigdata.revenue.input.DatasetPreflight.inspect(
            java.util.List.of(new Path(csv.toUri())),
            fs,
            vn.edu.bigdata.revenue.domain.GroupMode.CATEGORY_ID);
    assertTrue(ResultValidator.validateInput(original.manifest, fs).valid);
    Files.writeString(csv, header + "\nt,purchase,1,1,a,b,2,u,s\n");
    assertFalse(ResultValidator.validateInput(original.manifest, fs).valid);
  }

  @Test
  void catchesDuplicatesWrongAverageAndMissingCounts() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path output = new Path(dir.toUri());
    java.nio.file.Path part = dir.resolve("part-r-00000");
    Files.writeString(part, "A\t60.00\t3\t20.00\n");
    assertTrue(ResultValidator.validate(output, fs, 3).valid);
    assertFalse(ResultValidator.validate(output, fs, 4).valid);
    Files.writeString(part, "A\t60.00\t3\t20.00\nA\t60.00\t3\t20.00\n");
    assertFalse(ResultValidator.validate(output, fs, 6).valid);
    Files.writeString(part, "A\t60.00\t3\t10.00\n");
    assertFalse(ResultValidator.validate(output, fs, 3).valid);
    Files.writeString(part, "");
    assertTrue(ResultValidator.validate(output, fs, 0).valid);
  }
}
