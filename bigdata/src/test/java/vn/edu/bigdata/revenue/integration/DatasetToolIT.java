package vn.edu.bigdata.revenue.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.edu.bigdata.revenue.cli.DatasetTool;
import vn.edu.bigdata.revenue.input.CsvEventParser;

class DatasetToolIT {
  @TempDir java.nio.file.Path dir;

  @Test
  void samplingIsDeterministicAndPreservesHeaderAndNonPurchase() throws Exception {
    java.nio.file.Path csv = dir.resolve("events.csv"), manifest = dir.resolve("manifest.json");
    Files.writeString(
        csv, CsvEventParser.HEADER + "\nt,view,1,1,a,b,1,u,s\nt,purchase,1,1,a,b,1,u,s\n");
    DatasetTool tool = new DatasetTool();
    tool.setConf(new Configuration());
    assertEquals(
        0,
        tool.run(
            new String[] {
              "preflight", "--input", csv.toString(), "--manifest", manifest.toString()
            }));
    for (int i = 0; i < 2; i++)
      assertEquals(
          0,
          tool.run(
              new String[] {
                "sample",
                "--manifest",
                manifest.toString(),
                "--output",
                dir.resolve("sample" + i + ".csv").toString(),
                "--rate",
                "1",
                "--seed",
                "21"
              }));
    assertEquals(
        Files.readString(dir.resolve("sample0.csv")), Files.readString(dir.resolve("sample1.csv")));
    assertTrue(Files.readString(dir.resolve("sample0.csv")).contains(",view,"));
  }
}
