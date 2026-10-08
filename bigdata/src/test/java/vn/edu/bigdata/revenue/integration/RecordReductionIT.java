package vn.edu.bigdata.revenue.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.edu.bigdata.revenue.cli.RevenueTool;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.*;
import vn.edu.bigdata.revenue.metrics.RunManifest;
import vn.edu.bigdata.revenue.output.ResultValidator;
import vn.edu.bigdata.revenue.profile.*;

class RecordReductionIT {
  @TempDir java.nio.file.Path dir;

  @Test
  void optimizationReducesRecordsWithoutChangingProblemOrAddingJobs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", Long.MAX_VALUE);
    conf.setInt("mapreduce.task.io.sort.mb", 8);
    conf.setInt("mapreduce.client.completion.pollinterval", 100);
    FileSystem fs = FileSystem.getLocal(conf);
    java.nio.file.Path csv = dir.resolve("input.csv");
    StringBuilder data = new StringBuilder(CsvEventParser.HEADER).append('\n');
    for (int i = 0; i < 600; i++)
      data.append("t,purchase,1,").append(i % 3).append(",electronics.phone,b,1.00,u,s\n");
    Files.writeString(csv, data.toString());
    Path input = new Path(csv.toUri());
    var p = DatasetPreflight.inspect(List.of(input), fs, GroupMode.CATEGORY_ID);
    Path manifest = new Path(dir.resolve("m.json").toUri()),
        report = new Path(dir.resolve("p.json").toUri()),
        dictionary = new Path(dir.resolve("d.json").toUri());
    JsonArtifacts.write(manifest, fs, p.manifest);
    JsonArtifacts.write(report, fs, p);
    JsonArtifacts.write(
        dictionary,
        fs,
        GroupDictionary.from(
            DatasetProfiler.profile(List.of(input), fs, GroupMode.CATEGORY_ID, 100)));
    long[] expectedEmits = {600, 600, 3, 3, 2};
    for (int i = 0; i < 5; i++) {
      Path output = new Path(dir.resolve("v" + (i + 1)).toUri());
      RevenueTool tool = new RevenueTool();
      tool.setConf(conf);
      assertEquals(
          0,
          tool.run(
              new String[] {
                "--variant",
                "v" + (i + 1),
                "--manifest",
                manifest.toString(),
                "--preflight",
                report.toString(),
                "--dictionary",
                dictionary.toString(),
                "--output",
                output.toString(),
                "--reducers",
                "2"
              }));
      var run = JsonArtifacts.read(new Path(output, "run-manifest.json"), fs, RunManifest.class);
      assertEquals(1, run.stages.size());
      assertEquals(
          expectedEmits[i],
          run.stages
              .get(0)
              .counters
              .get("org.apache.hadoop.mapreduce.TaskCounter.MAP_OUTPUT_RECORDS"));
      var rows = ResultValidator.read(output, fs);
      assertEquals(3, rows.size());
      for (var row : rows.values()) {
        assertEquals(20000, row.sumMinor);
        assertEquals(200, row.purchaseCount);
        assertEquals("1.00", row.averageText);
      }
    }
  }
}
