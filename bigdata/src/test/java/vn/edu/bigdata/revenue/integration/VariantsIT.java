package vn.edu.bigdata.revenue.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.edu.bigdata.revenue.cli.RevenueTool;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.CsvEventParser;
import vn.edu.bigdata.revenue.input.DatasetPreflight;
import vn.edu.bigdata.revenue.input.JsonArtifacts;
import vn.edu.bigdata.revenue.input.PreflightReport;
import vn.edu.bigdata.revenue.profile.DatasetProfiler;
import vn.edu.bigdata.revenue.profile.GroupDictionary;
import vn.edu.bigdata.revenue.profile.ProfileReport;

class VariantsIT {
  @TempDir java.nio.file.Path dir;

  @Test
  void allVariantsMatchHandCheckedOracleWithMultipleSplitsAndFlushes() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set("mapreduce.framework.name", "local");
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 200);
    conf.setInt("mapreduce.task.io.sort.mb", 8);
    FileSystem fs = FileSystem.getLocal(conf);
    java.nio.file.Path csv = dir.resolve("events.csv");
    Files.writeString(
        csv,
        CsvEventParser.HEADER
            + "\nt,purchase,1,1,a,b,10,u,s\nt,purchase,1,1,a,b,20,u,s\nt,purchase,1,1,a,b,30,u,s\nt,view,1,1,a,b,99,u,s\nt,purchase,1,2,a,b,0,u,s\nt,purchase,1,2,a,b,0.01,u,s\nt,purchase,1,2,a,b,0.02,u,s\n");
    Path manifest = new Path(dir.resolve("input.json").toUri()),
        preflight = new Path(dir.resolve("preflight.json").toUri());
    PreflightReport report =
        DatasetPreflight.inspect(List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_ID);
    JsonArtifacts.write(manifest, fs, report.manifest);
    JsonArtifacts.write(preflight, fs, report);
    ProfileReport profile =
        DatasetProfiler.profile(List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_ID, 100);
    GroupDictionary dictionary = GroupDictionary.from(profile);
    Path artifact = new Path(dir.resolve("dictionary.json").toUri());
    JsonArtifacts.write(artifact, fs, dictionary);
    for (String variant : List.of("v1", "v2", "v3", "v4", "v5")) {
      RevenueTool tool = new RevenueTool();
      tool.setConf(conf);
      java.nio.file.Path output = dir.resolve(variant);
      assertEquals(
          0,
          tool.run(
              new String[] {
                "--variant",
                variant,
                "--manifest",
                manifest.toString(),
                "--preflight",
                preflight.toString(),
                "--output",
                output.toString(),
                "--reducers",
                "2",
                "--max-keys",
                "1",
                "--dictionary",
                artifact.toString()
              }),
          variant);
      Set<String> rows = new TreeSet<>();
      try (var paths = Files.list(output)) {
        for (java.nio.file.Path p :
            (Iterable<java.nio.file.Path>)
                paths.filter(p -> p.getFileName().toString().startsWith("part-r-"))::iterator)
          rows.addAll(Files.readAllLines(p));
      }
      assertEquals(Set.of("1\t60.00\t3\t20.00", "2\t0.03\t3\t0.01"), rows, variant);
      assertTrue(Files.readString(output.resolve("run-manifest.json")).contains("valid"));
    }
  }

  @Test
  void emptyPurchaseInputIsValidForBatchAggregation() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    FileSystem fs = FileSystem.getLocal(conf);
    java.nio.file.Path csv = dir.resolve("empty.csv");
    Files.writeString(csv, CsvEventParser.HEADER + "\nt,view,1,1,a,b,1,u,s\n");
    PreflightReport p =
        DatasetPreflight.inspect(List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_ID);
    Path manifest = new Path(dir.resolve("m.json").toUri()),
        report = new Path(dir.resolve("p.json").toUri()),
        hotPath = new Path(dir.resolve("h.json").toUri());
    JsonArtifacts.write(manifest, fs, p.manifest);
    JsonArtifacts.write(report, fs, p);
    GroupDictionary dictionary =
        GroupDictionary.from(
            DatasetProfiler.profile(
                List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_ID, 100));
    JsonArtifacts.write(hotPath, fs, dictionary);
    RevenueTool tool = new RevenueTool();
    tool.setConf(conf);
    java.nio.file.Path output = dir.resolve("out");
    assertEquals(
        0,
        tool.run(
            new String[] {
              "--variant",
              "v5",
              "--manifest",
              manifest.toString(),
              "--preflight",
              report.toString(),
              "--dictionary",
              hotPath.toString(),
              "--output",
              output.toString()
            }));
    assertTrue(Files.readString(output.resolve("run-manifest.json")).contains("V5_BATCH"));
  }
}
