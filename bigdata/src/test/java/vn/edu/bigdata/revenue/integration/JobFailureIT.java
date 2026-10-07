package vn.edu.bigdata.revenue.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.List;
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
import vn.edu.bigdata.revenue.job.JobExecutor;
import vn.edu.bigdata.revenue.job.JobPlan;
import vn.edu.bigdata.revenue.job.JobPlanFactory;
import vn.edu.bigdata.revenue.job.RunOptions;
import vn.edu.bigdata.revenue.job.Variant;

class JobFailureIT {
  @TempDir java.nio.file.Path dir;

  private Configuration conf() {
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.setInt("mapreduce.task.io.sort.mb", 8);
    conf.setInt("mapreduce.client.completion.pollinterval", 100);
    return conf;
  }

  @Test
  void refusesChangedInputAndExistingOutputWithoutTouchingIt() throws Exception {
    Configuration conf = conf();
    FileSystem fs = FileSystem.getLocal(conf);
    java.nio.file.Path csv = dir.resolve("events.csv");
    Files.writeString(csv, CsvEventParser.HEADER + "\nt,purchase,1,1,a,b,1,u,s\n");
    PreflightReport p =
        DatasetPreflight.inspect(List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_ID);
    Path manifest = new Path(dir.resolve("m.json").toUri()),
        report = new Path(dir.resolve("p.json").toUri());
    JsonArtifacts.write(manifest, fs, p.manifest);
    JsonArtifacts.write(report, fs, p);
    RevenueTool tool = new RevenueTool();
    tool.setConf(conf);
    java.nio.file.Path output = dir.resolve("out");
    Files.createDirectory(output);
    Files.writeString(output.resolve("keep.txt"), "keep");
    String[] args = {
      "--variant",
      "v1",
      "--manifest",
      manifest.toString(),
      "--preflight",
      report.toString(),
      "--output",
      output.toString()
    };
    assertEquals(2, tool.run(args));
    assertEquals("keep", Files.readString(output.resolve("keep.txt")));
    args[7] = dir.resolve("new-out").toString();
    Files.writeString(csv, CsvEventParser.HEADER + "\nt,purchase,1,1,a,b,2,u,s\n");
    assertEquals(2, tool.run(args));
    assertFalse(Files.exists(dir.resolve("new-out")));
  }

  @Test
  void malformedRuntimeInputFailsTheSingleAggregationJob() throws Exception {
    Configuration conf = conf();
    FileSystem fs = FileSystem.getLocal(conf);
    java.nio.file.Path csv = dir.resolve("events.csv");
    Files.writeString(csv, CsvEventParser.HEADER + "\nt,purchase,1,1,a,b,1,u,s\n");
    PreflightReport report =
        DatasetPreflight.inspect(List.of(new Path(csv.toUri())), fs, GroupMode.CATEGORY_ID);
    RunOptions o = new RunOptions();
    o.variant = Variant.V1_DIRECT;
    o.manifest = report.manifest;
    o.preflight = report;
    o.output = new Path(dir.resolve("out").toUri());
    JobPlan plan = JobPlanFactory.build(conf, o);
    // Mutation after planning reproduces a runtime structural failure, bypassing the CLI's
    // preflight.
    Files.writeString(csv, CsvEventParser.HEADER + "\nbroken\n");
    var run = JobExecutor.execute(plan);
    assertEquals("failed", run.validationStatus);
    assertEquals(1, run.stages.size());
    assertFalse(run.stages.get(0).success);
  }
}
