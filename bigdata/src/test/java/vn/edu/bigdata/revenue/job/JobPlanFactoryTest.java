package vn.edu.bigdata.revenue.job;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.*;
import vn.edu.bigdata.revenue.profile.*;

class JobPlanFactoryTest {
  @TempDir java.nio.file.Path dir;

  @Test
  void allOptimizationsUseOneJobAndRequireMatchingDictionary() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    java.nio.file.Path file = dir.resolve("input.csv");
    Files.writeString(file, CsvEventParser.HEADER + "\nt,purchase,1,1,a,b,1,u,s\n");
    var report =
        DatasetPreflight.inspect(List.of(new Path(file.toUri())), fs, GroupMode.CATEGORY_ID);
    var dictionary =
        GroupDictionary.from(
            DatasetProfiler.profile(
                List.of(new Path(file.toUri())), fs, GroupMode.CATEGORY_ID, 100));
    Path artifact = new Path(dir.resolve("dictionary.json").toUri());
    JsonArtifacts.write(artifact, fs, dictionary);
    for (Variant variant : Variant.values()) {
      RunOptions o = new RunOptions();
      o.variant = variant;
      o.manifest = report.manifest;
      o.preflight = report;
      o.output = new Path(dir.resolve(variant.name()).toUri());
      o.dictionary = dictionary;
      o.dictionaryPath = artifact;
      assertEquals(1, JobPlanFactory.build(conf, o).stages.size(), variant.name());
    }
    RunOptions o = new RunOptions();
    o.variant = Variant.V4_DENSE;
    o.manifest = report.manifest;
    o.preflight = report;
    o.output = new Path(dir.resolve("missing").toUri());
    assertThrows(IllegalArgumentException.class, () -> JobPlanFactory.build(conf, o));
  }
}
