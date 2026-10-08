package vn.edu.bigdata.revenue.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.DatasetPreflight;
import vn.edu.bigdata.revenue.input.InputManifest;
import vn.edu.bigdata.revenue.input.JsonArtifacts;
import vn.edu.bigdata.revenue.input.PreflightReport;
import vn.edu.bigdata.revenue.job.JobExecutor;
import vn.edu.bigdata.revenue.job.JobPlan;
import vn.edu.bigdata.revenue.job.JobPlanFactory;
import vn.edu.bigdata.revenue.job.RunOptions;
import vn.edu.bigdata.revenue.job.Variant;
import vn.edu.bigdata.revenue.metrics.RunManifest;
import vn.edu.bigdata.revenue.output.ResultValidator;
import vn.edu.bigdata.revenue.output.ValidationReport;
import vn.edu.bigdata.revenue.profile.GroupDictionary;

public final class RevenueTool extends Configured implements Tool {
  public int run(String[] args) {
    if (args.length == 0 || Arrays.asList(args).contains("--help")) {
      System.out.println(
          "RevenueTool --variant v1..v5 --manifest input.json --preflight preflight.json --output NEW_PATH [--group-by category_id|category_code|category_root --reducers 2 --max-keys 10000 --dictionary groups.json --compress true|false]");
      return 0;
    }
    Configuration conf = getConf() == null ? new Configuration() : getConf();
    RunOptions options = new RunOptions();
    long start = System.nanoTime();
    String started = java.time.Instant.now().toString();
    try {
      CliArguments a =
          new CliArguments(
              args,
              Set.of(
                  "variant",
                  "manifest",
                  "preflight",
                  "output",
                  "group-by",
                  "reducers",
                  "max-keys",
                  "dictionary",
                  "compress"));
      options.variant = Variant.parse(a.required("variant"));
      options.groupMode = GroupMode.parse(a.get("group-by", "category_id"));
      Path manifestPath = new Path(a.required("manifest")),
          reportPath = new Path(a.required("preflight"));
      options.manifest =
          JsonArtifacts.read(manifestPath, manifestPath.getFileSystem(conf), InputManifest.class);
      options.preflight =
          JsonArtifacts.read(reportPath, reportPath.getFileSystem(conf), PreflightReport.class);
      options.output = new Path(a.required("output"));
      options.reducers = a.integer("reducers", 2);
      options.maxKeys = a.integer("max-keys", 10000);
      String compression = a.get("compress", "false");
      if (!Set.of("true", "false").contains(compression))
        throw new IllegalArgumentException("compress must be true/false");
      options.compress = Boolean.parseBoolean(compression);
      if (options.variant == Variant.V4_DENSE || options.variant == Variant.V5_BATCH) {
        options.dictionaryPath = new Path(a.required("dictionary"));
        options.dictionary =
            JsonArtifacts.read(
                options.dictionaryPath,
                options.dictionaryPath.getFileSystem(conf),
                GroupDictionary.class);
      }
      options.validate(conf);
      List<Path> inputs = new ArrayList<>();
      for (var entry : options.manifest.files) inputs.add(new Path(entry.uri));
      FileSystem inputFs = inputs.get(0).getFileSystem(conf);
      PreflightReport fresh = DatasetPreflight.inspect(inputs, inputFs, options.groupMode);
      if (!fresh.isValid()
          || !fresh.inputFingerprint.equals(options.manifest.fingerprint())
          || fresh.validPurchaseCount != options.preflight.validPurchaseCount)
        throw new IllegalArgumentException("Input changed or preflight invalid");
      JobPlan plan = JobPlanFactory.build(conf, options);
      RunManifest run = JobExecutor.execute(plan);
      run.inputFingerprint = fresh.inputFingerprint;
      run.groupMode = options.groupMode.name();
      run.policyHash = InputManifest.policyHash();
      run.startedAt = started;
      run.preprocessingMillis = fresh.elapsedMillis;
      run.javaVersion = System.getProperty("java.version");
      run.hadoopVersion = VersionInfo.getVersion();
      run.framework = conf.get("mapreduce.framework.name", "local");
      run.reducers = options.reducers;
      run.maxKeys = options.maxKeys;
      run.compressed = options.compress;
      for (String key :
          List.of(
              "fs.defaultFS",
              "mapreduce.framework.name",
              "mapreduce.input.fileinputformat.split.maxsize",
              "mapreduce.map.memory.mb",
              "mapreduce.reduce.memory.mb",
              "mapreduce.task.io.sort.mb",
              "mapreduce.map.speculative",
              "mapreduce.reduce.speculative")) run.configuration.put(key, conf.get(key, "default"));
      FileSystem outputFs = options.output.getFileSystem(conf);
      if (!run.validationStatus.equals("failed")) {
        long actual =
            run.stages
                .get(0)
                .counters
                .getOrDefault(
                    "vn.edu.bigdata.revenue.hadoop.shared.RevenueCounters.VALID_PURCHASE", 0L);
        ValidationReport validation =
            ResultValidator.validate(options.output, outputFs, fresh.validPurchaseCount);
        if (actual != fresh.validPurchaseCount) validation.fail("Stage1 purchase counter mismatch");
        ValidationReport sourceValidation =
            ResultValidator.validateInput(options.manifest, inputFs);
        if (!sourceValidation.valid) validation.fail(sourceValidation.errors.toString());
        run.validationStatus = validation.valid ? "valid" : "failed";
        if (!validation.valid) run.error = validation.errors.toString();
      }
      run.endToEndMillis = (System.nanoTime() - start) / 1000000;
      JsonArtifacts.write(new Path(options.output, "run-manifest.json"), outputFs, run);
      System.out.println(
          "Run "
              + run.validationStatus
              + ": "
              + options.output
              + " ("
              + run.effectiveVariant
              + ")");
      return run.validationStatus.equals("valid") ? 0 : 1;
    } catch (IllegalArgumentException | java.io.IOException e) {
      System.err.println("Input/config error: " + e.getMessage());
      return 2;
    } catch (Exception e) {
      System.err.println("Run failed: " + e);
      return 1;
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new RevenueTool(), args));
  }
}
