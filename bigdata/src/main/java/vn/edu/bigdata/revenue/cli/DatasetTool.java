package vn.edu.bigdata.revenue.cli;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.BoundedLineReader;
import vn.edu.bigdata.revenue.input.CsvEventParser;
import vn.edu.bigdata.revenue.input.DatasetPreflight;
import vn.edu.bigdata.revenue.input.InputManifest;
import vn.edu.bigdata.revenue.input.JsonArtifacts;
import vn.edu.bigdata.revenue.input.ParseResult;
import vn.edu.bigdata.revenue.input.PreflightReport;
import vn.edu.bigdata.revenue.input.SamplingHash;
import vn.edu.bigdata.revenue.output.CsvExporter;
import vn.edu.bigdata.revenue.output.ResultValidator;
import vn.edu.bigdata.revenue.output.ValidationReport;
import vn.edu.bigdata.revenue.profile.DatasetProfiler;
import vn.edu.bigdata.revenue.profile.GroupDictionary;
import vn.edu.bigdata.revenue.profile.ProfileReport;

public final class DatasetTool extends Configured implements Tool {
  public int run(String[] args) {
    if (args.length == 0 || args[0].equals("--help")) {
      System.out.println(
          "DatasetTool preflight --input CSV_OR_DIR --manifest NEW_JSON [--report NEW_JSON --group-by category_id]\nDatasetTool profile --manifest JSON --output NEW_JSON --dictionary NEW_JSON [--group-by category_id --max-profile-keys 100000]\nDatasetTool sample --manifest JSON --output NEW_CSV --rate 0.1 [--seed 21 --group-by category_id]\nDatasetTool export --input RESULT_DIR --output NEW_CSV\nDatasetTool compare --left RESULT_DIR --right RESULT_DIR");
      return 0;
    }
    Configuration conf = getConf() == null ? new Configuration() : getConf();
    try {
      String command = args[0];
      Set<String> flags;
      switch (command) {
        case "preflight":
          flags = Set.of("input", "manifest", "report", "group-by");
          break;
        case "profile":
          flags = Set.of("manifest", "output", "dictionary", "group-by", "max-profile-keys");
          break;
        case "sample":
          flags = Set.of("manifest", "output", "rate", "seed", "group-by");
          break;
        case "export":
          flags = Set.of("input", "output");
          break;
        case "compare":
          flags = Set.of("left", "right");
          break;
        default:
          throw new IllegalArgumentException("Unknown command: " + command);
      }
      CliArguments a = new CliArguments(Arrays.copyOfRange(args, 1, args.length), flags);
      GroupMode mode = GroupMode.parse(a.get("group-by", "category_id"));
      if (command.equals("preflight")) {
        Path input = new Path(a.required("input"));
        FileSystem fs = input.getFileSystem(conf);
        PreflightReport report =
            DatasetPreflight.inspect(DatasetPreflight.listCsv(input, fs), fs, mode);
        if (!report.isValid())
          throw new IllegalArgumentException("Preflight failed: " + report.errors);
        Path manifest = new Path(a.required("manifest"));
        Path reportPath =
            new Path(
                a.get(
                    "report",
                    new Path(
                            manifest.getParent() == null ? new Path(".") : manifest.getParent(),
                            "preflight.json")
                        .toString()));
        ensureNew(manifest, conf);
        ensureNew(reportPath, conf);
        if (manifest.equals(reportPath))
          throw new IllegalArgumentException("Manifest/report paths must differ");
        JsonArtifacts.write(manifest, manifest.getFileSystem(conf), report.manifest);
        JsonArtifacts.write(reportPath, reportPath.getFileSystem(conf), report);
        System.out.println(
            "Preflight valid: "
                + report.validPurchaseCount
                + " purchases, fingerprint="
                + report.inputFingerprint);
        return 0;
      }
      if (command.equals("export")) {
        Path input = new Path(a.required("input")), output = new Path(a.required("output"));
        if (!input.getFileSystem(conf).getUri().equals(output.getFileSystem(conf).getUri()))
          throw new IllegalArgumentException("Export input/output must use same filesystem");
        CsvExporter.export(input, output, input.getFileSystem(conf));
        return 0;
      }
      if (command.equals("compare")) {
        Path left = new Path(a.required("left")), right = new Path(a.required("right"));
        if (!left.getFileSystem(conf).getUri().equals(right.getFileSystem(conf).getUri()))
          throw new IllegalArgumentException("Compare requires same filesystem");
        ValidationReport report = ResultValidator.compare(left, right, left.getFileSystem(conf));
        System.out.println(report.valid ? "Results equal" : report.errors.toString());
        return report.valid ? 0 : 1;
      }
      Path manifestPath = new Path(a.required("manifest"));
      InputManifest manifest =
          JsonArtifacts.read(manifestPath, manifestPath.getFileSystem(conf), InputManifest.class);
      List<Path> paths = new ArrayList<>();
      for (var entry : manifest.files) paths.add(new Path(entry.uri));
      if (paths.isEmpty()) throw new IllegalArgumentException("Empty manifest");
      FileSystem fs = paths.get(0).getFileSystem(conf);
      if (command.equals("profile")) {
        Path output = new Path(a.required("output")),
            dictionaryPath = new Path(a.required("dictionary"));
        ensureNew(output, conf);
        ensureNew(dictionaryPath, conf);
        ProfileReport report =
            DatasetProfiler.profile(paths, fs, mode, a.integer("max-profile-keys", 100000));
        if (!report.inputFingerprint.equals(manifest.fingerprint()))
          throw new IllegalArgumentException("Input changed since manifest");
        GroupDictionary dictionary = GroupDictionary.from(report);
        dictionary.validate(report.inputFingerprint, mode.name());
        JsonArtifacts.write(output, output.getFileSystem(conf), report);
        JsonArtifacts.write(dictionaryPath, dictionaryPath.getFileSystem(conf), dictionary);
        System.out.println("Profile: " + dictionary.groups.length + " dictionary groups");
        return 0;
      }
      Path output = new Path(a.required("output"));
      ensureNew(output, conf);
      if (!output.getName().endsWith(".csv"))
        throw new IllegalArgumentException("Sample output must end in .csv");
      double rate = Double.parseDouble(a.required("rate"));
      if (!Double.isFinite(rate) || rate <= 0 || rate > 1)
        throw new IllegalArgumentException("rate must be (0,1]");
      long seed = a.number("seed", 21);
      int threshold = (int) Math.floor(rate * 1000000);
      PreflightReport source = DatasetPreflight.inspect(paths, fs, mode);
      if (!source.isValid() || !source.inputFingerprint.equals(manifest.fingerprint()))
        throw new IllegalArgumentException("Source changed/invalid");
      FileSystem outFs = output.getFileSystem(conf);
      if (output.getParent() != null) outFs.mkdirs(output.getParent());
      Path temp = new Path(output.toString() + ".tmp-" + UUID.randomUUID());
      CsvEventParser parser = new CsvEventParser();
      try {
        try (Writer writer =
            new OutputStreamWriter(outFs.create(temp, false), StandardCharsets.UTF_8)) {
          writer.write(CsvEventParser.HEADER + "\n");
          for (InputManifest.Entry entry : manifest.files) {
            InputManifest.Entry actual =
                BoundedLineReader.scan(
                    new Path(entry.uri),
                    fs,
                    (offset, line) -> {
                      if (parser.parse(line).kind != ParseResult.Kind.HEADER
                          && SamplingHash.bucket(entry.uri, offset, seed, 1000000) < threshold)
                        writer.write(line + "\n");
                    });
            if (!actual.sha256.equals(entry.sha256))
              throw new IOException("Source changed during sampling");
          }
        }
        if (outFs.exists(output) || !outFs.rename(temp, output))
          throw new IOException("Cannot publish sample");
      } finally {
        if (outFs.exists(temp)) outFs.delete(temp, false);
      }
      PreflightReport sampled = DatasetPreflight.inspect(List.of(output), outFs, mode);
      JsonArtifacts.write(new Path(output.toString() + ".manifest.json"), outFs, sampled.manifest);
      JsonArtifacts.write(new Path(output.toString() + ".preflight.json"), outFs, sampled);
      Map<String, Object> provenance = new TreeMap<>();
      provenance.put("sourceFingerprint", manifest.fingerprint());
      provenance.put("rate", rate);
      provenance.put("seed", seed);
      provenance.put("sampleFingerprint", sampled.inputFingerprint);
      JsonArtifacts.write(new Path(output.toString() + ".sample.json"), outFs, provenance);
      System.out.println("Sample: " + output);
      return 0;
    } catch (IllegalArgumentException | IOException e) {
      System.err.println("Dataset error: " + e.getMessage());
      return 2;
    }
  }

  private static void ensureNew(Path path, Configuration conf) throws IOException {
    if (path.getFileSystem(conf).exists(path))
      throw new IllegalArgumentException("Output already exists: " + path);
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new DatasetTool(), args));
  }
}
