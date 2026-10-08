package vn.edu.bigdata.revenue.job;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.InputManifest;
import vn.edu.bigdata.revenue.input.PreflightReport;
import vn.edu.bigdata.revenue.profile.GroupDictionary;

public final class RunOptions {
  public static final String GROUP_MODE = "revenue.group.mode",
      MAX_KEYS = "revenue.cache.max.keys",
      FINGERPRINT = "revenue.input.fingerprint";
  public Variant variant = Variant.V1_DIRECT;
  public GroupMode groupMode = GroupMode.CATEGORY_ID;
  public InputManifest manifest;
  public PreflightReport preflight;
  public Path output, dictionaryPath;
  public GroupDictionary dictionary;
  public int reducers = 2, maxKeys = 10000;
  public boolean compress = false;

  public void validate(Configuration conf) throws IOException {
    if (manifest == null
        || preflight == null
        || !preflight.isValid()
        || preflight.schemaVersion != 1
        || !manifest.fingerprint().equals(preflight.inputFingerprint)
        || !manifest.fingerprint().equals(preflight.manifest.fingerprint())
        || !groupMode.name().equals(preflight.groupMode))
      throw new IllegalArgumentException("Preflight/manifest/policy mismatch");
    if (reducers < 1 || maxKeys < 1)
      throw new IllegalArgumentException("Invalid reducer/cache settings");
    if (output == null) throw new IllegalArgumentException("Output required");
    if (output.getFileSystem(conf).exists(output))
      throw new IllegalArgumentException("Output already exists");
    for (InputManifest.Entry e : manifest.files) {
      Path input = new Path(e.uri);
      if (overlaps(output, input, conf)) throw new IllegalArgumentException("Input/output overlap");
    }
    if (variant == Variant.V4_DENSE || variant == Variant.V5_BATCH) {
      if (dictionary == null || dictionaryPath == null)
        throw new IllegalArgumentException("V4/V5 require --dictionary");
      dictionary.validate(manifest.fingerprint(), groupMode.name());
      if (overlaps(output, dictionaryPath, conf))
        throw new IllegalArgumentException("Dictionary/output overlap");
    }
  }

  private static boolean overlaps(Path a, Path b, Configuration conf) throws IOException {
    URI x = a.getFileSystem(conf).makeQualified(a).toUri().normalize(),
        y = b.getFileSystem(conf).makeQualified(b).toUri().normalize();
    if (!java.util.Objects.equals(x.getScheme(), y.getScheme())
        || !java.util.Objects.equals(x.getAuthority(), y.getAuthority())) return false;
    String xp = x.getPath().replaceAll("/+$", ""), yp = y.getPath().replaceAll("/+$", "");
    return xp.equals(yp) || xp.startsWith(yp + "/") || yp.startsWith(xp + "/");
  }
}
