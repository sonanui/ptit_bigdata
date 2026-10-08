package vn.edu.bigdata.revenue.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class RunManifest {
  public int schemaVersion = 2;
  public String requestedVariant,
      effectiveVariant,
      inputFingerprint,
      groupMode,
      policyHash,
      validationStatus = "pending",
      error,
      startedAt,
      javaVersion,
      hadoopVersion,
      framework;
  public int reducers, maxKeys;
  public long preprocessingMillis, endToEndMillis;
  public boolean compressed;
  public Map<String, String> configuration = new TreeMap<>();
  public List<Stage> stages = new ArrayList<>();

  public static final class Stage {
    public String jobId, name, error;
    public boolean success;
    public long elapsedMillis;
    public Map<String, Long> counters = new TreeMap<>();
  }
}
