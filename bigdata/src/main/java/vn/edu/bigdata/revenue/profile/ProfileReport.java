package vn.edu.bigdata.revenue.profile;

import java.util.Map;
import java.util.TreeMap;
import vn.edu.bigdata.revenue.input.PreflightReport;

public final class ProfileReport {
  public int schemaVersion = 1;
  public String groupMode, inputFingerprint, policyHash;
  public long validPurchaseCount, elapsedMillis;
  public Map<String, Long> countByGroup = new TreeMap<>();
  public Map<String, Long> counters = new TreeMap<>();
  public PreflightReport preflight;
}
