package vn.edu.bigdata.revenue.input;

import java.util.ArrayList;
import java.util.List;

public final class PreflightReport {
  public int schemaVersion = 1;
  public InputManifest manifest = new InputManifest();
  public String inputFingerprint, groupMode;
  public long validPurchaseCount, lines, elapsedMillis;
  public List<String> errors = new ArrayList<>();

  @com.fasterxml.jackson.annotation.JsonIgnore
  public boolean isValid() {
    return errors.isEmpty();
  }
}
