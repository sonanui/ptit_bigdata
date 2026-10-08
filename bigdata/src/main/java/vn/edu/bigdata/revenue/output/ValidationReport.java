package vn.edu.bigdata.revenue.output;

import java.util.ArrayList;
import java.util.List;

public final class ValidationReport {
  public boolean valid = true;
  public long groupCount, totalCount, totalSumMinor;
  public List<String> errors = new ArrayList<>();

  public void fail(String error) {
    valid = false;
    if (errors.size() < 20) errors.add(error);
  }
}
