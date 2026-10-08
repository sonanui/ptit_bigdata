package vn.edu.bigdata.revenue.domain;

import java.util.Locale;

public enum GroupMode {
  CATEGORY_ID,
  CATEGORY_CODE,
  CATEGORY_ROOT;

  public static GroupMode parse(String name) {
    return valueOf(name.toUpperCase(Locale.ROOT));
  }
}
