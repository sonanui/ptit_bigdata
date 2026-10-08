package vn.edu.bigdata.revenue.job;

import java.util.Locale;

public enum Variant {
  V1_DIRECT,
  V2_COMBINER,
  V3_IN_MAPPER,
  V4_DENSE,
  V5_BATCH;

  public static Variant parse(String name) {
    switch (name.toLowerCase(Locale.ROOT)) {
      case "v1":
        return V1_DIRECT;
      case "v2":
        return V2_COMBINER;
      case "v3":
        return V3_IN_MAPPER;
      case "v4":
        return V4_DENSE;
      case "v5":
        return V5_BATCH;
      default:
        return valueOf(name.toUpperCase(Locale.ROOT));
    }
  }
}
