package vn.edu.bigdata.revenue.domain;

public final class GroupKeyResolver {
  private GroupKeyResolver() {}

  public static String resolve(Purchase p, GroupMode mode) {
    if (mode == GroupMode.CATEGORY_ID) {
      if (!p.categoryId.matches("[0-9]+"))
        throw new IllegalArgumentException("Invalid category ID");
      return p.categoryId;
    }
    if (p.categoryCode.isEmpty()) return "__UNKNOWN__";
    if (p.categoryCode.indexOf('\t') >= 0
        || p.categoryCode.indexOf('\n') >= 0
        || p.categoryCode.indexOf('\r') >= 0)
      throw new IllegalArgumentException("Unsafe group key");
    return mode == GroupMode.CATEGORY_ROOT ? p.categoryCode.split("\\.", 2)[0] : p.categoryCode;
  }
}
