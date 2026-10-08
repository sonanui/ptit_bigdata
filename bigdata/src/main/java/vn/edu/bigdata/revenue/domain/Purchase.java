package vn.edu.bigdata.revenue.domain;

public final class Purchase {
  public final String categoryId;
  public final String categoryCode;
  public final long priceMinor;

  public Purchase(String categoryId, String categoryCode, long priceMinor) {
    if (priceMinor < 0) throw new IllegalArgumentException("Negative price");
    this.categoryId = categoryId == null ? "" : categoryId.trim();
    this.categoryCode = categoryCode == null ? "" : categoryCode.trim();
    this.priceMinor = priceMinor;
  }
}
