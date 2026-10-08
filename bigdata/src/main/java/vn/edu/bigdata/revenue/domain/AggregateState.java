package vn.edu.bigdata.revenue.domain;

public final class AggregateState {
  private final long sumMinor;
  private final long purchaseCount;

  public AggregateState(long sumMinor, long purchaseCount) {
    if (sumMinor < 0 || purchaseCount < 0 || (purchaseCount == 0 && sumMinor != 0))
      throw new IllegalArgumentException("Invalid aggregate state");
    this.sumMinor = sumMinor;
    this.purchaseCount = purchaseCount;
  }

  public static AggregateState empty() {
    return new AggregateState(0, 0);
  }

  public static AggregateState single(long priceMinor) {
    return new AggregateState(priceMinor, 1);
  }

  public AggregateState merge(AggregateState other) {
    return new AggregateState(
        Math.addExact(sumMinor, other.sumMinor), Math.addExact(purchaseCount, other.purchaseCount));
  }

  public long sumMinor() {
    return sumMinor;
  }

  public long purchaseCount() {
    return purchaseCount;
  }
}
