package vn.edu.bigdata.revenue.output;

import vn.edu.bigdata.revenue.domain.AggregateState;
import vn.edu.bigdata.revenue.domain.Money;

public final class RevenueFormatter {
  private RevenueFormatter() {}

  public static String value(AggregateState state) {
    return Money.formatMinor(state.sumMinor())
        + "\t"
        + state.purchaseCount()
        + "\t"
        + Money.average(state.sumMinor(), state.purchaseCount());
  }
}
