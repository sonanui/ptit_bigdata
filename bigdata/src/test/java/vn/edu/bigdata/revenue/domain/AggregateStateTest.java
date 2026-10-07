package vn.edu.bigdata.revenue.domain;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AggregateStateTest {
  @Test
  void sumsCountsInsteadOfAveragingAverages() {
    AggregateState result =
        AggregateState.single(1000)
            .merge(AggregateState.single(2000).merge(AggregateState.single(3000)));
    assertEquals(6000, result.sumMinor());
    assertEquals(3, result.purchaseCount());
    assertEquals("20.00", Money.average(result.sumMinor(), result.purchaseCount()));
    assertEquals(3, result.merge(AggregateState.empty()).purchaseCount());
  }

  @Test
  void moneyIsExactAndZeroContributesToCount() {
    assertEquals(1230, Money.parseMinor("12.300"));
    assertEquals("0.01", Money.average(1, 2));
    assertEquals(1, AggregateState.single(0).purchaseCount());
    for (String value : new String[] {"12.301", "NaN", "Infinity", "-1", ""}) {
      assertThrows(IllegalArgumentException.class, () -> Money.parseMinor(value));
    }
    assertThrows(IllegalArgumentException.class, () -> Money.average(0, 0));
  }

  @Test
  void refusesOverflow() {
    assertThrows(
        ArithmeticException.class,
        () -> new AggregateState(Long.MAX_VALUE, 1).merge(AggregateState.single(1)));
    assertThrows(
        ArithmeticException.class,
        () -> new AggregateState(0, Long.MAX_VALUE).merge(AggregateState.single(0)));
  }

  @Test
  void resolvesGroupsWithoutInventingBranches() {
    Purchase p = new Purchase("2053013552222222222", "electronics.smartphone", 1);
    assertEquals("2053013552222222222", GroupKeyResolver.resolve(p, GroupMode.CATEGORY_ID));
    assertEquals("electronics", GroupKeyResolver.resolve(p, GroupMode.CATEGORY_ROOT));
    assertEquals(
        "__UNKNOWN__", GroupKeyResolver.resolve(new Purchase("1", "", 1), GroupMode.CATEGORY_CODE));
    assertThrows(
        IllegalArgumentException.class,
        () -> GroupKeyResolver.resolve(new Purchase("", "", 1), GroupMode.CATEGORY_ID));
  }
}
