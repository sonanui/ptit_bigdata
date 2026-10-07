package vn.edu.bigdata.revenue.input;

import vn.edu.bigdata.revenue.domain.AggregateState;

public final class PreparationResult {
  public enum Reason {
    VALID_PURCHASE,
    NON_PURCHASE,
    UNKNOWN_EVENT,
    INVALID_PRICE,
    INVALID_GROUP,
    HEADER,
    MALFORMED_CSV
  }

  private final Reason reason;
  private final String group;
  private final AggregateState state;
  private final boolean missingCategoryCode;

  private PreparationResult(Reason reason, String group, AggregateState state, boolean missing) {
    this.reason = reason;
    this.group = group;
    this.state = state;
    this.missingCategoryCode = missing;
  }

  public static PreparationResult skipped(Reason reason) {
    return new PreparationResult(reason, null, null, false);
  }

  public static PreparationResult accepted(String group, long price, boolean missing) {
    return new PreparationResult(
        Reason.VALID_PURCHASE, group, AggregateState.single(price), missing);
  }

  public Reason reason() {
    return reason;
  }

  public String group() {
    return group;
  }

  public AggregateState state() {
    return state;
  }

  public boolean missingCategoryCode() {
    return missingCategoryCode;
  }

  public boolean valid() {
    return reason == Reason.VALID_PURCHASE;
  }
}
