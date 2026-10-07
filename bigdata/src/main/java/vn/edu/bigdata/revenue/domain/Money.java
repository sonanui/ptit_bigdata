package vn.edu.bigdata.revenue.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class Money {
  private Money() {}

  public static long parseMinor(String text) {
    try {
      BigDecimal value = new BigDecimal(text.trim()).setScale(2, RoundingMode.UNNECESSARY);
      if (value.signum() < 0) throw new IllegalArgumentException("Negative price");
      return value.movePointRight(2).longValueExact();
    } catch (ArithmeticException | NumberFormatException e) {
      throw new IllegalArgumentException("Invalid price", e);
    }
  }

  public static String formatMinor(long value) {
    return BigDecimal.valueOf(value, 2).toPlainString();
  }

  public static String average(long sum, long count) {
    if (count <= 0 || sum < 0) throw new IllegalArgumentException("Invalid average operands");
    return BigDecimal.valueOf(sum, 2)
        .divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP)
        .toPlainString();
  }
}
