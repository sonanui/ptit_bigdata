package vn.edu.bigdata.revenue.output;

import vn.edu.bigdata.revenue.domain.Money;

public final class OutputRow {
  public final String groupKey, averageText;
  public final long sumMinor, purchaseCount;

  private OutputRow(String group, long sum, long count, String average) {
    groupKey = group;
    sumMinor = sum;
    purchaseCount = count;
    averageText = average;
  }

  public static OutputRow parseTsv(String line) {
    String[] columns = line.split("\t", -1);
    if (columns.length != 4 || columns[0].isEmpty())
      throw new IllegalArgumentException("Invalid output columns");
    long sum = Money.parseMinor(columns[1]), count = Long.parseLong(columns[2]);
    if (count <= 0
        || !columns[1].equals(Money.formatMinor(sum))
        || !columns[3].equals(Money.average(sum, count)))
      throw new IllegalArgumentException("Invalid output amount/count/average");
    return new OutputRow(columns[0], sum, count, columns[3]);
  }
}
