package vn.edu.bigdata.revenue.hadoop.io;

import java.io.*;
import org.apache.hadoop.io.Writable;

/** Compact entries: group ID (int), sum (long), count (long). */
public final class AggregateBatchWritable implements Writable {
  private int[] ids = new int[0];
  private long[] sums = new long[0], counts = new long[0];

  public AggregateBatchWritable() {}

  public AggregateBatchWritable(int[] ids, long[] sums, long[] counts) {
    if (ids.length != sums.length || ids.length != counts.length || ids.length > 100000)
      throw new IllegalArgumentException("Invalid batch dimensions");
    this.ids = ids.clone();
    this.sums = sums.clone();
    this.counts = counts.clone();
    for (int i = 0; i < ids.length; i++) check(ids[i], sums[i], counts[i]);
  }

  private static void check(int id, long sum, long count) {
    if (id < 0 || sum < 0 || count <= 0) throw new IllegalArgumentException("Invalid batch entry");
  }

  public int size() {
    return ids.length;
  }

  public int id(int i) {
    return ids[i];
  }

  public long sum(int i) {
    return sums[i];
  }

  public long count(int i) {
    return counts[i];
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(ids.length);
    for (int i = 0; i < ids.length; i++) {
      out.writeInt(ids[i]);
      out.writeLong(sums[i]);
      out.writeLong(counts[i]);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int n = in.readInt();
    if (n < 0 || n > 100000) throw new IOException("Batch exceeds guard");
    ids = new int[n];
    sums = new long[n];
    counts = new long[n];
    for (int i = 0; i < n; i++) {
      ids[i] = in.readInt();
      sums[i] = in.readLong();
      counts[i] = in.readLong();
      try {
        check(ids[i], sums[i], counts[i]);
      } catch (IllegalArgumentException e) {
        throw new IOException("Corrupt batch", e);
      }
    }
  }
}
