package vn.edu.bigdata.revenue.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import vn.edu.bigdata.revenue.domain.AggregateState;

public final class SumCountWritable implements Writable {
  private long sum, count;

  public SumCountWritable() {}

  public SumCountWritable(long sum, long count) {
    set(sum, count);
  }

  public void set(long sum, long count) {
    new AggregateState(sum, count);
    this.sum = sum;
    this.count = count;
  }

  public AggregateState toState() {
    return new AggregateState(sum, count);
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(sum);
    out.writeLong(count);
  }

  public void readFields(DataInput in) throws IOException {
    try {
      set(in.readLong(), in.readLong());
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid serialized aggregate", e);
    }
  }
}
