package vn.edu.bigdata.revenue.hadoop.v2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import vn.edu.bigdata.revenue.domain.AggregateState;
import vn.edu.bigdata.revenue.hadoop.io.SumCountWritable;

public final class SumCountCombiner
    extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {
  protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
      throws IOException, InterruptedException {
    AggregateState total = AggregateState.empty();
    for (SumCountWritable value : values) total = total.merge(value.toState());
    context.write(key, new SumCountWritable(total.sumMinor(), total.purchaseCount()));
  }
}
