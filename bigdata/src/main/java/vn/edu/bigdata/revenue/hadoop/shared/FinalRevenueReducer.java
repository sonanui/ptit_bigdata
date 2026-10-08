package vn.edu.bigdata.revenue.hadoop.shared;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import vn.edu.bigdata.revenue.domain.AggregateState;
import vn.edu.bigdata.revenue.hadoop.io.SumCountWritable;
import vn.edu.bigdata.revenue.output.RevenueFormatter;

public final class FinalRevenueReducer extends Reducer<Text, SumCountWritable, Text, Text> {
  protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
      throws IOException, InterruptedException {
    AggregateState total = AggregateState.empty();
    for (SumCountWritable value : values) total = total.merge(value.toState());
    context.write(key, new Text(RevenueFormatter.value(total)));
  }
}
