package vn.edu.bigdata.revenue.hadoop.v4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import vn.edu.bigdata.revenue.hadoop.io.SumCountWritable;
import vn.edu.bigdata.revenue.hadoop.shared.DenseAggregationMapper;

public final class DensePurchaseMapper extends DenseAggregationMapper<Text, SumCountWritable> {
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Text key = new Text();
    SumCountWritable value = new SumCountWritable();
    for (int id = 0; id < counts.length; id++)
      if (counts[id] > 0) {
        key.set(dictionary.groups[id]);
        value.set(sums[id], counts[id]);
        context.write(key, value);
      }
  }
}
