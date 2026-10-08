package vn.edu.bigdata.revenue.hadoop.v1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vn.edu.bigdata.revenue.domain.AggregateState;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.hadoop.io.SumCountWritable;
import vn.edu.bigdata.revenue.hadoop.shared.MapperSupport;
import vn.edu.bigdata.revenue.input.PreparationResult;
import vn.edu.bigdata.revenue.job.RunOptions;

public class DirectPurchaseMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {
  private final MapperSupport support = new MapperSupport();
  private GroupMode mode;

  protected void setup(Context context) {
    mode = GroupMode.parse(context.getConfiguration().get(RunOptions.GROUP_MODE, "CATEGORY_ID"));
  }

  protected void map(LongWritable offset, Text line, Context context)
      throws IOException, InterruptedException {
    PreparationResult result = support.prepare(line, context, mode);
    if (result.valid()) emit(result.group(), result.state(), context);
  }

  protected void emit(String group, AggregateState state, Context context)
      throws IOException, InterruptedException {
    context.write(new Text(group), new SumCountWritable(state.sumMinor(), state.purchaseCount()));
  }
}
