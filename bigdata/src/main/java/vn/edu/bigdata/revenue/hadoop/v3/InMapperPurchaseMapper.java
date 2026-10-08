package vn.edu.bigdata.revenue.hadoop.v3;

import java.io.IOException;
import vn.edu.bigdata.revenue.domain.AggregateState;
import vn.edu.bigdata.revenue.hadoop.shared.BoundedAccumulator;
import vn.edu.bigdata.revenue.hadoop.shared.RevenueCounters;
import vn.edu.bigdata.revenue.hadoop.v1.DirectPurchaseMapper;
import vn.edu.bigdata.revenue.job.RunOptions;

public final class InMapperPurchaseMapper extends DirectPurchaseMapper {
  private BoundedAccumulator<String> cache;

  protected void setup(Context context) {
    super.setup(context);
    cache = new BoundedAccumulator<>(context.getConfiguration().getInt(RunOptions.MAX_KEYS, 10000));
  }

  protected void emit(String group, AggregateState value, Context context)
      throws IOException, InterruptedException {
    cache.add(group, value, (k, v) -> super.emit(k, v, context));
  }

  protected void cleanup(Context context) throws IOException, InterruptedException {
    cache.flush((k, v) -> super.emit(k, v, context));
    context.getCounter(RevenueCounters.CACHE_FLUSHES).increment(cache.flushes());
    context.getCounter(RevenueCounters.CACHE_PEAK_KEYS).increment(cache.peak());
  }
}
