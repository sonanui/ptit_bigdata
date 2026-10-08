package vn.edu.bigdata.revenue.hadoop.v5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import vn.edu.bigdata.revenue.hadoop.io.AggregateBatchWritable;
import vn.edu.bigdata.revenue.hadoop.shared.DenseAggregationMapper;
import vn.edu.bigdata.revenue.input.EventParser;
import vn.edu.bigdata.revenue.input.ProjectedCsvEventParser;

public final class BatchPurchaseMapper
    extends DenseAggregationMapper<IntWritable, AggregateBatchWritable> {
  protected EventParser parser() {
    return new ProjectedCsvEventParser();
  }

  protected void cleanup(Context context) throws IOException, InterruptedException {
    int r = context.getNumReduceTasks();
    int[] sizes = new int[r];
    for (int id = 0; id < counts.length; id++) if (counts[id] > 0) sizes[id % r]++;
    int[][] ids = new int[r][];
    long[][] totals = new long[r][], numbers = new long[r][];
    for (int p = 0; p < r; p++) {
      ids[p] = new int[sizes[p]];
      totals[p] = new long[sizes[p]];
      numbers[p] = new long[sizes[p]];
      sizes[p] = 0;
    }
    for (int id = 0; id < counts.length; id++)
      if (counts[id] > 0) {
        int p = id % r, i = sizes[p]++;
        ids[p][i] = id;
        totals[p][i] = sums[id];
        numbers[p][i] = counts[id];
      }
    for (int p = 0; p < r; p++)
      if (sizes[p] > 0)
        context.write(
            new IntWritable(p), new AggregateBatchWritable(ids[p], totals[p], numbers[p]));
  }
}
