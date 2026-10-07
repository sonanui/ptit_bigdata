package vn.edu.bigdata.revenue.hadoop.v5;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import vn.edu.bigdata.revenue.domain.AggregateState;
import vn.edu.bigdata.revenue.hadoop.io.AggregateBatchWritable;
import vn.edu.bigdata.revenue.input.JsonArtifacts;
import vn.edu.bigdata.revenue.job.RunOptions;
import vn.edu.bigdata.revenue.output.RevenueFormatter;
import vn.edu.bigdata.revenue.profile.GroupDictionary;

public final class BatchRevenueReducer
    extends Reducer<IntWritable, AggregateBatchWritable, Text, Text> {
  private GroupDictionary dictionary;
  private long[] sums, counts;
  private int partition, reducers;

  protected void setup(Context context) throws IOException {
    dictionary =
        JsonArtifacts.read(
            new Path("group-dictionary.json"),
            FileSystem.getLocal(context.getConfiguration()),
            GroupDictionary.class);
    dictionary.validate(
        context.getConfiguration().get(RunOptions.FINGERPRINT),
        context.getConfiguration().get(RunOptions.GROUP_MODE));
    sums = new long[dictionary.groups.length];
    counts = new long[dictionary.groups.length];
    partition = context.getTaskAttemptID().getTaskID().getId();
    reducers = context.getNumReduceTasks();
  }

  protected void reduce(IntWritable key, Iterable<AggregateBatchWritable> batches, Context context)
      throws IOException {
    if (key.get() != partition) throw new IOException("Batch routed to wrong reducer");
    for (AggregateBatchWritable batch : batches)
      for (int i = 0; i < batch.size(); i++) {
        int id = batch.id(i);
        if (id >= counts.length || id % reducers != partition)
          throw new IOException("Invalid batch ownership");
        sums[id] = Math.addExact(sums[id], batch.sum(i));
        counts[id] = Math.addExact(counts[id], batch.count(i));
      }
  }

  protected void cleanup(Context context) throws IOException, InterruptedException {
    for (int id = partition; id < counts.length; id += reducers)
      if (counts[id] > 0)
        context.write(
            new Text(dictionary.groups[id]),
            new Text(RevenueFormatter.value(new AggregateState(sums[id], counts[id]))));
  }
}
