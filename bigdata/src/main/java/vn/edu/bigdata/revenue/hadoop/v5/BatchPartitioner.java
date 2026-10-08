package vn.edu.bigdata.revenue.hadoop.v5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import vn.edu.bigdata.revenue.hadoop.io.AggregateBatchWritable;

public final class BatchPartitioner extends Partitioner<IntWritable, AggregateBatchWritable> {
  public int getPartition(IntWritable key, AggregateBatchWritable value, int reducers) {
    if (key.get() < 0 || key.get() >= reducers)
      throw new IllegalArgumentException("Invalid batch partition");
    return key.get();
  }
}
