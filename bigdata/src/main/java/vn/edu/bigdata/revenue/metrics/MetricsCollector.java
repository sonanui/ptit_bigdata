package vn.edu.bigdata.revenue.metrics;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

public final class MetricsCollector {
  private MetricsCollector() {}

  public static Map<String, Long> collect(Job job) throws IOException {
    Map<String, Long> result = new TreeMap<>();
    Counters counters = job.getCounters();
    if (counters != null)
      for (CounterGroup group : counters)
        for (Counter counter : group)
          result.put(group.getName() + "." + counter.getName(), counter.getValue());
    return result;
  }
}
