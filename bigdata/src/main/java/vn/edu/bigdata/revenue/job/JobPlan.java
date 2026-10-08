package vn.edu.bigdata.revenue.job;

import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public final class JobPlan {
  public final List<Job> stages;
  public final Variant requested, effective;
  public final Path intermediate;

  public JobPlan(List<Job> stages, Variant requested, Variant effective, Path intermediate) {
    this.stages = List.copyOf(stages);
    this.requested = requested;
    this.effective = effective;
    this.intermediate = intermediate;
  }
}
