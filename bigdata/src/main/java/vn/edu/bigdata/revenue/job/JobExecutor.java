package vn.edu.bigdata.revenue.job;

import org.apache.hadoop.mapreduce.Job;
import vn.edu.bigdata.revenue.hadoop.shared.RevenueCounters;
import vn.edu.bigdata.revenue.metrics.MetricsCollector;
import vn.edu.bigdata.revenue.metrics.RunManifest;

public final class JobExecutor {
  private JobExecutor() {}

  public static RunManifest execute(JobPlan plan) {
    RunManifest manifest = new RunManifest();
    manifest.requestedVariant = plan.requested.name();
    manifest.effectiveVariant = plan.effective.name();
    for (Job job : plan.stages) {
      RunManifest.Stage stage = new RunManifest.Stage();
      stage.name = job.getJobName();
      long start = System.nanoTime();
      manifest.stages.add(stage);
      try {
        stage.success = job.waitForCompletion(false);
        stage.jobId = String.valueOf(job.getJobID());
        stage.counters = MetricsCollector.collect(job);
        if (stage.success
            && job.getCounters().findCounter(RevenueCounters.MALFORMED_CSV).getValue() > 0) {
          stage.success = false;
          stage.error = "Runtime malformed CSV";
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        stage.success = false;
        stage.error = e.toString();
      }
      stage.elapsedMillis = (System.nanoTime() - start) / 1000000;
      if (!stage.success) {
        manifest.validationStatus = "failed";
        manifest.error = stage.error == null ? "Job failed: " + stage.jobId : stage.error;
        break;
      }
    }
    return manifest;
  }
}
