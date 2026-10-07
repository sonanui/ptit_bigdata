package vn.edu.bigdata.revenue.job;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import vn.edu.bigdata.revenue.hadoop.io.AggregateBatchWritable;
import vn.edu.bigdata.revenue.hadoop.io.SumCountWritable;
import vn.edu.bigdata.revenue.hadoop.shared.FinalRevenueReducer;
import vn.edu.bigdata.revenue.hadoop.v1.DirectPurchaseMapper;
import vn.edu.bigdata.revenue.hadoop.v2.SumCountCombiner;
import vn.edu.bigdata.revenue.hadoop.v3.InMapperPurchaseMapper;
import vn.edu.bigdata.revenue.hadoop.v4.DensePurchaseMapper;
import vn.edu.bigdata.revenue.hadoop.v5.BatchPartitioner;
import vn.edu.bigdata.revenue.hadoop.v5.BatchPurchaseMapper;
import vn.edu.bigdata.revenue.hadoop.v5.BatchRevenueReducer;

public final class JobPlanFactory {
  private JobPlanFactory() {}

  public static JobPlan build(Configuration base, RunOptions options) throws IOException {
    options.validate(base);
    Configuration conf = new Configuration(base);
    conf.set(RunOptions.GROUP_MODE, options.groupMode.name());
    conf.setInt(RunOptions.MAX_KEYS, options.maxKeys);
    conf.set(RunOptions.FINGERPRINT, options.manifest.fingerprint());
    conf.setBoolean("mapreduce.map.output.compress", options.compress);
    conf.set("mapreduce.map.output.compress.codec", DefaultCodec.class.getName());
    Job job = Job.getInstance(conf, "revenue-" + options.variant);
    job.setJarByClass(JobPlanFactory.class);
    job.setInputFormatClass(TextInputFormat.class);
    for (var entry : options.manifest.files)
      FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(entry.uri));
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(SumCountWritable.class);
    job.setReducerClass(FinalRevenueReducer.class);
    switch (options.variant) {
      case V1_DIRECT:
        job.setMapperClass(DirectPurchaseMapper.class);
        break;
      case V2_COMBINER:
        job.setMapperClass(DirectPurchaseMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        break;
      case V3_IN_MAPPER:
        job.setMapperClass(InMapperPurchaseMapper.class);
        break;
      case V4_DENSE:
        job.setMapperClass(DensePurchaseMapper.class);
        break;
      case V5_BATCH:
        job.setMapperClass(BatchPurchaseMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(AggregateBatchWritable.class);
        job.setPartitionerClass(BatchPartitioner.class);
        job.setReducerClass(BatchRevenueReducer.class);
        break;
      default:
        throw new IllegalArgumentException("Unsupported variant");
    }
    if (options.variant == Variant.V4_DENSE || options.variant == Variant.V5_BATCH) {
      URI uri =
          options.dictionaryPath.getFileSystem(conf).makeQualified(options.dictionaryPath).toUri();
      job.addCacheFile(URI.create(uri + "#group-dictionary.json"));
    }
    job.setNumReduceTasks(options.reducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setCompressOutput(job, false);
    FileOutputFormat.setOutputPath(job, options.output);
    return new JobPlan(List.of(job), options.variant, options.variant, null);
  }
}
