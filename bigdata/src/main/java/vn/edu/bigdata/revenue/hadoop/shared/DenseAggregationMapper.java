package vn.edu.bigdata.revenue.hadoop.shared;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.*;
import vn.edu.bigdata.revenue.job.RunOptions;
import vn.edu.bigdata.revenue.profile.GroupDictionary;

/** Exactly one array slot per known group; no aggregate-object allocation on merge. */
public abstract class DenseAggregationMapper<K, V> extends Mapper<LongWritable, Text, K, V> {
  protected GroupDictionary dictionary;
  protected long[] sums, counts;
  private Map<String, Integer> ids;
  private GroupMode mode;
  private MapperSupport support;

  protected EventParser parser() {
    return new CsvEventParser();
  }

  protected void setup(Context context) throws IOException {
    mode = GroupMode.parse(context.getConfiguration().get(RunOptions.GROUP_MODE));
    dictionary =
        JsonArtifacts.read(
            new Path("group-dictionary.json"),
            FileSystem.getLocal(context.getConfiguration()),
            GroupDictionary.class);
    dictionary.validate(context.getConfiguration().get(RunOptions.FINGERPRINT), mode.name());
    ids = dictionary.index();
    sums = new long[dictionary.groups.length];
    counts = new long[dictionary.groups.length];
    support = new MapperSupport(parser());
  }

  protected void map(LongWritable offset, Text line, Context context) throws IOException {
    PreparationResult result = support.prepare(line, context, mode);
    if (!result.valid()) return;
    Integer id = ids.get(result.group());
    if (id == null) throw new IOException("Group absent from dictionary: " + result.group());
    sums[id] = Math.addExact(sums[id], result.state().sumMinor());
    counts[id] = Math.addExact(counts[id], 1);
  }
}
