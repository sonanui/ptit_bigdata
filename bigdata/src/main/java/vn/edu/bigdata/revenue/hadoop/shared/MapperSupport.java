package vn.edu.bigdata.revenue.hadoop.shared;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.input.CsvEventParser;
import vn.edu.bigdata.revenue.input.EventParser;
import vn.edu.bigdata.revenue.input.PreparationResult;
import vn.edu.bigdata.revenue.input.PurchasePreparation;

public final class MapperSupport {
  private final EventParser parser;

  public MapperSupport() {
    this(new CsvEventParser());
  }

  public MapperSupport(EventParser parser) {
    this.parser = parser;
  }

  public PreparationResult prepare(Text line, Mapper.Context context, GroupMode mode) {
    PreparationResult result = PurchasePreparation.prepare(parser.parse(line.toString()), mode);
    String reason = result.reason().name();
    if (reason.equals("HEADER")) reason = "HEADERS";
    context.getCounter(RevenueCounters.valueOf(reason)).increment(1);
    if (result.missingCategoryCode())
      context.getCounter(RevenueCounters.MISSING_CATEGORY_CODE).increment(1);
    return result;
  }
}
