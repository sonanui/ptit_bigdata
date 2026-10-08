package vn.edu.bigdata.revenue.input;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import vn.edu.bigdata.revenue.domain.GroupMode;

class ProjectedCsvEventParserTest {
  @Test
  void projectionPreservesCsvSemanticsAndPreparation() {
    var reference = new CsvEventParser();
    var projected = new ProjectedCsvEventParser();
    for (String line :
        new String[] {
          CsvEventParser.HEADER,
          "t,purchase,1,1,a,b,12.30,u,",
          "t,view,1,1,a,b,NaN,u,s",
          "t,purchase,1,1,a,\"quoted,brand\",0,u,s",
          "\uFEFF" + CsvEventParser.HEADER,
          "t,purchase,1,1,a,b,1,u,s\r",
          "t,purchase,1,1,a,b,1,u",
          "t,purchase,1,1,a,\"two\rlines\",1,u,s",
          "t,purchase,1,1,a,b,1,u,s\n"
        }) {
      var expected = PurchasePreparation.prepare(reference.parse(line), GroupMode.CATEGORY_ID);
      var actual = PurchasePreparation.prepare(projected.parse(line), GroupMode.CATEGORY_ID);
      assertEquals(expected.reason(), actual.reason(), line);
      if (expected.valid()) {
        assertEquals(expected.group(), actual.group());
        assertEquals(expected.state().sumMinor(), actual.state().sumMinor());
      }
    }
  }
}
