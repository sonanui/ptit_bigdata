package vn.edu.bigdata.revenue.input;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import vn.edu.bigdata.revenue.domain.GroupMode;

class PurchasePreparationTest {
  private final CsvEventParser parser = new CsvEventParser();

  private PreparationResult prepare(String line) {
    return PurchasePreparation.prepare(parser.parse(line), GroupMode.CATEGORY_ID);
  }

  @Test
  void filtersBeforeParsingPriceAndClassifiesOnce() {
    assertEquals(PreparationResult.Reason.NON_PURCHASE, prepare("t,view,1,1,a,b,NaN,u,s").reason());
    assertEquals(PreparationResult.Reason.UNKNOWN_EVENT, prepare("t,other,1,1,a,b,1,u,s").reason());
    assertEquals(
        PreparationResult.Reason.INVALID_PRICE, prepare("t,purchase,1,,a,b,NaN,u,s").reason());
    assertEquals(
        PreparationResult.Reason.INVALID_GROUP, prepare("t,purchase,1,,a,b,1,u,s").reason());
    PreparationResult result = prepare("t,purchase,1,1,a,b,0,u,s");
    assertTrue(result.valid());
    assertEquals(0, result.state().sumMinor());
    assertEquals(1, result.state().purchaseCount());
  }

  @Test
  void handlesQuotedCommaTrailingEmptyBomAndHeader() {
    assertTrue(prepare("t,purchase,1,1,a,\"x,y\",10.00,u,").valid());
    assertTrue(prepare("t,purchase,1,1,a,\"x\"\"y\",10.00,u,\r").valid());
    assertEquals(
        PreparationResult.Reason.HEADER, prepare("\uFEFF" + CsvEventParser.HEADER).reason());
    assertEquals(
        PreparationResult.Reason.MALFORMED_CSV, prepare("t,purchase,1,1,a,b,1,u").reason());
    assertEquals(
        PreparationResult.Reason.MALFORMED_CSV,
        prepare("t,purchase,1,1,a,\"broken,1,u,s").reason());
  }

  @Test
  void missingCategoryCodeIsAnExplicitGroup() {
    PreparationResult r =
        PurchasePreparation.prepare(
            parser.parse("t,purchase,1,1,,b,1,u,s"), GroupMode.CATEGORY_ROOT);
    assertEquals("__UNKNOWN__", r.group());
    assertTrue(r.missingCategoryCode());
  }
}
