package vn.edu.bigdata.revenue.input;

import static vn.edu.bigdata.revenue.input.PreparationResult.Reason.*;

import java.nio.charset.StandardCharsets;
import vn.edu.bigdata.revenue.domain.GroupKeyResolver;
import vn.edu.bigdata.revenue.domain.GroupMode;
import vn.edu.bigdata.revenue.domain.Money;
import vn.edu.bigdata.revenue.domain.Purchase;

public final class PurchasePreparation {
  private PurchasePreparation() {}

  public static PreparationResult prepare(ParseResult parsed, GroupMode mode) {
    if (parsed.kind == ParseResult.Kind.HEADER) return PreparationResult.skipped(HEADER);
    if (parsed.kind == ParseResult.Kind.MALFORMED) return PreparationResult.skipped(MALFORMED_CSV);
    ParsedEvent e = parsed.event;
    if (!e.eventType.equals("purchase")) {
      return PreparationResult.skipped(
          e.eventType.equals("view")
                  || e.eventType.equals("cart")
                  || e.eventType.equals("remove_from_cart")
              ? NON_PURCHASE
              : UNKNOWN_EVENT);
    }
    long price;
    try {
      price = Money.parseMinor(e.priceText);
    } catch (IllegalArgumentException ex) {
      return PreparationResult.skipped(INVALID_PRICE);
    }
    Purchase p = new Purchase(e.categoryId, e.categoryCode, price);
    try {
      String group = GroupKeyResolver.resolve(p, mode);
      if (group.isEmpty() || group.getBytes(StandardCharsets.UTF_8).length > 256)
        return PreparationResult.skipped(INVALID_GROUP);
      return PreparationResult.accepted(
          group, price, mode != GroupMode.CATEGORY_ID && p.categoryCode.isEmpty());
    } catch (IllegalArgumentException ex) {
      return PreparationResult.skipped(INVALID_GROUP);
    }
  }
}
