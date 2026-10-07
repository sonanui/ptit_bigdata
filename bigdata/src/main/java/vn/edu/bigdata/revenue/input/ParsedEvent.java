package vn.edu.bigdata.revenue.input;

public final class ParsedEvent {
  public final String eventType, categoryId, categoryCode, priceText;

  public ParsedEvent(String eventType, String categoryId, String categoryCode, String priceText) {
    this.eventType = eventType.trim();
    this.categoryId = categoryId;
    this.categoryCode = categoryCode;
    this.priceText = priceText;
  }
}
