package vn.edu.bigdata.revenue.input;

public final class ParseResult {
  public enum Kind {
    HEADER,
    EVENT,
    MALFORMED
  }

  public final Kind kind;
  public final ParsedEvent event;

  private ParseResult(Kind kind, ParsedEvent event) {
    this.kind = kind;
    this.event = event;
  }

  public static ParseResult header() {
    return new ParseResult(Kind.HEADER, null);
  }

  public static ParseResult malformed() {
    return new ParseResult(Kind.MALFORMED, null);
  }

  public static ParseResult event(ParsedEvent event) {
    return new ParseResult(Kind.EVENT, event);
  }
}
