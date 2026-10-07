package vn.edu.bigdata.revenue.input;

import java.nio.charset.StandardCharsets;

/** Projects only the four required fields; complex CSV uses the reference parser. */
public final class ProjectedCsvEventParser implements EventParser {
  private final CsvEventParser fallback = new CsvEventParser();

  public ParseResult parse(String line) {
    if (line.indexOf('"') >= 0 || line.indexOf('\r') >= 0 || line.startsWith("\uFEFF"))
      return fallback.parse(line);
    if (line.indexOf('\n') >= 0
        || line.getBytes(StandardCharsets.UTF_8).length > CsvEventParser.MAX_LINE_BYTES)
      return ParseResult.malformed();
    if (line.equals(CsvEventParser.HEADER)) return ParseResult.header();
    int start = 0, column = 0;
    String type = null, id = null, code = null, price = null;
    for (int i = 0; i <= line.length(); i++) {
      if (i == line.length() || line.charAt(i) == ',') {
        if (column == 1) type = line.substring(start, i);
        else if (column == 3) id = line.substring(start, i);
        else if (column == 4) code = line.substring(start, i);
        else if (column == 6) price = line.substring(start, i);
        column++;
        start = i + 1;
      }
    }
    if (column != 9) return ParseResult.malformed();
    return ParseResult.event(new ParsedEvent(type, id, code, price));
  }
}
