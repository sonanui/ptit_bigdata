package vn.edu.bigdata.revenue.input;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public final class CsvEventParser implements EventParser {
  public static final String HEADER =
      "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session";
  public static final int MAX_LINE_BYTES = 65536;
  private static final String[] HEADER_FIELDS = HEADER.split(",");

  public ParseResult parse(String line) {
    String[] r = fields(line);
    if (r == null) return ParseResult.malformed();
    if (isHeader(r)) return ParseResult.header();
    return ParseResult.event(new ParsedEvent(r[1], r[3], r[4], r[6]));
  }

  /**
   * Splits one physical line into exactly nine RFC 4180 fields, or returns null when the line is
   * malformed. Shared by the MapReduce mappers (through {@link #parse}) and the Spark jobs, so both
   * engines apply the same structural rules.
   */
  public static String[] fields(String line) {
    if (line.endsWith("\r")) line = line.substring(0, line.length() - 1);
    if (line.getBytes(StandardCharsets.UTF_8).length > MAX_LINE_BYTES || line.indexOf('\n') >= 0)
      return null;
    if (line.indexOf('\r') >= 0) return null;
    if (line.startsWith("﻿")) line = line.substring(1);
    try (CSVParser parser = CSVParser.parse(line, CSVFormat.RFC4180)) {
      List<CSVRecord> rows = parser.getRecords();
      if (rows.size() != 1 || rows.get(0).size() != 9) return null;
      CSVRecord r = rows.get(0);
      String[] values = new String[9];
      for (int i = 0; i < 9; i++) values[i] = r.get(i);
      return values;
    } catch (IOException | java.io.UncheckedIOException | IllegalArgumentException e) {
      return null;
    }
  }

  public static boolean isHeader(String[] fields) {
    return Arrays.equals(fields, HEADER_FIELDS);
  }
}
