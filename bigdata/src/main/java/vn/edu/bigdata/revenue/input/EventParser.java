package vn.edu.bigdata.revenue.input;

@FunctionalInterface
public interface EventParser {
  ParseResult parse(String line);
}
