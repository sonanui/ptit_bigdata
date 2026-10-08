package vn.edu.bigdata.revenue.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Strict value flags; Hadoop generic -D flags are consumed by ToolRunner. */
public final class CliArguments {
  private final Map<String, String> values = new HashMap<>();

  public CliArguments(String[] args, Set<String> allowed) {
    for (int i = 0; i < args.length; i += 2) {
      if (i + 1 >= args.length || !args[i].startsWith("--"))
        throw new IllegalArgumentException("Expected --name value");
      String key = args[i].substring(2);
      if (!allowed.contains(key) || values.putIfAbsent(key, args[i + 1]) != null)
        throw new IllegalArgumentException("Unknown/duplicate option: " + key);
    }
  }

  public String required(String key) {
    String v = values.get(key);
    if (v == null || v.isEmpty()) throw new IllegalArgumentException("Required --" + key);
    return v;
  }

  public String get(String key, String fallback) {
    return values.getOrDefault(key, fallback);
  }

  public int integer(String key, int fallback) {
    return Integer.parseInt(get(key, String.valueOf(fallback)));
  }

  public long number(String key, long fallback) {
    return Long.parseLong(get(key, String.valueOf(fallback)));
  }
}
