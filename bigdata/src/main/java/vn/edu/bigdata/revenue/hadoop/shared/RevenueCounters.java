package vn.edu.bigdata.revenue.hadoop.shared;

public enum RevenueCounters {
  HEADERS,
  MALFORMED_CSV,
  UNKNOWN_EVENT,
  NON_PURCHASE,
  INVALID_PRICE,
  INVALID_GROUP,
  VALID_PURCHASE,
  MISSING_CATEGORY_CODE,
  CACHE_FLUSHES,
  CACHE_PEAK_KEYS
}
