package vn.edu.bigdata.revenue.hadoop.shared;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import vn.edu.bigdata.revenue.domain.AggregateState;

public final class BoundedAccumulator<K> {
  @FunctionalInterface
  public interface Emitter<K> {
    void emit(K key, AggregateState value) throws IOException, InterruptedException;
  }

  private final int maxKeys;
  private final Map<K, AggregateState> cache = new HashMap<>();
  private long flushes;
  private int peak;

  public BoundedAccumulator(int maxKeys) {
    if (maxKeys < 1) throw new IllegalArgumentException("maxKeys must be positive");
    this.maxKeys = maxKeys;
  }

  public void add(K key, AggregateState value, Emitter<K> emitter)
      throws IOException, InterruptedException {
    if (!cache.containsKey(key) && cache.size() == maxKeys) flush(emitter);
    cache.merge(key, value, AggregateState::merge);
    peak = Math.max(peak, cache.size());
  }

  public void flush(Emitter<K> emitter) throws IOException, InterruptedException {
    if (cache.isEmpty()) return;
    for (Map.Entry<K, AggregateState> entry : cache.entrySet())
      emitter.emit(entry.getKey(), entry.getValue());
    cache.clear();
    flushes++;
  }

  public int size() {
    return cache.size();
  }

  public long flushes() {
    return flushes;
  }

  public int peak() {
    return peak;
  }
}
