package vn.edu.bigdata.revenue.hadoop;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import vn.edu.bigdata.revenue.domain.AggregateState;
import vn.edu.bigdata.revenue.hadoop.shared.BoundedAccumulator;

class BoundedAccumulatorTest {
  @Test
  void flushDoesNotLoseTheTriggeringRecord() throws Exception {
    Map<String, AggregateState> results = new HashMap<>();
    BoundedAccumulator<String> cache = new BoundedAccumulator<>(1);
    BoundedAccumulator.Emitter<String> sink =
        (key, value) -> results.merge(key, value, AggregateState::merge);
    cache.add("A", AggregateState.single(1000), sink);
    cache.add("B", AggregateState.single(2000), sink);
    cache.add("A", AggregateState.single(3000), sink);
    assertEquals(1, cache.size());
    cache.flush(sink);
    cache.flush(sink);
    assertEquals(4000, results.get("A").sumMinor());
    assertEquals(2, results.get("A").purchaseCount());
    assertEquals(2000, results.get("B").sumMinor());
  }
}
