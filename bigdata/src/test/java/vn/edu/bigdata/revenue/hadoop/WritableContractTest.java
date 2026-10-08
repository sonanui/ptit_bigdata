package vn.edu.bigdata.revenue.hadoop;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.junit.jupiter.api.Test;
import vn.edu.bigdata.revenue.hadoop.io.SumCountWritable;

class WritableContractTest {
  @Test
  void roundTripsStateWithoutAliasing() throws Exception {
    SumCountWritable original = new SumCountWritable(6000, 3);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    original.write(new DataOutputStream(bytes));
    SumCountWritable copy = new SumCountWritable();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    var state = copy.toState();
    copy.set(0, 1);
    assertEquals(6000, state.sumMinor());
    assertEquals(3, state.purchaseCount());
  }

  @Test
  void batchesRoundTripAndPreserveReducerOwnership() throws Exception {
    var original =
        new vn.edu.bigdata.revenue.hadoop.io.AggregateBatchWritable(
            new int[] {0, 2}, new long[] {6000, 0}, new long[] {3, 1});
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    original.write(new DataOutputStream(bytes));
    var copy = new vn.edu.bigdata.revenue.hadoop.io.AggregateBatchWritable();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    assertEquals(2, copy.size());
    assertEquals(6000, copy.sum(0));
    assertEquals(1, copy.count(1));
    var partitioner = new vn.edu.bigdata.revenue.hadoop.v5.BatchPartitioner();
    assertEquals(1, partitioner.getPartition(new org.apache.hadoop.io.IntWritable(1), copy, 2));
    assertThrows(
        IllegalArgumentException.class,
        () -> partitioner.getPartition(new org.apache.hadoop.io.IntWritable(2), copy, 2));
  }
}
