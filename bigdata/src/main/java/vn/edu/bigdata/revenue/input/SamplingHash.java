package vn.edu.bigdata.revenue.input;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class SamplingHash {
  private SamplingHash() {}

  public static int bucket(String path, long offset, long seed, int buckets) {
    if (buckets < 1) throw new IllegalArgumentException("buckets must be positive");
    if (buckets == 1) return 0;
    MessageDigest digest = InputManifest.digest();
    digest.update(path.getBytes(StandardCharsets.UTF_8));
    digest.update((byte) 0);
    digest.update(ByteBuffer.allocate(16).putLong(seed).putLong(offset).array());
    return Math.floorMod(ByteBuffer.wrap(digest.digest()).getInt(), buckets);
  }
}
