package vn.edu.bigdata.revenue.input;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class InputManifest {
  public int schemaVersion = 1;
  public String policyHash = policyHash();
  public List<Entry> files = new ArrayList<>();

  public static final class Entry {
    public String uri, sha256;
    public long bytes;

    public Entry() {}

    public Entry(String uri, long bytes, String sha256) {
      this.uri = uri;
      this.bytes = bytes;
      this.sha256 = sha256;
    }
  }

  public String fingerprint() {
    if (schemaVersion != 1 || !policyHash().equals(policyHash) || files.isEmpty())
      throw new IllegalArgumentException("Invalid manifest schema/policy/files");
    List<Entry> sorted = new ArrayList<>(files);
    sorted.sort(Comparator.comparing(e -> e.uri));
    Set<String> seen = new HashSet<>();
    StringBuilder canonical = new StringBuilder(policyHash).append('\n');
    for (Entry e : sorted) {
      if (e.uri == null
          || !seen.add(e.uri)
          || e.bytes < 0
          || e.sha256 == null
          || !e.sha256.matches("[0-9a-f]{64}"))
        throw new IllegalArgumentException("Invalid/duplicate manifest entry");
      canonical
          .append(e.uri)
          .append('\t')
          .append(e.bytes)
          .append('\t')
          .append(e.sha256)
          .append('\n');
    }
    return sha256(canonical.toString());
  }

  public static String policyHash() {
    return sha256(
        "revenue-v1:purchase:scale2:halfup:unknown-code:category-id-string:no-dedup:key256:line65536");
  }

  public static MessageDigest digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String hex(byte[] bytes) {
    StringBuilder out = new StringBuilder();
    for (byte b : bytes) out.append(String.format("%02x", b & 255));
    return out.toString();
  }

  public static String sha256(String text) {
    return hex(digest().digest(text.getBytes(StandardCharsets.UTF_8)));
  }
}
