package vn.edu.bigdata.revenue.input;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Reads bounded UTF-8 physical lines, preserving Hadoop TextInputFormat byte offsets. */
public final class BoundedLineReader {
  @FunctionalInterface
  public interface Visitor {
    void accept(long offset, String line) throws IOException;
  }

  private BoundedLineReader() {}

  public static InputManifest.Entry scan(Path path, FileSystem fs, Visitor visitor)
      throws IOException {
    MessageDigest digest = InputManifest.digest();
    long offset = 0, lineOffset = 0;
    ByteArrayOutputStream line = new ByteArrayOutputStream();
    try (InputStream in = new BufferedInputStream(new DigestInputStream(fs.open(path), digest))) {
      int b;
      while ((b = in.read()) != -1) {
        offset++;
        if (b == 10) {
          visitor.accept(lineOffset, decode(line.toByteArray()));
          line.reset();
          lineOffset = offset;
        } else {
          if (line.size() >= CsvEventParser.MAX_LINE_BYTES)
            throw new IOException("CSV line exceeds 64 KiB: " + path);
          line.write(b);
        }
      }
      if (line.size() > 0) visitor.accept(lineOffset, decode(line.toByteArray()));
    }
    return new InputManifest.Entry(
        fs.makeQualified(path).toUri().normalize().toString(),
        offset,
        InputManifest.hex(digest.digest()));
  }

  private static String decode(byte[] bytes) throws IOException {
    int length = bytes.length;
    if (length > 0 && bytes[length - 1] == 13) length--;
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes, 0, length))
          .toString();
    } catch (CharacterCodingException e) {
      throw new IOException("Invalid UTF-8", e);
    }
  }
}
