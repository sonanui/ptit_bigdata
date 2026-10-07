package vn.edu.bigdata.revenue.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Atomic, create-only JSON artifacts on Hadoop-compatible filesystems. */
public final class JsonArtifacts {
  private static final ObjectMapper JSON = new ObjectMapper();

  private JsonArtifacts() {}

  public static <T> T read(Path path, FileSystem fs, Class<T> type) throws IOException {
    if (fs.getFileStatus(path).getLen() > 32L * 1024 * 1024)
      throw new IOException("Artifact exceeds 32 MiB: " + path);
    try (FSDataInputStream in = fs.open(path)) {
      return JSON.readValue((java.io.InputStream) in, type);
    }
  }

  public static void write(Path path, FileSystem fs, Object value) throws IOException {
    if (fs.exists(path)) throw new IOException("Artifact already exists: " + path);
    Path parent = path.getParent();
    if (parent != null) fs.mkdirs(parent);
    Path temp = new Path(path.toString() + ".tmp-" + UUID.randomUUID());
    try {
      try (FSDataOutputStream out = fs.create(temp, false)) {
        JSON.writerWithDefaultPrettyPrinter().writeValue((java.io.OutputStream) out, value);
      }
      if (fs.exists(path) || !fs.rename(temp, path))
        throw new IOException("Cannot publish artifact: " + path);
    } finally {
      if (fs.exists(temp)) fs.delete(temp, false);
    }
  }
}
