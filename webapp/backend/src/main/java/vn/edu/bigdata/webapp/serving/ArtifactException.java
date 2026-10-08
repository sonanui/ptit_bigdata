package vn.edu.bigdata.webapp.serving;

/** Artifact có trong hợp đồng nhưng hỏng (thiếu file, sai checksum, JSON/CSV lỗi): HTTP 503. */
public class ArtifactException extends RuntimeException {
  public ArtifactException(String message, Throwable cause) {
    super(message, cause);
  }
}
