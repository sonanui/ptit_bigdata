package vn.edu.bigdata.webapp.serving;

/** Run, mô hình, sản phẩm hoặc artifact được yêu cầu không tồn tại: HTTP 404. */
public class ArtifactNotFoundException extends RuntimeException {
  public ArtifactNotFoundException(String message) {
    super(message);
  }
}
