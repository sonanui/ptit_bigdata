package vn.edu.bigdata.webapp.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import vn.edu.bigdata.webapp.serving.ArtifactException;
import vn.edu.bigdata.webapp.serving.ArtifactNotFoundException;

/** Lỗi trả về dạng RFC 9457 (ProblemDetail): không có mô hình/dữ liệu thì nói rõ, không trả kết quả giả. */
@RestControllerAdvice
class ApiErrors {
  @ExceptionHandler(ArtifactNotFoundException.class)
  ProblemDetail notFound(ArtifactNotFoundException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
  }

  @ExceptionHandler(ArtifactException.class)
  ProblemDetail broken(ArtifactException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
  }

  @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
  ProblemDetail invalid(RuntimeException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
  }
}
