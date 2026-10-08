package vn.edu.bigdata.webapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Lớp trình bày của pipeline: đọc serving artifacts đã publish (analytics, benchmark, mô hình) và
 * suy luận K-Means/KNN. Không train, không chạy Spark/MapReduce, không đọc HDFS hay CSV thô.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class WebappApplication {
  public static void main(String[] args) {
    SpringApplication.run(WebappApplication.class, args);
  }
}
