# Backend Spring Boot (chỉ /api): đọc serving artifact mount chỉ đọc tại /serving, suy luận K-Means/KNN bằng Java.
# Build: docker compose --profile web build web-backend
FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
RUN mvn -B -q dependency:go-offline
COPY src ./src
# Test parity với artifact thật cần thư mục serving nên tự bỏ qua trong image; unit test vẫn chạy.
RUN mvn -B package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/revenue-webapp.jar app.jar
ENV SERVING_DIR=/serving
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=6 \
  CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8080 && printf 'GET /api/health HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '200' <&3"]
ENTRYPOINT ["java", "-Xmx512m", "-jar", "app.jar"]
