# Image "bigdata": build + chạy Hadoop MapReduce (module bigdata, JDK 11, LocalJobRunner) và các script Python.
# Spark không chạy trong image này (chạy local trên host, xem docs/spark-local.md); web có Dockerfile riêng trong webapp/.
FROM maven:3.9.11-eclipse-temurin-11

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/bigdata
# pom gốc là aggregator; trên JDK 11 chỉ module bigdata được kích hoạt.
COPY pom.xml mvnw ./
COPY bigdata/pom.xml ./bigdata/
RUN chmod +x mvnw && ./mvnw -B -pl bigdata dependency:go-offline

COPY bigdata/src ./bigdata/src
COPY scripts ./scripts
COPY config ./config
RUN chmod +x scripts/*.sh \
    && ./mvnw -B -pl bigdata -DskipTests package dependency:build-classpath \
       -DincludeScope=compile -Dmdep.outputFile=/opt/bigdata/.build-cache/classpath.txt \
    && mkdir -p data/raw data/samples results

CMD ["bash", "scripts/demo.sh", "results/docker-demo"]
