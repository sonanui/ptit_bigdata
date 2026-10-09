# Image "spark": chạy các job Spark Java (package vn.edu.bigdata.revenue.spark) và notebook K-Means/KNN trong Docker.
# JDK 21 (Spark 4.0 cần JDK 17 hoặc 21), spark-submit lấy từ PySpark 4.0.4 (trùng spark.version trong bigdata/pom.xml).
# Build: docker compose build spark   (build từ gốc repo; profile spark của module bigdata tự bật vì JDK >= 17)
FROM maven:3.9.11-eclipse-temurin-21

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv git \
    && rm -rf /var/lib/apt/lists/* \
    && git config --system --add safe.directory '*'

# Môi trường Python của notebook: cùng phiên bản với notebooks/requirements.txt.
RUN python3 -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
COPY notebooks/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && ln -s "$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:$PATH

# Jar Spark: build và chạy test (profile spark gồm cả test MapReduce) ngay khi build image.
# Trên JDK >= 17, pom gốc tự bật profile webapp (module webapp/backend, không có trong image này) nên tắt nó.
WORKDIR /opt/build
COPY pom.xml ./
COPY bigdata/pom.xml ./bigdata/
RUN mvn -B -q -P '!webapp' -pl bigdata dependency:go-offline
COPY bigdata/src ./bigdata/src
RUN mvn -B -P '!webapp' -pl bigdata package \
    && mkdir -p /opt/app && cp bigdata/target/revenue-aggregation-spark.jar /opt/app/
ENV SPARK_APP_JAR=/opt/app/revenue-aggregation-spark.jar

# Repo được bind-mount vào /opt/project (scripts, notebooks, results, docs/evidence).
WORKDIR /opt/project
CMD ["bash"]
