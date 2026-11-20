# StreamGuard processor: Python 3.13 + Java 17 + PySpark 4.0.1. Build context: repo root.
# All Spark jars are downloaded here, so nothing is fetched from Maven at runtime.
# Code is not copied: ../src, ../sql and ../config are mounted read-only.
FROM python:3.13-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_JARS_DIR=/opt/jars \
    PYTHONPATH=/opt/streamguard/src \
    PYTHONUNBUFFERED=1

# Kafka source for Structured Streaming and its compile dependencies (from the 4.0.1 POM;
# lz4, snappy, zstd, slf4j and hadoop-client come with PySpark), plus the PostgreSQL JDBC
# driver. Versions are pinned and checked into this file.
ARG MAVEN=https://repo1.maven.org/maven2
RUN mkdir -p /opt/jars && cd /opt/jars \
    && curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar \
    && curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
    && curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    && curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
    && curl -fsSLO ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    && ls -l /opt/jars

WORKDIR /opt/streamguard
COPY requirements.txt requirements-spark.txt ./
RUN pip install --no-cache-dir -r requirements-spark.txt

# Spark UI (published on 127.0.0.1:4040 by compose).
EXPOSE 4040

ENTRYPOINT ["python", "-m", "streamguard.processor"]
