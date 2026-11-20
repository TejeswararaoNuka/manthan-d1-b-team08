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

# Spark jars from deploy/processor/jars.txt (the same list CI downloads).
ARG MAVEN=https://repo1.maven.org/maven2
COPY deploy/processor/jars.txt /tmp/jars.txt
RUN mkdir -p /opt/jars \
    && grep -v '^#' /tmp/jars.txt | grep . | while read -r path; do \
         curl -fsSL -o "/opt/jars/$(basename "$path")" "${MAVEN}/${path}" || exit 1; \
       done \
    && ls -l /opt/jars

WORKDIR /opt/streamguard
COPY requirements.txt requirements-spark.txt ./
RUN pip install --no-cache-dir -r requirements-spark.txt

# Spark UI (published on 127.0.0.1:4040 by compose).
EXPOSE 4040

ENTRYPOINT ["python", "-m", "streamguard.processor"]
