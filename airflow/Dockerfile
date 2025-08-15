FROM apache/airflow:2.10.5

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

USER root

# Java + (curl + certificates)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jdk curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Spark 3.4.0 + Hadoop 3
ARG SPARK_VERSION=3.4.0
ARG HADOOP_VERSION=3
RUN curl -fsSL -o /tmp/spark.tgz \
      https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
 && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${PATH}:/opt/spark/bin"

# S3a JARs
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && chown -R airflow:0 /opt/spark/jars

# --- Delta Lake jars (para Spark 3.4 / Scala 2.12) ---
ARG DELTA_VERSION=2.4.0
RUN curl -fsSL -o /opt/spark/jars/delta-core_2.12-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
 && chown -R airflow:0 /opt/spark/jars

USER airflow
