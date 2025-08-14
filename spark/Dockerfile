# Use a single stage build to avoid issues
FROM openjdk:11.0.11-jre-slim-buster

# Install essential packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    python3 \
    python3-pip \
    procps \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Environment variables
ENV SPARK_VERSION=3.4.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

# Download and install Spark
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p /opt/spark \
    && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
    && rm apache-spark.tgz

# Download AWS SDK, Hadoop AWS and Delta jars directly into spark jars folder
RUN mkdir -p /opt/spark/jars && \
    cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-contribs_2.12/2.4.0/delta-contribs_2.12-2.4.0.jar

# Python dependencies
RUN pip3 install "pyzmq==25.1.1" && pip3 install nbconvert

# Spark environment setup
ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
    SPARK_WORKER_WEBUI_PORT=8080 \
    SPARK_WORKER_PORT=7000 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

# Create logs directory
RUN mkdir -p ${SPARK_LOG_DIR} \
    && touch ${SPARK_MASTER_LOG} ${SPARK_WORKER_LOG} \
    && ln -sf /dev/stdout ${SPARK_MASTER_LOG} \
    && ln -sf /dev/stdout ${SPARK_WORKER_LOG}

# Copy start script
COPY start-spark.sh /

# Final verification
RUN echo "Verificando JARs da AWS e Delta:" \
    && ls -la /opt/spark/jars/*aws* \
    && ls -la /opt/spark/jars/*delta* \
    && echo "Conteúdo da pasta jars:" \
    && ls -la /opt/spark/jars/

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/start-spark.sh"]
