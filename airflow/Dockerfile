FROM apache/airflow:2.10.5
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /requirements.txt

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow