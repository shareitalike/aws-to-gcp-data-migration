FROM apache/airflow:2.8.1

USER root

# Install OpenJDK-17 (needed for Spark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Set JAVA_HOME so Spark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME
