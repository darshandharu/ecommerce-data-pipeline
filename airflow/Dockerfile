# Airflow image with Java (for local Spark), Google + Spark providers, dbt.
FROM apache/airflow:2.9.3-python3.11

USER root
# Java is required to run PySpark locally inside the Airflow workers
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk-headless procps curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
