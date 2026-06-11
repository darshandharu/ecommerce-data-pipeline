# Standalone PySpark runner (jobs + DQ dashboard).
FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
# Lean runtime deps (pyspark + dashboard); Airflow/dbt live in their own images.
COPY docker/runtime-requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pyspark_jobs /app/pyspark_jobs
COPY monitoring  /app/monitoring
COPY configs     /app/configs
COPY scripts     /app/scripts

# Create a user at UID 50000 (gid 0) matching the Airflow workers, so files
# written by this container and Airflow are mutually overwritable AND Hadoop's
# UnixPrincipal can resolve a username (avoids KerberosAuthException when the
# container runs as a non-root UID). Placed last to keep the pip layer cached.
RUN useradd --uid 50000 --gid 0 --create-home --home-dir /home/airflow \
    --shell /bin/bash airflow

# default: show the make help / job entrypoints
CMD ["python", "-c", "import pyspark; print('PySpark', pyspark.__version__, 'ready')"]
