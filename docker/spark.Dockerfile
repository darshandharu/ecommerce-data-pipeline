# Standalone PySpark runner (jobs + DQ dashboard).
FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pyspark_jobs /app/pyspark_jobs
COPY monitoring  /app/monitoring
COPY configs     /app/configs
COPY scripts     /app/scripts

# default: show the make help / job entrypoints
CMD ["python", "-c", "import pyspark; print('PySpark', pyspark.__version__, 'ready')"]
