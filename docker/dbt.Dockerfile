# dbt + BigQuery runner.
FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core==1.8.3 dbt-bigquery==1.8.2

ENV DBT_PROFILES_DIR=/dbt
WORKDIR /dbt
COPY dbt /dbt

CMD ["dbt", "--version"]
