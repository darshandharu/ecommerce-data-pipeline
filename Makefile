# ============================================================
#  E-Commerce Data Pipeline — developer convenience targets
# ============================================================
.PHONY: help install lint format test \
        download-data generate-cdc \
        run-bronze run-silver run-gold run-dq run-cdc run-all \
        dbt-deps dbt-run dbt-test dbt-docs \
        dashboard up down logs clean

PY ?= python
SPARK_SUBMIT ?= spark-submit

help:
	@echo "Targets:"
	@echo "  install        Install python dependencies"
	@echo "  lint/format    ruff + black"
	@echo "  test           Run pytest"
	@echo "  download-data  Download Olist dataset from Kaggle"
	@echo "  generate-cdc   Generate simulated daily CDC files"
	@echo "  run-bronze     Raw ingestion (PySpark)"
	@echo "  run-silver     Clean + standardize (PySpark)"
	@echo "  run-gold       Business aggregates + BigQuery load"
	@echo "  run-dq         Run the data quality framework"
	@echo "  run-cdc        Apply a day's CDC batch"
	@echo "  run-all        bronze -> silver -> dq -> gold"
	@echo "  dbt-run/test   Run / test dbt models"
	@echo "  dashboard      Launch the Streamlit DQ dashboard"
	@echo "  up/down        docker-compose lifecycle"

install:
	$(PY) -m pip install -r requirements.txt

lint:
	ruff check pyspark_jobs dags scripts monitoring tests

format:
	black pyspark_jobs dags scripts monitoring tests
	ruff check --fix pyspark_jobs dags scripts monitoring tests

test:
	pytest -q --cov=pyspark_jobs tests/

download-data:
	bash scripts/download_data.sh

generate-cdc:
	$(PY) scripts/generate_cdc_files.py --days 7

run-bronze:
	$(PY) -m pyspark_jobs.bronze.ingest_raw --config configs/pipeline_config.yaml

run-silver:
	$(PY) -m pyspark_jobs.silver.run_silver --config configs/pipeline_config.yaml

run-dq:
	$(PY) -m pyspark_jobs.data_quality.run_dq --config configs/pipeline_config.yaml --layer silver

run-gold:
	$(PY) -m pyspark_jobs.gold.run_gold --config configs/pipeline_config.yaml

run-cdc:
	$(PY) -m pyspark_jobs.cdc.cdc_processor --config configs/pipeline_config.yaml --date $(DATE)

run-all: run-bronze run-silver run-dq run-gold

dbt-deps:
	cd dbt && dbt deps

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

dashboard:
	streamlit run monitoring/dashboard/dq_dashboard.py

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

clean:
	rm -rf spark-warehouse metastore_db derby.log dbt/target dbt/logs
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
