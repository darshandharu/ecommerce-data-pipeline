#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Run the whole medallion pipeline locally (no GCP). Intended to be run inside
# the `spark` Docker container:
#
#   docker compose --profile tools run --rm spark bash scripts/run_local_pipeline.sh
#
# Steps: generate CDC files -> bronze -> silver -> data quality -> gold (no BQ).
# ---------------------------------------------------------------------------
set -euo pipefail

CONFIG="configs/pipeline_config.yaml"

# One shared run id across all stages so the dashboard can correlate a single
# pipeline execution (DQ results + row counts + timings).
export PIPELINE_RUN_ID="local-$(date -u +%Y%m%dT%H%M%S)"
echo "PIPELINE_RUN_ID=$PIPELINE_RUN_ID"

echo "==================================================================="
echo " 0/5  Generating simulated daily CDC files"
echo "==================================================================="
python scripts/generate_cdc_files.py --days 7 --start 2018-01-01

echo "==================================================================="
echo " 1/5  BRONZE — raw ingestion"
echo "==================================================================="
python -m pyspark_jobs.bronze.ingest_raw --config "$CONFIG"

echo "==================================================================="
echo " 2/5  SILVER — clean + standardize"
echo "==================================================================="
python -m pyspark_jobs.silver.run_silver --config "$CONFIG"

echo "==================================================================="
echo " 3/5  DATA QUALITY — validation gate (report-only here)"
echo "==================================================================="
# --no-fail so a WARN/FAIL doesn't abort the demo run; results still recorded
python -m pyspark_jobs.data_quality.run_dq --config "$CONFIG" --layer silver --no-fail

echo "==================================================================="
echo " 4/5  GOLD — star schema (BigQuery skipped in local-only mode)"
echo "==================================================================="
python -m pyspark_jobs.gold.run_gold --config "$CONFIG" --no-bq

echo "==================================================================="
echo " 5/5  DONE — audit data written to data/audit/"
echo "      Launch the dashboard:  docker compose --profile dashboard up dashboard"
echo "==================================================================="
