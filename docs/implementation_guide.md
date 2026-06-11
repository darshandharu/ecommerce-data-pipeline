# Step-by-Step Implementation Guide

A walkthrough to take this project from clone to a working portfolio piece,
with the "why" behind each step so you can talk through it in interviews.

## Step 0 — Understand the goal

Build an ETL/ELT pipeline that turns 9 raw Olist CSVs into governed analytics
marts in BigQuery, orchestrated by Airflow, modelled by dbt, with a data
quality framework, CDC, and an observability dashboard.

## Step 1 — Environment

1. `python -m venv .venv && source .venv/bin/activate`
2. `make install`
3. `cp .env.example .env` and fill in (GCP optional — local-only works).

## Step 2 — Get the data

```bash
make download-data          # Kaggle: olistbr/brazilian-ecommerce -> data/raw/
```
Why declared schemas? See `configs/tables_schema.yaml` — deterministic typing
beats `inferSchema` for reproducibility and catches malformed inputs early.

## Step 3 — Bronze (raw ingestion)

```bash
make run-bronze
```
- Reads each CSV with its declared schema.
- Stamps lineage columns (`_run_id`, `_source_file`, `_ingested_at`).
- Writes Parquet partitioned by `_ingest_date`.
- Logs row counts + duration to `audit.pipeline_run_log`.

## Step 4 — Silver (clean + standardize)

```bash
make run-silver
```
Per-table recipes in `pyspark_jobs/silver/run_silver.py` compose reusable
transforms (`transformations/cleaning.py`, `business_rules.py`):
null handling, type correction, dedup on natural keys, standardization
(state/city casing), date formatting, and business-rule flags
(`is_valid_order`, delivery metrics).

## Step 5 — Data Quality gate

```bash
make run-dq                 # --no-fail for report-only
```
- Runs `configs/dq_rules.yaml`: nulls, duplicates, PK uniqueness, referential
  integrity, invalid timestamps, FK references.
- Writes every result to `audit.dq_results`.
- Raises `DataQualityError` on a `FAIL` breach → pipeline stops (the gate).

## Step 6 — Gold (star schema + BigQuery)

```bash
make run-gold               # add --no-bq to stay local
```
Builds `dim_*` and `fct_*`, then loads BigQuery with **partitioning**
(`fct_orders` on `order_purchase_date`) and **clustering**
(`customer_id, order_status`).

## Step 7 — dbt marts

```bash
cd dbt && dbt deps && dbt build --target dev
```
staging (views) → intermediate (ephemeral) → marts (tables): the six business
metrics, each with tests and docs.

## Step 8 — CDC simulation

```bash
make generate-cdc                                   # build daily I/U/D files
make run-cdc DATE=2018-01-02                         # apply one day
```
`cdc_processor.py` merges the day's changes into Silver (upsert + soft delete)
using window-ranking on `cdc_timestamp`. The `ecommerce_cdc_daily` DAG does this
on a schedule.

## Step 9 — Observability

```bash
make dashboard              # Streamlit on :8501
```
Headline metrics: records processed, failed records, duplicates, null
violations, pipeline status, execution time — sourced from the audit tables.

## Step 10 — Orchestrate it all

```bash
docker-compose up -d        # Airflow at :8080
```
Trigger `ecommerce_end_to_end_pipeline`. It chains bronze→silver→DQ→gold→dbt
with retries (exponential backoff), SLA monitoring and email alerts.

## Step 11 — CI/CD

Push to GitHub. `ci.yml` lints + tests + import-checks DAGs; `dbt.yml` validates
models; `cd.yml` builds images and deploys DAGs to Cloud Composer.

## Step 12 — Make it yours (portfolio polish)

- Capture screenshots (see `docs/screenshots.md`).
- Connect Looker Studio/Power BI to the `rpt_*` views in `sql/04_reporting_views.sql`.
- Add a short Loom/GIF of the Airflow DAG + DQ dashboard to your README.

## Talking points for interviews

- **Why medallion?** clear separation of raw/clean/curated; reprocess any layer.
- **Why a DQ gate?** prevent bad data reaching the warehouse; auditable results.
- **Why CDC?** demonstrate incremental loads + merge semantics without Delta.
- **Why partition/cluster?** cost + performance on BigQuery scans.
- **Why dbt on top of Spark Gold?** ELT for analysts: tested, documented metrics.
