# Setup Instructions

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.11 | for local jobs |
| Java (JDK) | 11 or 17 | required by PySpark |
| Docker + Compose | latest | for the containerized stack |
| Kaggle account | — | to download the dataset |
| GCP project | — | (optional) for BigQuery/dbt; pipeline runs locally without it |

## Option A — Local (no Docker)

```bash
# 1. Clone & enter
git clone <your-repo-url> && cd "E-Commerce PySpark Pipeline"

# 2. Create a virtualenv
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install deps
make install                      # pip install -r requirements.txt

# 4. Configure
cp .env.example .env              # edit GCP + paths (leave GCP blank to run local-only)
set -a && source .env && set +a   # export the vars (bash)

# 5. Get the data
make download-data                # needs ~/.kaggle/kaggle.json

# 6. Generate simulated daily CDC files
make generate-cdc

# 7. Run the medallion pipeline
make run-bronze
make run-silver
make run-dq
make run-gold                     # add --no-bq to skip BigQuery (run-gold target uses BQ)

# 8. Launch the DQ dashboard
make dashboard                    # http://localhost:8501
```

> **Local-only mode:** if `GCP_PROJECT_ID` is left as the placeholder, the
> BigQuery loader logs a skip and the pipeline still produces local Parquet
> under `data/gold` and `data/audit`, so the dashboard works end-to-end.

### Windows / PowerShell

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts\generate_cdc_files.py --days 7
python -m pyspark_jobs.bronze.ingest_raw --config configs\pipeline_config.yaml
python -m pyspark_jobs.silver.run_silver --config configs\pipeline_config.yaml
python -m pyspark_jobs.data_quality.run_dq --config configs\pipeline_config.yaml --layer silver
python -m pyspark_jobs.gold.run_gold --config configs\pipeline_config.yaml --no-bq
streamlit run monitoring\dashboard\dq_dashboard.py
```

## Option B — Docker

```bash
cp .env.example .env              # fill in values
mkdir -p secrets && cp /path/to/gcp-key.json secrets/gcp-key.json   # optional

docker-compose up -d              # starts postgres + airflow web/scheduler
open http://localhost:8080        # airflow / airflow

# run a one-off Spark job
docker-compose --profile tools run --rm spark \
  python -m pyspark_jobs.bronze.ingest_raw --config configs/pipeline_config.yaml

# run dbt
docker-compose --profile tools run --rm dbt dbt build --target prod

# dashboard
docker-compose --profile dashboard up dashboard   # http://localhost:8501
```

## BigQuery bootstrap (optional)

```bash
# substitute project/region then create datasets + DDLs
for f in sql/01_create_datasets.sql sql/02_gold_ddl.sql sql/03_audit_ddl.sql sql/04_reporting_views.sql; do
  sed "s/\${PROJECT}/$GCP_PROJECT_ID/g; s/\${REGION}/$GCP_REGION/g" "$f" \
    | bq query --use_legacy_sql=false
done
```

## Running tests

```bash
make test          # pytest with coverage
make lint          # ruff
make format        # black + ruff --fix
```
