# Deployment Guide

This guide covers deploying the pipeline to **Google Cloud** using Cloud
Composer (managed Airflow), Dataproc (managed Spark), BigQuery and dbt.

## 1. GCP footprint

| Component | GCP service | Purpose |
|-----------|-------------|---------|
| Orchestration | Cloud Composer 2 | runs the Airflow DAGs |
| Spark | Dataproc (or Serverless Spark) | runs the PySpark jobs |
| Warehouse | BigQuery | bronze/silver/gold/audit datasets |
| Object store | Cloud Storage | staging bucket + DAG bucket + data lake |
| Secrets | Secret Manager | service-account keys, SMTP creds |
| CI/CD | GitHub Actions | build images, deploy DAGs |

## 2. One-time provisioning

```bash
export PROJECT=my-gcp-project REGION=US
gcloud config set project $PROJECT

# Buckets
gsutil mb -l $REGION gs://${PROJECT}-ecom-staging
gsutil mb -l $REGION gs://${PROJECT}-ecom-datalake

# BigQuery datasets + DDLs
for f in sql/0*.sql; do
  sed "s/\${PROJECT}/$PROJECT/g; s/\${REGION}/$REGION/g" "$f" | bq query --use_legacy_sql=false
done

# Service account
gcloud iam service-accounts create ecom-pipeline
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:ecom-pipeline@${PROJECT}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:ecom-pipeline@${PROJECT}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:ecom-pipeline@${PROJECT}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

## 3. Cloud Composer

```bash
gcloud composer environments create ecom-composer \
  --location $REGION --image-version composer-2-airflow-2.9.3

# install extra PyPI deps
gcloud composer environments update ecom-composer --location $REGION \
  --update-pypi-packages-from-file airflow/requirements.txt

# set Airflow variables / connections
gcloud composer environments run ecom-composer --location $REGION variables -- \
  set alert_email data-alerts@example.com
```

DAG + code sync is automated by [`.github/workflows/cd.yml`](../.github/workflows/cd.yml)
(`gsutil rsync` to the Composer DAG bucket). Set repo secret
`COMPOSER_DAG_BUCKET` to enable it.

## 4. Spark on Dataproc (Serverless)

The DAGs currently use `python -m ...` (local Spark) for portability. For
production scale, swap the `BashOperator`s for `DataprocCreateBatchOperator`:

```python
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

run_silver = DataprocCreateBatchOperator(
    task_id="transform_silver",
    project_id=PROJECT, region=REGION,
    batch={
        "pyspark_batch": {
            "main_python_file_uri": "gs://.../pyspark_jobs/silver/run_silver.py",
            "args": ["--config", "gs://.../configs/pipeline_config.yaml"],
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar"],
        }
    },
)
```

## 5. dbt in production

- Store the BigQuery key in Secret Manager; mount it via
  `GOOGLE_APPLICATION_CREDENTIALS`.
- `dbt build --target prod` runs in the `dbt_run`/`dbt_test` DAG tasks.
- Schedule `dbt source freshness` to alert on stale Gold loads.

## 6. CI/CD pipeline

| Workflow | Trigger | Action |
|----------|---------|--------|
| `ci.yml` | push/PR | ruff + black + pytest + DAG import-check |
| `dbt.yml`| PR touching `dbt/**` | `dbt deps/compile/build` on a CI dataset |
| `cd.yml` | push to `main` / tag | build & push images, rsync DAGs to Composer |

### Required GitHub secrets/vars

| Name | Type | Used by |
|------|------|---------|
| `GCP_PROJECT_ID` | secret | dbt, cd |
| `GCP_REGION` | var | dbt, cd |
| `GCP_SA_KEY` | secret (JSON) | dbt, cd |
| `COMPOSER_DAG_BUCKET` | secret | cd (deploy DAGs) |

## 7. Rollback

- DAGs are versioned in git — revert the commit and let `cd.yml` resync.
- Gold tables are full-refresh (`WRITE_TRUNCATE`); re-run `run_gold` to restore.
- dbt marts are `table` materializations; `dbt run --select <model>` rebuilds.
