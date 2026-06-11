# Screenshots (for your portfolio)

Drop your captured images into `docs/images/` and they'll render below.
These are the shots recruiters look for — capture them after a successful run.

## 1. Airflow DAG — end-to-end pipeline
![Airflow end-to-end DAG](images/airflow_dag.png)
> Graph view of `ecommerce_end_to_end_pipeline` with all tasks green.

## 2. Airflow CDC DAG
![Airflow CDC DAG](images/airflow_cdc_dag.png)
> `ecommerce_cdc_daily` showing a daily incremental run.

## 3. Data Quality Dashboard
![DQ dashboard](images/dq_dashboard.png)
> Streamlit dashboard: records processed, failed, duplicates, null violations,
> pipeline status, execution time.

## 4. BigQuery Gold tables
![BigQuery gold](images/bigquery_gold.png)
> `fct_orders` partitioned by `order_purchase_date`, clustered by customer/status.

## 5. dbt docs / lineage graph
![dbt lineage](images/dbt_lineage.png)
> `dbt docs serve` DAG showing staging → intermediate → marts.

## 6. dbt test results
![dbt tests](images/dbt_tests.png)
> `dbt build` output with all tests passing.

## 7. BI dashboard (Looker Studio / Power BI)
![BI dashboard](images/bi_dashboard.png)
> Revenue trend, top categories, delivery scorecard from the `rpt_*` views.

---

### How to capture

| Shot | Command |
|------|---------|
| Airflow | `docker-compose up -d` → http://localhost:8080 |
| DQ dashboard | `make dashboard` → http://localhost:8501 |
| dbt docs | `cd dbt && dbt docs generate && dbt docs serve` |
| BigQuery | GCP console → BigQuery → `ecom_gold` |
| BI | connect Looker Studio to `rpt_revenue_trend` etc. |
