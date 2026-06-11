# Architecture

## 1. System Architecture

```mermaid
flowchart TB
    subgraph SRC["Data Sources"]
        CSV["Olist CSV files"]
        CDCF["Daily CDC files\n(I/U/D)"]
    end

    subgraph ORCH["Orchestration — Apache Airflow"]
        DAG1["ecommerce_end_to_end_pipeline"]
        DAG2["ecommerce_cdc_daily"]
    end

    subgraph PROC["Processing — PySpark"]
        B["Bronze\ningest_raw"]
        S["Silver\nclean + standardize"]
        DQ["Data Quality\nframework"]
        G["Gold\nstar schema"]
        CDC["CDC processor\nmerge upsert/delete"]
    end

    subgraph WH["Warehouse — BigQuery"]
        GOLD[("ecom_gold\npartitioned + clustered")]
        AUDIT[("ecom_audit\ndq_results / run_log")]
    end

    subgraph DBT["Analytics — dbt"]
        STG["staging"]
        INT["intermediate"]
        MART["marts / metrics"]
    end

    subgraph BI["Serving"]
        DASH["Streamlit DQ Dashboard"]
        POW["Power BI / Looker / Tableau"]
    end

    CSV --> B
    CDCF --> CDC
    DAG1 --> B --> S --> DQ --> G --> GOLD
    DQ --> AUDIT
    B & S & G --> AUDIT
    DAG2 --> CDC --> S
    GOLD --> STG --> INT --> MART --> POW
    AUDIT --> DASH
    MART --> DASH
```

## 2. Medallion Architecture

```mermaid
flowchart LR
    RAW["Raw CSV / CDC"] --> BRONZE
    subgraph BRONZE["🥉 Bronze (Raw)"]
        b1["Immutable copy\nDeclared schema\nLineage columns\nParquet, partitioned by ingest_date"]
    end
    BRONZE --> SILVER
    subgraph SILVER["🥈 Silver (Clean)"]
        s1["Null handling\nType correction\nDedup (natural keys)\nStandardization\nDate formatting\nBusiness rules\nDQ gate"]
    end
    SILVER --> GOLD
    subgraph GOLD["🥇 Gold (Curated)"]
        g1["Conformed star schema\nfct_orders / fct_order_items\ndim_customers / sellers / products / date\nBigQuery partition + cluster"]
    end
    GOLD --> ANALYTICS["📈 Analytics (dbt marts)"]
```

## 3. Layer responsibilities

| Layer  | Tech     | Storage             | Idempotent? | Notes |
|--------|----------|---------------------|-------------|-------|
| Bronze | PySpark  | Parquet (`bronze/`) | Yes (overwrite) | Schema-on-write, lineage stamped |
| Silver | PySpark  | Parquet (`silver/`) | Yes / CDC-merge | Cleaning + DQ gate + CDC target |
| Gold   | PySpark  | BigQuery (`ecom_gold`) | Yes | Partitioned/clustered star schema |
| Marts  | dbt      | BigQuery (`*_marts`)| Yes | Business metrics + tests |
| Audit  | PySpark/BQ | `ecom_audit`      | Append | DQ results + run log |

## 4. Data Quality gate

The Silver→Gold transition is **gated**: `pyspark_jobs.data_quality.run_dq`
runs the declarative rules in `configs/dq_rules.yaml`. Any `FAIL`-severity check
that breaches its threshold raises `DataQualityError`, which fails the Airflow
task and stops the pipeline before bad data reaches BigQuery. All results
(pass and fail) are written to `audit.dq_results`.

## 5. CDC flow

```mermaid
sequenceDiagram
    participant F as Daily CDC file (I/U/D)
    participant C as CDC processor
    participant S as Silver snapshot
    F->>C: read batch for {{ ds }}
    C->>S: read current snapshot
    C->>C: union + window-rank by cdc_timestamp
    C->>C: keep latest per PK, drop op='D'
    C->>S: overwrite snapshot (merged)
    C->>Audit: write run-log (I/U/D counts)
```
