# dbt — Analytics Layer

Transforms the BigQuery **Gold** star schema into business-ready marts.

## Layers

```
sources (gold.*)  ->  staging (views)  ->  intermediate (ephemeral)  ->  marts (tables)
```

| Layer        | Materialization | Purpose                                  |
|--------------|-----------------|------------------------------------------|
| staging      | view            | rename/recast/clean 1:1 with sources     |
| intermediate | ephemeral       | joins & enrichment reused across marts   |
| marts        | table           | business metrics consumed by BI tools    |

## Marts (business metrics)

| Model | Metric |
|-------|--------|
| `mart_revenue_monthly`     | Revenue by month |
| `mart_top_categories`      | Top-selling categories |
| `mart_customer_retention`  | Customer retention (cohorts) |
| `mart_avg_order_value`     | Average order value |
| `mart_delivery_performance`| Delivery performance |
| `mart_seller_performance`  | Seller performance |

## Run

```bash
export GCP_PROJECT_ID=... GOOGLE_APPLICATION_CREDENTIALS=/path/key.json
dbt deps          # install dbt_utils
dbt build         # run + test everything
dbt docs generate && dbt docs serve
```

## Tests

- Generic: `not_null`, `unique`, `relationships`, `accepted_values`,
  `dbt_utils.accepted_range`, `dbt_utils.unique_combination_of_columns`
- Singular: [`tests/assert_revenue_non_negative.sql`](tests/assert_revenue_non_negative.sql)
- Source freshness configured on `gold.fct_orders`.
