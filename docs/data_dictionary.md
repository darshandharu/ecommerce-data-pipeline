# Data Dictionary

Field-level reference for the Gold star schema, audit tables and key marts.

## Gold — `fct_orders` (grain: one row per order)

| Column | Type | Description |
|--------|------|-------------|
| order_id | STRING (PK) | Unique order identifier |
| customer_id | STRING (FK→dim_customers) | Customer who placed the order |
| order_status | STRING | delivered, shipped, canceled, … |
| order_purchase_timestamp | TIMESTAMP | When the order was placed |
| order_purchase_date | DATE | Partition key (date of purchase) |
| order_approved_at | TIMESTAMP | Payment approval time |
| order_delivered_customer_date | TIMESTAMP | Actual delivery to customer |
| order_estimated_delivery_date | TIMESTAMP | Promised delivery date |
| items_total | NUMERIC | Sum of item prices |
| freight_total | NUMERIC | Sum of freight values |
| order_value | NUMERIC | items_total + freight_total |
| item_count | INT64 | Number of items in the order |
| seller_count | INT64 | Distinct sellers in the order |
| payment_total | NUMERIC | Sum of payments |
| max_installments | INT64 | Max payment installments |
| primary_payment_type | STRING | credit_card, boleto, voucher, … |
| avg_review_score | NUMERIC | Average review score (1–5) |
| review_count | INT64 | Number of reviews |
| delivery_days | INT64 | Purchase → delivery (days) |
| estimated_delivery_days | INT64 | Purchase → estimate (days) |
| delivery_delay_days | INT64 | Delivery − estimate (days; +late) |
| is_late_delivery | BOOL | True if delivered after estimate |

## Gold — `fct_order_items` (grain: one row per item)

| Column | Type | Description |
|--------|------|-------------|
| order_id | STRING (FK) | Parent order |
| order_item_id | INT64 | Item sequence within order |
| product_id | STRING (FK→dim_products) | Product sold |
| seller_id | STRING (FK→dim_sellers) | Seller |
| price | NUMERIC | Item price |
| freight_value | NUMERIC | Item freight |
| line_revenue | NUMERIC | price + freight_value |
| shipping_limit_date | TIMESTAMP | Seller shipping deadline |

## Gold — dimensions

### `dim_customers`
| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING (PK) | Per-order customer key |
| customer_unique_id | STRING | Stable customer identity (retention) |
| customer_zip_code_prefix | STRING | Zip prefix |
| customer_city | STRING | City (lower-cased) |
| customer_state | STRING | 2-letter UF (clustered) |

### `dim_sellers`
| Column | Type | Description |
|--------|------|-------------|
| seller_id | STRING (PK) | Seller key |
| seller_zip_code_prefix | STRING | Zip prefix |
| seller_city | STRING | City |
| seller_state | STRING | 2-letter UF |

### `dim_products`
| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING (PK) | Product key |
| product_category_name | STRING | Original PT category |
| category | STRING | English category (clustered) |
| product_weight_g / *_cm | INT64 | Physical dimensions |

### `dim_date`
| Column | Type | Description |
|--------|------|-------------|
| date_day | DATE (PK) | Calendar day |
| date_key | INT64 | yyyymmdd surrogate |
| year/quarter/month/day | INT64 | Date parts |
| month_name / day_of_week | STRING | Labels |
| is_weekend | BOOL | Weekend flag |

## Audit — `dq_results`

| Column | Type | Description |
|--------|------|-------------|
| run_id | STRING | Pipeline run identifier |
| table_name | STRING | Table checked |
| check_type | STRING | not_null, unique, referential_integrity, … |
| column | STRING | Column(s) checked |
| records_scanned | INT64 | Rows evaluated |
| records_failed | INT64 | Rows failing the check |
| threshold | FLOAT64 | Tolerated failure fraction |
| severity | STRING | FAIL or WARN |
| status | STRING | PASS / WARN / FAIL |
| detail | STRING | Human-readable summary |
| executed_at | TIMESTAMP | When the check ran |

## Audit — `pipeline_run_log`

| Column | Type | Description |
|--------|------|-------------|
| run_id | STRING | Pipeline run identifier |
| stage | STRING | bronze / silver / dq / gold / cdc |
| table_name | STRING | Table processed |
| status | STRING | RUNNING / SUCCESS / FAILED |
| rows_in / rows_out | INT64 | Row-count tracking |
| duration_sec | FLOAT64 | Execution time |
| started_at / ended_at | TIMESTAMP | Stage timing |
| message | STRING | Error or note (e.g. CDC I/U/D counts) |

## Marts (selected)

| Model | Grain | Key measures |
|-------|-------|--------------|
| mart_revenue_monthly | month | revenue, orders, avg_order_value |
| mart_top_categories | category | revenue, units_sold, revenue_rank |
| mart_customer_retention | cohort_month × month_offset | active_customers, revenue |
| mart_avg_order_value | state × payment_type | avg_order_value, median_order_value |
| mart_delivery_performance | month × state | on_time_pct, avg_delivery_days |
| mart_seller_performance | seller | revenue, avg_review_score, late_delivery_pct |
