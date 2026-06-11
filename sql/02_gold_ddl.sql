-- ============================================================
--  GOLD layer DDLs — partitioned + clustered star schema.
--  fct_orders is the central fact; dims conform across marts.
-- ============================================================

-- ---------- dimensions ----------
CREATE TABLE IF NOT EXISTS `${PROJECT}.ecom_gold.dim_customers` (
  customer_id              STRING NOT NULL,
  customer_unique_id       STRING,
  customer_zip_code_prefix STRING,
  customer_city            STRING,
  customer_state           STRING
)
CLUSTER BY customer_state
OPTIONS (description = 'Customer dimension');

CREATE TABLE IF NOT EXISTS `${PROJECT}.ecom_gold.dim_sellers` (
  seller_id              STRING NOT NULL,
  seller_zip_code_prefix STRING,
  seller_city            STRING,
  seller_state           STRING
)
CLUSTER BY seller_state
OPTIONS (description = 'Seller dimension');

CREATE TABLE IF NOT EXISTS `${PROJECT}.ecom_gold.dim_products` (
  product_id            STRING NOT NULL,
  product_category_name STRING,
  category              STRING,
  product_weight_g      INT64,
  product_length_cm     INT64,
  product_height_cm     INT64,
  product_width_cm      INT64
)
CLUSTER BY category
OPTIONS (description = 'Product dimension with EN category');

CREATE TABLE IF NOT EXISTS `${PROJECT}.ecom_gold.dim_date` (
  date_day    DATE NOT NULL,
  date_key    INT64,
  year        INT64,
  quarter     INT64,
  month       INT64,
  month_name  STRING,
  day         INT64,
  day_of_week STRING,
  is_weekend  BOOL
)
OPTIONS (description = 'Calendar date dimension');

-- ---------- facts ----------
CREATE TABLE IF NOT EXISTS `${PROJECT}.ecom_gold.fct_orders` (
  order_id                       STRING NOT NULL,
  customer_id                    STRING,
  order_status                   STRING,
  order_purchase_timestamp       TIMESTAMP,
  order_purchase_date            DATE,
  order_approved_at              TIMESTAMP,
  order_delivered_customer_date  TIMESTAMP,
  order_estimated_delivery_date  TIMESTAMP,
  items_total                    NUMERIC,
  freight_total                  NUMERIC,
  order_value                    NUMERIC,
  item_count                     INT64,
  seller_count                   INT64,
  payment_total                  NUMERIC,
  max_installments               INT64,
  primary_payment_type           STRING,
  avg_review_score               NUMERIC,
  review_count                   INT64,
  delivery_days                  INT64,
  estimated_delivery_days        INT64,
  delivery_delay_days            INT64,
  is_late_delivery               BOOL
)
PARTITION BY order_purchase_date
CLUSTER BY customer_id, order_status
OPTIONS (
  description = 'Order fact (one row per order)',
  require_partition_filter = false
);

CREATE TABLE IF NOT EXISTS `${PROJECT}.ecom_gold.fct_order_items` (
  order_id            STRING NOT NULL,
  order_item_id       INT64,
  product_id          STRING,
  seller_id           STRING,
  price               NUMERIC,
  freight_value       NUMERIC,
  line_revenue        NUMERIC,
  shipping_limit_date TIMESTAMP
)
CLUSTER BY seller_id, product_id
OPTIONS (description = 'Order-item fact (one row per item)');
