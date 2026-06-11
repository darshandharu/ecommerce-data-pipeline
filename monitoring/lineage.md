# Data Lineage

Operational lineage is captured automatically in `audit.pipeline_run_log`
(one row per run/stage/table). The logical lineage of each Gold/mart table is
documented here and in `dbt docs`.

```mermaid
flowchart LR
    subgraph raw["Raw CSV"]
        r_orders[olist_orders]
        r_items[olist_order_items]
        r_pay[olist_order_payments]
        r_rev[olist_order_reviews]
        r_cust[olist_customers]
        r_sell[olist_sellers]
        r_prod[olist_products]
        r_cat[category_translation]
    end

    r_orders --> b_orders[bronze.orders] --> s_orders[silver.orders]
    r_items --> b_items[bronze.order_items] --> s_items[silver.order_items]
    r_pay --> s_pay[silver.order_payments]
    r_rev --> s_rev[silver.order_reviews]
    r_cust --> s_cust[silver.customers]
    r_sell --> s_sell[silver.sellers]
    r_prod --> s_prod[silver.products]
    r_cat --> s_cat[silver.category_translation]

    s_orders & s_items & s_pay & s_rev --> g_fo[gold.fct_orders]
    s_items --> g_fi[gold.fct_order_items]
    s_cust --> g_dc[gold.dim_customers]
    s_sell --> g_ds[gold.dim_sellers]
    s_prod & s_cat --> g_dp[gold.dim_products]
    s_orders --> g_dd[gold.dim_date]

    g_fo --> m_rev[mart_revenue_monthly]
    g_fo --> m_aov[mart_avg_order_value]
    g_fo --> m_del[mart_delivery_performance]
    g_fo --> m_ret[mart_customer_retention]
    g_fi & g_dp --> m_cat[mart_top_categories]
    g_fi & g_ds --> m_sell[mart_seller_performance]
```

## Column-level notes

- `fct_orders.order_value` ← Σ(`order_items.price + freight_value`) per order.
- `fct_orders.avg_review_score` ← AVG(`order_reviews.review_score`) per order.
- `dim_products.category` ← `products.product_category_name` translated via
  `category_translation` (EN), falling back to the PT name.
- `mart_customer_retention.cohort_month` ← MIN(`order_month`) per
  `customer_unique_id`.
