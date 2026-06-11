-- ============================================================
--  Reporting views for BI tools (Power BI / Looker Studio / Tableau).
--  These sit on top of the dbt marts and present stable, friendly
--  column names for self-service dashboards.
-- ============================================================

-- Executive KPI snapshot
CREATE OR REPLACE VIEW `${PROJECT}.ecom_gold.rpt_kpi_overview` AS
SELECT
  SUM(revenue)                       AS total_revenue,
  SUM(orders)                        AS total_orders,
  ROUND(SUM(revenue) / NULLIF(SUM(orders), 0), 2) AS avg_order_value,
  SUM(customers)                     AS total_customers
FROM `${PROJECT}.ecom_gold.marts_mart_revenue_monthly`;

-- Revenue trend (Power BI line chart source)
CREATE OR REPLACE VIEW `${PROJECT}.ecom_gold.rpt_revenue_trend` AS
SELECT order_month, revenue, orders, avg_order_value
FROM `${PROJECT}.ecom_gold.marts_mart_revenue_monthly`
ORDER BY order_month;

-- Category leaderboard
CREATE OR REPLACE VIEW `${PROJECT}.ecom_gold.rpt_category_leaderboard` AS
SELECT category, revenue, units_sold, revenue_rank
FROM `${PROJECT}.ecom_gold.marts_mart_top_categories`
WHERE revenue_rank <= 20;

-- Delivery scorecard
CREATE OR REPLACE VIEW `${PROJECT}.ecom_gold.rpt_delivery_scorecard` AS
SELECT customer_state,
       SUM(delivered_orders) AS delivered_orders,
       ROUND(AVG(on_time_pct), 1) AS on_time_pct,
       ROUND(AVG(avg_delivery_days), 1) AS avg_delivery_days
FROM `${PROJECT}.ecom_gold.marts_mart_delivery_performance`
GROUP BY customer_state;

-- Data Quality dashboard feed (failed/duplicate/null breakdown by run)
CREATE OR REPLACE VIEW `${PROJECT}.ecom_audit.rpt_dq_dashboard` AS
SELECT
  run_id,
  DATE(executed_at)                                       AS run_date,
  COUNT(*)                                                AS checks_total,
  COUNTIF(status = 'PASS')                                AS checks_passed,
  COUNTIF(status = 'WARN')                                AS checks_warned,
  COUNTIF(status = 'FAIL')                                AS checks_failed,
  SUM(records_scanned)                                    AS records_processed,
  SUM(records_failed)                                     AS records_failed,
  SUM(IF(check_type = 'duplicates' OR check_type = 'unique', records_failed, 0)) AS duplicate_records,
  SUM(IF(check_type = 'not_null', records_failed, 0))     AS null_violations,
  CASE WHEN COUNTIF(status = 'FAIL') > 0 THEN 'FAILED' ELSE 'PASSED' END AS pipeline_status
FROM `${PROJECT}.ecom_audit.dq_results`
GROUP BY run_id, run_date;
