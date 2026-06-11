-- ============================================================
--  AUDIT layer DDLs — DQ results + pipeline run log.
--  These back the Data Quality Dashboard and lineage tracking.
-- ============================================================

CREATE TABLE IF NOT EXISTS `${PROJECT}.ecom_audit.dq_results` (
  run_id          STRING NOT NULL,
  table_name      STRING,
  check_type      STRING,
  column          STRING,
  records_scanned INT64,
  records_failed  INT64,
  threshold       FLOAT64,
  severity        STRING,
  status          STRING,            -- PASS | WARN | FAIL
  detail          STRING,
  executed_at     TIMESTAMP
)
PARTITION BY DATE(executed_at)
CLUSTER BY table_name, status
OPTIONS (description = 'One row per data quality check executed');

CREATE TABLE IF NOT EXISTS `${PROJECT}.ecom_audit.pipeline_run_log` (
  run_id       STRING NOT NULL,
  stage        STRING,               -- bronze | silver | dq | gold | cdc
  table_name   STRING,
  status       STRING,               -- RUNNING | SUCCESS | FAILED
  rows_in      INT64,
  rows_out     INT64,
  duration_sec FLOAT64,
  started_at   TIMESTAMP,
  ended_at     TIMESTAMP,
  message      STRING
)
PARTITION BY DATE(started_at)
CLUSTER BY stage, status
OPTIONS (description = 'Row-count tracking + execution time per stage/table');

-- Convenience view: latest run summary used by the dashboard.
CREATE OR REPLACE VIEW `${PROJECT}.ecom_audit.v_latest_run_summary` AS
WITH latest AS (
  SELECT MAX(run_id) AS run_id FROM `${PROJECT}.ecom_audit.pipeline_run_log`
)
SELECT
  l.stage,
  COUNT(*)                                  AS tables,
  SUM(l.rows_out)                           AS total_rows_out,
  ROUND(SUM(l.duration_sec), 1)             AS total_seconds,
  COUNTIF(l.status = 'FAILED')              AS failed_stages
FROM `${PROJECT}.ecom_audit.pipeline_run_log` l
JOIN latest USING (run_id)
GROUP BY l.stage;
