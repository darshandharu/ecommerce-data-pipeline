-- ============================================================
--  Create the BigQuery datasets for each medallion layer.
--  Replace ${PROJECT} / ${REGION} before running, or use
--  `bq query --use_legacy_sql=false < this_file`.
-- ============================================================
CREATE SCHEMA IF NOT EXISTS `${PROJECT}.ecom_bronze`
  OPTIONS (location = '${REGION}', description = 'Raw ingested layer');

CREATE SCHEMA IF NOT EXISTS `${PROJECT}.ecom_silver`
  OPTIONS (location = '${REGION}', description = 'Cleaned/conformed layer');

CREATE SCHEMA IF NOT EXISTS `${PROJECT}.ecom_gold`
  OPTIONS (location = '${REGION}', description = 'Star schema / analytics layer');

CREATE SCHEMA IF NOT EXISTS `${PROJECT}.ecom_audit`
  OPTIONS (location = '${REGION}', description = 'Audit, DQ results, run logs');
