-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  Stage 5a: BigQuery Table DDL
--  Run this in BigQuery Console or via bq CLI
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- NOTE: If you used Terraform, tables are already created.
-- This file is for manual setup or reference.

-- ── Staging Table (no partitioning — overwritten each load) ──
CREATE TABLE IF NOT EXISTS `staging.enriched_orders_staging` (
  order_id        STRING    NOT NULL,
  user_id         STRING,
  amount          FLOAT64   NOT NULL,
  currency        STRING    NOT NULL,
  status          STRING,
  event_count     INT64,
  event_types     STRING,
  user_segment    STRING,
  lifetime_value  FLOAT64,
  country         STRING,
  process_date    DATE      NOT NULL,
  row_hash        STRING,
  created_at      TIMESTAMP,
  updated_at      TIMESTAMP
);


-- ── Production Table (partitioned + clustered) ──
CREATE TABLE IF NOT EXISTS `analytics.enriched_orders` (
  order_id        STRING    NOT NULL,
  user_id         STRING,
  amount          FLOAT64   NOT NULL,
  currency        STRING    NOT NULL,
  status          STRING,
  event_count     INT64,
  event_types     STRING,
  user_segment    STRING,
  lifetime_value  FLOAT64,
  country         STRING,
  process_date    DATE      NOT NULL,
  row_hash        STRING,
  created_at      TIMESTAMP,
  updated_at      TIMESTAMP
)
PARTITION BY process_date
CLUSTER BY user_id, user_segment
OPTIONS (
  description = 'Enriched orders — partitioned by date, clustered by user',
  require_partition_filter = true,
  labels = [('pipeline', 'aws_gcp_migration')]
);


-- ── Safe View for Dashboards (bakes in partition filter) ──
CREATE OR REPLACE VIEW `analytics.enriched_orders_last_90d` AS
SELECT *
FROM `analytics.enriched_orders`
WHERE process_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
