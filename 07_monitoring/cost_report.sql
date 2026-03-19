-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  Stage 7b: BigQuery Cost Analysis
--  Run in BQ Console to see how much you're spending
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- ── Query 1: Cost by table (last 24 hours) ──
SELECT
  referenced_tables.table_id,
  COUNT(*) AS query_count,
  ROUND(SUM(total_bytes_billed) / POW(1024, 3), 2) AS gb_billed,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4) * 6.25, 2) AS estimated_cost_usd
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
  UNNEST(referenced_tables) AS referenced_tables
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND job_type = 'QUERY'
  AND state = 'DONE'
GROUP BY 1
ORDER BY gb_billed DESC;


-- ── Query 2: Slot usage over time (last 24 hours) ──
SELECT
  TIMESTAMP_TRUNC(creation_time, HOUR) AS hour,
  COUNT(*) AS jobs,
  ROUND(SUM(total_slot_ms) / 1000 / 3600, 1) AS slot_hours,
  ROUND(AVG(total_bytes_billed) / POW(1024, 3), 2) AS avg_gb_per_query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND job_type = 'QUERY'
  AND state = 'DONE'
GROUP BY 1
ORDER BY 1;


-- ── Query 3: Storage cost by dataset ──
SELECT
  table_schema AS dataset,
  COUNT(*) AS table_count,
  ROUND(SUM(total_logical_bytes) / POW(1024, 3), 2) AS total_gb,
  ROUND(SUM(total_logical_bytes) / POW(1024, 4) * 0.02, 4) AS storage_cost_usd_month
FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
GROUP BY 1
ORDER BY total_gb DESC;


-- ── Query 4: Partition usage (are we hitting partition filters?) ──
SELECT
  job_id,
  query,
  total_bytes_billed,
  ROUND(total_bytes_billed / POW(1024, 3), 2) AS gb_billed,
  CASE
    WHEN total_bytes_billed > 10737418240 -- > 10 GB
    THEN '⚠️ LARGE SCAN — check partition filter'
    ELSE '✅ OK'
  END AS cost_alert
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND job_type = 'QUERY'
  AND state = 'DONE'
ORDER BY total_bytes_billed DESC
LIMIT 20;
