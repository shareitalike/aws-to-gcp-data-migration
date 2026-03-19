-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  Stage 5c: Idempotent MERGE — Staging → Production
--
--  Run in BigQuery Console after load_staging.py
--  Replace ${RUN_DATE} with actual date, e.g., '2026-03-19'
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- ── Pre-merge Quality Gate ──
DECLARE staging_count INT64;
DECLARE staging_nulls INT64;

SET staging_count = (
  SELECT COUNT(*)
  FROM `staging.enriched_orders_staging`
);

SET staging_nulls = (
  SELECT COUNT(*)
  FROM `staging.enriched_orders_staging`
  WHERE order_id IS NULL
);

-- Abort if staging is empty
IF staging_count = 0 THEN
  RAISE USING MESSAGE = 'ABORT: Staging table is empty!';
END IF;

-- Abort if null primary keys exist
IF staging_nulls > 0 THEN
  RAISE USING MESSAGE =
    FORMAT('ABORT: Found %d null order_ids in staging', staging_nulls);
END IF;


-- ── MERGE: Upsert into production ──
-- KEY DESIGN DECISIONS:
--   1. Match on order_id + process_date (composite natural key)
--   2. Partition filter on target (process_date) limits scan cost
--   3. WHEN MATCHED updates all fields + updated_at timestamp
--   4. WHEN NOT MATCHED inserts new rows
--   5. Idempotent: running twice produces same result

MERGE `analytics.enriched_orders` AS target
USING `staging.enriched_orders_staging` AS source
ON target.order_id = source.order_id
   AND target.process_date = source.process_date
WHEN MATCHED THEN
  UPDATE SET
    target.user_id         = source.user_id,
    target.amount          = source.amount,
    target.currency        = source.currency,
    target.status          = source.status,
    target.event_count     = source.event_count,
    target.event_types     = source.event_types,
    target.user_segment    = source.user_segment,
    target.lifetime_value  = source.lifetime_value,
    target.country         = source.country,
    target.row_hash        = source.row_hash,
    target.updated_at      = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    order_id, user_id, amount, currency, status,
    event_count, event_types, user_segment,
    lifetime_value, country, process_date,
    row_hash, created_at, updated_at
  )
  VALUES (
    source.order_id, source.user_id, source.amount,
    source.currency, source.status,
    source.event_count, source.event_types, source.user_segment,
    source.lifetime_value, source.country, source.process_date,
    source.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );


-- ── Post-merge Verification ──
DECLARE target_count INT64;

SET target_count = (
  SELECT COUNT(*)
  FROM `analytics.enriched_orders`
  WHERE process_date = '${RUN_DATE}'
);

SELECT
  staging_count AS staging_rows,
  target_count  AS production_rows,
  CASE
    WHEN target_count >= staging_count THEN '✅ MERGE SUCCESSFUL'
    ELSE '❌ ROW COUNT MISMATCH — INVESTIGATE'
  END AS status;
