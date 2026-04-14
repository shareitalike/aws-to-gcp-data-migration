

  create or replace view `aws-gcp-migration-490909`.`analytics`.`stg_enriched_orders`
  OPTIONS()
  as -- A generic staging view to select from the raw staging table
SELECT
    order_id,
    user_id,
    amount,
    currency,
    status,
    event_count,
    event_types,
    user_segment,
    lifetime_value,
    country,
    row_hash,
    SAFE_CAST(process_date AS DATE) as process_date,
    created_at,
    updated_at
FROM `aws-gcp-migration-490909`.`staging`.`enriched_orders_staging`;

