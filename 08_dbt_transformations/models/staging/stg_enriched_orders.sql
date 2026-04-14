-- A generic staging view to select from the raw staging table
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
FROM {{ source('staging_dataset', 'enriched_orders_staging') }}
