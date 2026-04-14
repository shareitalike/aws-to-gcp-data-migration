-- A generic staging view to select from the raw staging table
/*
    INTERVIEW NOTE: Why is the staging layer a VIEW instead of a TABLE?
    DECISION: Staging views (materialized='view') provide:
        1. No storage cost (it's just a saved query).
        2. Real-time access to the latest raw data.
        3. Column renaming and type casting without duplicating data.
    TRADEOFF: If the upstream staging table is huge (TBs), querying the view
        repeatedly can be expensive and slow. In those cases, we'd use 
        materialized='table' to create a physical "Lakehouse Silver" layer.
*/

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
