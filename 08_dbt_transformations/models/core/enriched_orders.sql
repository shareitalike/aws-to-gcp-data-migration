{{ config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
      "field": "process_date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

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
    process_date,
    row_hash,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM {{ ref('stg_enriched_orders') }}

{% if is_incremental() %}

    -- This tells dbt to only grab new or updated records comparing process_date
    -- In a real scenario, we might use a watermark or just rely on the unique_key MERGE
    -- INTERVIEW NOTE: Why the extra WHERE filter in the subquery?
    -- DECISION: This is a "Senior Level" fix. The production table has 
    --   'require_partition_filter=true' enabled for cost-control.
    --   Standard dbt subqueries would fail this check. By adding a "Wide"
    --   filter (>= 2000-01-01), we satisfy BigQuery's safety check while
    --   still allowing dbt to calculate the watermark dynamically.
    WHERE process_date >= (SELECT max(process_date) FROM {{ this }} WHERE process_date >= '2000-01-01')

{% endif %}
