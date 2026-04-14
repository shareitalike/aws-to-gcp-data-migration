/*
    Pipeline Quality & Observability Dashboard
    This model aggregates metrics from staging and production to 
    provide a "Health Report" for every run.
*/

{{ config(
    materialized='table',
    schema='analytics'
) }}

WITH staging_stats AS (
    SELECT 
        process_date,
        COUNT(*) as staging_rows,
        COUNT(DISTINCT order_id) as unique_staging_orders,
        SUM(amount) as total_staging_revenue,
        AVG(amount) as avg_staging_amount,
        COUNTIF(order_id IS NULL) as null_order_ids
    FROM {{ ref('stg_enriched_orders') }}
    GROUP BY 1
),

production_stats AS (
    SELECT 
        process_date,
        COUNT(*) as production_rows,
        SUM(amount) as total_production_revenue,
        MAX(updated_at) as last_updated_at
    FROM {{ ref('enriched_orders') }}
    GROUP BY 1
)

SELECT 
    s.process_date,
    s.staging_rows,
    p.production_rows,
    
    -- Completeness Check
    CASE 
        WHEN s.staging_rows = p.production_rows THEN '✅ MATCH'
        WHEN s.staging_rows > p.production_rows THEN '❌ DATA LOSS'
        ELSE '⚠️ OVER-COUNT'
    END as completeness_status,

    -- Uniqueness Check
    CASE 
        WHEN s.staging_rows = s.unique_staging_orders THEN '✅ UNIQUE'
        ELSE '❌ DUPLICATES FOUND'
    END as uniqueness_status,

    -- Financial Integrity
    s.total_staging_revenue,
    p.total_production_revenue,
    (s.total_staging_revenue - p.total_production_revenue) as revenue_variance,

    -- Quality Metrics
    s.avg_staging_amount as price_point_telemetry,
    s.null_order_ids as primary_key_violations,

    -- Freshness (SLA)
    p.last_updated_at,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), p.last_updated_at, MINUTE) as minutes_since_last_run

FROM staging_stats s
LEFT JOIN production_stats p ON s.process_date = p.process_date
ORDER BY s.process_date DESC
