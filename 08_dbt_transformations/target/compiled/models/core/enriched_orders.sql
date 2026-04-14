

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
FROM `aws-gcp-migration-490909`.`analytics`.`stg_enriched_orders`



    -- This tells dbt to only grab new or updated records comparing process_date
    -- In a real scenario, we might use a watermark or just rely on the unique_key MERGE
    WHERE process_date >= (SELECT max(process_date) FROM `aws-gcp-migration-490909`.`analytics`.`enriched_orders` WHERE process_date >= '2000-01-01')

