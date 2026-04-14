-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `aws-gcp-migration-490909`.`analytics`.`enriched_orders` as DBT_INTERNAL_DEST
        using (

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


        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id))

    
    when matched then update set
        `order_id` = DBT_INTERNAL_SOURCE.`order_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`amount` = DBT_INTERNAL_SOURCE.`amount`,`currency` = DBT_INTERNAL_SOURCE.`currency`,`status` = DBT_INTERNAL_SOURCE.`status`,`event_count` = DBT_INTERNAL_SOURCE.`event_count`,`event_types` = DBT_INTERNAL_SOURCE.`event_types`,`user_segment` = DBT_INTERNAL_SOURCE.`user_segment`,`lifetime_value` = DBT_INTERNAL_SOURCE.`lifetime_value`,`country` = DBT_INTERNAL_SOURCE.`country`,`process_date` = DBT_INTERNAL_SOURCE.`process_date`,`row_hash` = DBT_INTERNAL_SOURCE.`row_hash`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`
    

    when not matched then insert
        (`order_id`, `user_id`, `amount`, `currency`, `status`, `event_count`, `event_types`, `user_segment`, `lifetime_value`, `country`, `process_date`, `row_hash`, `created_at`, `updated_at`)
    values
        (`order_id`, `user_id`, `amount`, `currency`, `status`, `event_count`, `event_types`, `user_segment`, `lifetime_value`, `country`, `process_date`, `row_hash`, `created_at`, `updated_at`)


    