{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp as order_purchase_timestamp,
    DATE(order_purchase_timestamp) as order_date,
    
    -- INGESTION TIME STRATEGY: Track when we received this data
    CURRENT_TIMESTAMP as ingestion_time,
    
    -- Simulate late arrival: some orders arrive 1-3 days after order_date
    CASE 
        WHEN order_id IN (
            SELECT order_id FROM {{ source('olist_data', 'olist_orders_dataset') }}
            WHERE MOD(ABS(HASHTEXT(order_id)), 10) = 0  -- 10% of orders
        ) THEN order_purchase_timestamp + INTERVAL '2 days'
        ELSE order_purchase_timestamp + INTERVAL '1 hour'
    END as simulated_arrival_time

FROM {{ source('olist_data', 'olist_orders_dataset') }}

WHERE order_status != 'unavailable'
  AND order_purchase_timestamp IS NOT NULL
  
  {% if is_incremental() %}
    -- Only process recently arrived data
    AND order_purchase_timestamp > (
        SELECT MAX(order_purchase_timestamp) - INTERVAL '1 day'
        FROM {{ this }}
    )
  {% endif %}