{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'date'],
    incremental_strategy='merge'
) }}

WITH daily_payments AS (
    SELECT 
        o.customer_id,
        DATE(o.order_purchase_timestamp) as date,
        COUNT(DISTINCT o.order_id) as orders_count,
        SUM(p.payment_value) as total_payments,
        AVG(p.payment_value) as avg_payment_value
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    
    {% if is_incremental() %}
      -- LOOKBACK STRATEGY: Reprocess last 3 days to catch late payments
      AND DATE(o.order_purchase_timestamp) >= (
        SELECT MAX(date) - INTERVAL '3 days'
        FROM {{ this }}
      )
    {% endif %}
    
    GROUP BY 1, 2
)

SELECT 
    customer_id,
    date,
    orders_count,
    total_payments,
    avg_payment_value,
    CURRENT_TIMESTAMP as processed_at
FROM daily_payments