{{ config(materialized='table') }}

WITH first_orders AS (
    SELECT 
        o.customer_id,
        MIN(o.order_purchase_timestamp) as first_order_timestamp,
        DATE(MIN(o.order_purchase_timestamp)) as landing_date
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY o.customer_id
)

SELECT
    customer_id,
    first_order_timestamp,
    landing_date,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'landing_date']) }} as customer_unique_key
FROM first_orders