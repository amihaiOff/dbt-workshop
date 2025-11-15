{{ config(materialized='table') }}

WITH future_orders AS (
    SELECT 
        customer_id,
        DATE(order_purchase_timestamp) as order_date
    FROM {{ ref('stg_orders') }}
    WHERE order_status NOT IN ('canceled', 'unavailable')
)

SELECT 
    f.customer_id,
    f.feature_date,
    CASE 
        WHEN MIN(fo.order_date) IS NULL THEN 1  -- No future orders = churned
        WHEN (MIN(fo.order_date) - f.feature_date) > INTERVAL '90 days' THEN 1  -- Gap > 90 days = churned
        ELSE 0  -- Active
    END as will_churn_90d
FROM {{ ref('int_customer_daily_features') }} f
LEFT JOIN future_orders fo
    ON f.customer_id = fo.customer_id
    AND fo.order_date > f.feature_date
    AND fo.order_date <= f.feature_date + INTERVAL '90 days'
GROUP BY 1, 2