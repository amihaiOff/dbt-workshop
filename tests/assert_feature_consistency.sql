-- Test that daily features don't have impossible values
-- Features should be consistent with underlying order data

WITH feature_validation AS (
    SELECT 
        f.customer_id,
        f.date,
        f.orders_14d,
        f.payment_14d,
        
        -- Count actual orders in last 14 days from source
        COUNT(DISTINCT o.order_id) as actual_orders_14d,
        COALESCE(SUM(p.payment_value), 0) as actual_payment_14d
        
    FROM {{ ref('int_customer_daily_features') }} f
    LEFT JOIN {{ ref('stg_orders') }} o
        ON f.customer_id = o.customer_id
        AND DATE(o.order_purchase_timestamp) BETWEEN 
            f.date - INTERVAL '14 days' AND f.date
        AND o.order_status NOT IN ('canceled', 'unavailable')
    LEFT JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE f.date >= CURRENT_DATE - INTERVAL '7 days'  -- Only check recent features
    GROUP BY 1, 2, 3, 4
)

-- Return mismatches (errors)
SELECT 
    customer_id,
    date,
    orders_14d as feature_orders,
    actual_orders_14d,
    ABS(orders_14d - actual_orders_14d) as order_difference,
    'Feature vs actual order count mismatch' as error_type
FROM feature_validation
WHERE ABS(orders_14d - actual_orders_14d) > 2  -- Allow small tolerance