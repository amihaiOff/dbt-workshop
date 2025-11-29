-- Test that daily features don't have impossible values
-- Features should be consistent with underlying order data

WITH feature_validation AS (
    SELECT 
        f.customer_id,
        f.date,
        f.orders_30d,
        f.payment_30d,
        
        -- Count actual orders in last 30 days from source
        COUNT(DISTINCT o.order_id) as actual_orders_30d,
        COALESCE(SUM(p.payment_value), 0) as actual_payment_30d
        
    FROM {{ ref('int_customer_daily_features') }} f
    LEFT JOIN {{ ref('stg_orders') }} o
        ON f.customer_id = o.customer_id
        AND date(o.order_purchase_timestamp) BETWEEN 
            date(f.date, '-30 days') AND f.date
        AND o.order_status NOT IN ('canceled', 'unavailable')
    LEFT JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE f.date >= date('now', '-7 days')  -- Only check recent features
    GROUP BY 1, 2, 3, 4
)

-- Return mismatches (errors)
SELECT 
    customer_id,
    date,
    orders_30d as feature_orders,
    actual_orders_30d,
    ABS(orders_30d - actual_orders_30d) as order_difference,
    'Feature vs actual order count mismatch' as error_type
FROM feature_validation
WHERE ABS(orders_30d - actual_orders_30d) > 2  -- Allow small tolerance