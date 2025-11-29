-- Custom test to validate rolling window calculations
-- Tests that 7d aggregations are always <= 14d aggregations

WITH window_validation AS (
    SELECT 
        customer_id,
        landing_date,
        payment_7d,
        payment_14d,
        orders_7d,
        orders_14d
    FROM {{ ref('int_customer_daily_features') }}
)

-- Return rows where 7d > 14d (which should be impossible)
SELECT 
    customer_id,
    landing_date,
    payment_7d,
    payment_14d,
    orders_7d,
    orders_14d,
    'payment_7d (' || payment_7d || ') exceeds payment_14d (' || payment_14d || ')' as error_message
FROM window_validation
WHERE payment_7d > payment_14d
   OR orders_7d > orders_14d
