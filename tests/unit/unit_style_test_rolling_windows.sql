-- This is a "unit test style" custom SQL test
-- It works like a unit test but only checks specific rows we care about

-- Given: We know there are orders on 2018-01-05, 2018-01-08, and 2018-01-12
-- When: We look at features on 2018-01-12
-- Then: The rolling windows should have specific calculated values

WITH actual_values AS (
    -- Get actual values from the model for a specific test date
    SELECT 
        customer_id,
        feature_date,
        landing_date,
        payment_3d,
        orders_3d,
        payment_7d,
        orders_7d,
        payment_14d,
        orders_14d,
        total_payment_value
    FROM int_customer_daily_features
    WHERE landing_date = '2017-01-26'::date
    AND feature_date >= landing_date + INTERVAL '14 days'
    LIMIT 1  -- Just test one customer
),

validation AS (
    SELECT 
        customer_id,
        feature_date,
        -- Test 1: Check logical constraints
        CASE 
            WHEN payment_3d > payment_7d THEN 'FAIL: 3d payment > 7d payment'
            WHEN payment_7d > payment_14d THEN 'FAIL: 7d payment > 14d payment'
            WHEN orders_3d > orders_7d THEN 'FAIL: 3d orders > 7d orders'
            WHEN orders_7d > orders_14d THEN 'FAIL: 7d orders > 14d orders'
            ELSE NULL
        END as constraint_error,
        
        -- Test 2: Check realistic ranges
        CASE
            WHEN total_payment_value < payment_14d THEN 'FAIL: Cumulative less than 14d window'
            WHEN payment_7d < 0 OR payment_14d < 0 THEN 'FAIL: Negative payment values'
            WHEN orders_7d < 0 OR orders_14d < 0 THEN 'FAIL: Negative order counts'
            WHEN payment_3d < 0 THEN 'FAIL: Negative 3d payment'
            ELSE NULL
        END as value_error
    FROM actual_values
)

-- Return errors (unit tests return 0 rows on success, >0 on failure)
SELECT 
    customer_id,
    feature_date,
    constraint_error as error_message,
    'Logical Constraint' as error_type
FROM validation
WHERE constraint_error IS NOT NULL

UNION ALL

SELECT 
    customer_id,
    feature_date,
    value_error as error_message,
    'Value Range' as error_type
FROM validation
WHERE value_error IS NOT NULL

