-- Ensure no missing dates in customer feature history
-- Critical for ML model training consistency

WITH customer_date_gaps AS (
    SELECT 
        customer_id,
        date,
        LAG(date) OVER (
            PARTITION BY customer_id 
            ORDER BY date
        ) as prev_date,
        -- Calculate days between consecutive dates
        EXTRACT(DAY FROM date - LAG(date) OVER (
            PARTITION BY customer_id 
            ORDER BY date
        )) as days_gap
    FROM {{ ref('int_customer_daily_features') }}
    WHERE date >= '2018-10-01'::date  -- Check last month of data
)

-- Return date gaps > 1 day (errors)
SELECT 
    customer_id,
    date,
    prev_date,
    days_gap,
    'Customer has ' || days_gap || ' day gap in feature history' as error_message
FROM customer_date_gaps
WHERE days_gap > 1
