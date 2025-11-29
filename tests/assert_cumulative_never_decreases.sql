-- Validate that cumulative features never decrease over time
-- For any customer, total_payment_value should only increase or stay the same

WITH customer_cumulative AS (
    SELECT 
        customer_id,
        total_payment_value,
        LAG(total_payment_value) OVER (
            PARTITION BY customer_id 
            ORDER BY feature_date
        ) as prev_total_payment
    FROM {{ ref('int_customer_daily_features') }}
)

-- Return rows where cumulative value decreased
SELECT 
    customer_id,
    total_payment_value as current_total,
    prev_total_payment as previous_total,
    'Cumulative payment decreased from ' || prev_total_payment || ' to ' || total_payment_value as error_message
FROM customer_cumulative
WHERE prev_total_payment IS NOT NULL
  AND total_payment_value < prev_total_payment
