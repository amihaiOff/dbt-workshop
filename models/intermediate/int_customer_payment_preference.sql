{{ config(materialized='table') }}

WITH customer_payments AS (
    -- Get most common payment type per customer
    SELECT 
        o.customer_id,
        p.payment_type,
        COUNT(*) as payment_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1, 2
),

ranked_preferences AS (
    SELECT 
        customer_id,
        payment_type,
        payment_count,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY payment_count DESC, payment_type
        ) as rank
    FROM customer_payments
)

SELECT 
    customer_id,
    payment_type as favorite_payment_method,
    payment_count as usage_count,
    CURRENT_TIMESTAMP::TIMESTAMP as updated_at  -- For snapshot tracking
FROM ranked_preferences
WHERE rank = 1