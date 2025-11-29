-- This test fails if any orders have negative total value
-- Custom tests return ERRORS (rows), not successes (empty results)

WITH order_totals AS (
    SELECT 
        o.order_id,
        o.customer_id,
        date(o.order_purchase_timestamp) as order_date,
        COALESCE(SUM(p.payment_value), 0) as total_payment
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1, 2, 3
)

-- Return rows only when there's a problem
SELECT 
    order_id,
    customer_id,
    order_date,
    total_payment,
    'Order has negative total payment: ' || total_payment as error_message
FROM order_totals
WHERE total_payment < 0