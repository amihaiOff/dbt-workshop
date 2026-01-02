{{ config(materialized='table') }}

-- Staging model for late-arriving orders
-- Joins orders with payments to get transaction amounts
-- Includes both transaction time and ingestion time for point-in-time correctness

SELECT
    lo.order_id,
    lo.customer_id,
    lo.order_purchase_timestamp,
    DATE(lo.order_purchase_timestamp) as transaction_date,
    lo.order_approved_at as ingestion_timestamp,
    DATE(lo.order_approved_at) as ingestion_date,
    p.payment_value::DECIMAL(10,2) as payment_value,

    -- Calculate how late this order arrived (for analysis)
    (lo.order_approved_at - lo.order_purchase_timestamp) as arrival_delay

FROM {{ source('olist_data', 'olist_late_orders') }} lo
INNER JOIN {{ source('olist_data', 'olist_order_payments') }} p
    ON lo.order_id = p.order_id
WHERE lo.order_id IS NOT NULL
  AND lo.customer_id IS NOT NULL
  AND p.payment_value > 0
