{{ config(materialized='table') }}

SELECT
    order_id,
    payment_sequential::int as payment_sequential,
    payment_type,
    payment_installments::int as payment_installments,
    payment_value::decimal(10,2) as payment_value
FROM {{ source('olist_data', 'olist_order_payments') }}
WHERE order_id IS NOT NULL
  AND payment_value > 0