{{ config(materialized='table') }}

SELECT
    order_id,
    CAST(payment_sequential AS INTEGER) as payment_sequential,
    payment_type,
    CAST(payment_installments AS INTEGER) as payment_installments,
    CAST(payment_value AS REAL) as payment_value
FROM {{ source('olist_data', 'olist_data__olist_order_payments') }}
WHERE order_id IS NOT NULL
  AND payment_value > 0