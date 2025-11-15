{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp as order_purchase_timestamp,
    order_approved_at::timestamp as order_approved_at,
    order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
    order_delivered_customer_date::timestamp as order_delivered_customer_date,
    order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
    DATE(order_purchase_timestamp) as order_date
FROM {{ source('olist_data', 'olist_orders') }}
WHERE order_status != 'unavailable'  -- Filter test orders
  AND order_purchase_timestamp IS NOT NULL
  {{ build_until('order_purchase_timestamp') }}