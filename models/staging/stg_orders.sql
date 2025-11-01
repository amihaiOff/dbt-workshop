{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status,
    datetime(order_purchase_timestamp) as order_purchase_timestamp,
    datetime(order_approved_at) as order_approved_at,
    datetime(order_delivered_carrier_date) as order_delivered_carrier_date,
    datetime(order_delivered_customer_date) as order_delivered_customer_date,
    datetime(order_estimated_delivery_date) as order_estimated_delivery_date,
    date(order_purchase_timestamp) as order_date
FROM {{ source('olist_data', 'olist_data__olist_orders') }}
WHERE order_status != 'unavailable'  -- Filter test orders
  AND order_purchase_timestamp IS NOT NULL