{{ config(materialized='table') }}

SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price::DECIMAL(10,2) as price,
    oi.freight_value::DECIMAL(10,2) as freight_value,
    o.order_purchase_timestamp::timestamp as order_purchase_timestamp,
    DATE(o.order_purchase_timestamp) as order_date,
    o.order_status
FROM {{ source('olist_data', 'olist_order_items') }} oi
INNER JOIN {{ source('olist_data', 'olist_orders') }} o
    ON oi.order_id = o.order_id
WHERE oi.order_id IS NOT NULL
  AND oi.seller_id IS NOT NULL
  AND oi.price > 0
  AND o.order_status NOT IN ('canceled', 'unavailable')
  AND o.order_purchase_timestamp IS NOT NULL
