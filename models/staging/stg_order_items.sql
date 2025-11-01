{{ config(materialized='table') }}

SELECT
    order_id,
    CAST(order_item_id AS INTEGER) as order_item_id,
    product_id,
    seller_id,
    datetime(shipping_limit_date) as shipping_limit_date,
    CAST(price AS REAL) as price,
    CAST(freight_value AS REAL) as freight_value
FROM {{ source('olist_data', 'olist_data__olist_order_items') }}
WHERE order_id IS NOT NULL
  AND price > 0