{{ config(materialized='table') }}

SELECT
    order_id,
    order_item_id::int as order_item_id,
    product_id,
    seller_id,
    shipping_limit_date::timestamp as shipping_limit_date,
    price::decimal(10,2) as price,
    freight_value::decimal(10,2) as freight_value
FROM {{ source('olist_data', 'olist_order_items') }}
WHERE order_id IS NOT NULL
  AND price > 0