{{ config(materialized='table') }}

-- Use variable to control analysis date for testing
{% set snapshot_date = var('snapshot_date', '2018-10-17') %}

-- Log which date we're using
{{ log("📅 Analyzing seller performance up to: " ~ snapshot_date, info=True) }}

WITH seller_metrics AS (
    SELECT
        seller_id,
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT product_id) as unique_products,
        SUM(price) as total_revenue,
        AVG(price) as avg_order_value,
        MIN(order_date) as first_sale_date,
        MAX(order_date) as last_sale_date,

        -- Calculate tier based on order volume
        CASE
            WHEN COUNT(DISTINCT order_id) >= 500 THEN 'platinum'
            WHEN COUNT(DISTINCT order_id) >= 100 THEN 'gold'
            WHEN COUNT(DISTINCT order_id) >= 20 THEN 'silver'
            ELSE 'bronze'
        END as seller_tier,

        -- Track when this calculation was made
        CURRENT_TIMESTAMP as updated_at

    FROM {{ ref('stg_order_items_snapshot') }}
    WHERE order_date <= '{{ snapshot_date }}'::date
    GROUP BY seller_id
)

SELECT
    seller_id,
    seller_tier,
    total_orders,
    unique_products,
    total_revenue,
    avg_order_value,
    first_sale_date,
    last_sale_date,
    updated_at
FROM seller_metrics
