{{ config(materialized='table') }}

-- Daily behavioral features for customers
-- Tracks product diversity, seller preferences, payment patterns, and engagement
-- Key: customer_id + date (joinable with mart_customer_features_enriched)

WITH customer_dates AS (
    -- Use same date spine as features mart for consistency
    SELECT DISTINCT
        customer_id,
        date as feature_date
    FROM {{ ref('int_customer_daily_features') }}
),

order_details AS (
    -- Get all order details with products, sellers, and payments
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) as order_date,
        oi.product_id,
        oi.seller_id,
        oi.price,
        p.payment_type,
        p.payment_value
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_items') }} oi
        ON o.order_id = oi.order_id
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
),

cumulative_diversity AS (
    -- Calculate cumulative product and seller diversity up to each date
    SELECT
        cd.customer_id,
        cd.feature_date,

        -- Product diversity metrics
        COUNT(DISTINCT CASE
            WHEN od.order_date <= cd.feature_date
            THEN od.product_id
        END) as cumulative_unique_products,

        -- Seller diversity metrics
        COUNT(DISTINCT CASE
            WHEN od.order_date <= cd.feature_date
            THEN od.seller_id
        END) as cumulative_unique_sellers,

        -- Price preferences (cumulative)
        AVG(CASE
            WHEN od.order_date <= cd.feature_date
            THEN od.price
        END) as avg_item_price_all_time,

        MIN(CASE
            WHEN od.order_date <= cd.feature_date
            THEN od.price
        END) as min_item_price,

        MAX(CASE
            WHEN od.order_date <= cd.feature_date
            THEN od.price
        END) as max_item_price

    FROM customer_dates cd
    LEFT JOIN order_details od
        ON cd.customer_id = od.customer_id
    GROUP BY cd.customer_id, cd.feature_date
),

rolling_metrics AS (
    -- Calculate rolling window metrics for recent behavior
    SELECT
        cd.customer_id,
        cd.feature_date,

        -- Recent product diversity (last 30 days)
        COUNT(DISTINCT CASE
            WHEN od.order_date <= cd.feature_date
            AND od.order_date > cd.feature_date - INTERVAL '30 days'
            THEN od.product_id
        END) as unique_products_30d,

        -- Recent seller diversity (last 30 days)
        COUNT(DISTINCT CASE
            WHEN od.order_date <= cd.feature_date
            AND od.order_date > cd.feature_date - INTERVAL '30 days'
            THEN od.seller_id
        END) as unique_sellers_30d,

        -- Recent average price
        AVG(CASE
            WHEN od.order_date <= cd.feature_date
            AND od.order_date > cd.feature_date - INTERVAL '30 days'
            THEN od.price
        END) as avg_item_price_30d,

        -- Count orders in different windows for engagement
        COUNT(DISTINCT CASE
            WHEN od.order_date <= cd.feature_date
            AND od.order_date > cd.feature_date - INTERVAL '7 days'
            THEN od.order_date
        END) as order_days_7d,

        COUNT(DISTINCT CASE
            WHEN od.order_date <= cd.feature_date
            AND od.order_date > cd.feature_date - INTERVAL '30 days'
            THEN od.order_date
        END) as order_days_30d

    FROM customer_dates cd
    LEFT JOIN order_details od
        ON cd.customer_id = od.customer_id
    GROUP BY cd.customer_id, cd.feature_date
),

payment_preferences AS (
    -- Calculate payment method preferences
    SELECT
        cd.customer_id,
        cd.feature_date,

        -- Most used payment type
        MODE() WITHIN GROUP (ORDER BY
            CASE WHEN od.order_date <= cd.feature_date
            THEN od.payment_type END
        ) as primary_payment_type,

        -- Payment method diversity
        COUNT(DISTINCT CASE
            WHEN od.order_date <= cd.feature_date
            THEN od.payment_type
        END) as payment_methods_used,

        -- Credit card usage rate
        SUM(CASE
            WHEN od.order_date <= cd.feature_date
            AND od.payment_type = 'credit_card'
            THEN od.payment_value
            ELSE 0
        END) / NULLIF(SUM(CASE
            WHEN od.order_date <= cd.feature_date
            THEN od.payment_value
        END), 0) as credit_card_rate

    FROM customer_dates cd
    LEFT JOIN order_details od
        ON cd.customer_id = od.customer_id
    GROUP BY cd.customer_id, cd.feature_date
)

SELECT
    cd.customer_id,
    cd.feature_date,

    -- Product diversity features
    COALESCE(cum.cumulative_unique_products, 0) as cumulative_unique_products,
    COALESCE(rm.unique_products_30d, 0) as unique_products_30d,

    -- Seller diversity features
    COALESCE(cum.cumulative_unique_sellers, 0) as cumulative_unique_sellers,
    COALESCE(rm.unique_sellers_30d, 0) as unique_sellers_30d,

    -- Price preference features
    ROUND(COALESCE(cum.avg_item_price_all_time, 0)::NUMERIC, 2) as avg_item_price_all_time,
    ROUND(COALESCE(rm.avg_item_price_30d, 0)::NUMERIC, 2) as avg_item_price_30d,
    ROUND(COALESCE(cum.min_item_price, 0)::NUMERIC, 2) as min_item_price,
    ROUND(COALESCE(cum.max_item_price, 0)::NUMERIC, 2) as max_item_price,

    -- Price tier preference (derived feature)
    CASE
        WHEN COALESCE(cum.avg_item_price_all_time, 0) >= 200 THEN 'premium'
        WHEN COALESCE(cum.avg_item_price_all_time, 0) >= 100 THEN 'mid_tier'
        WHEN COALESCE(cum.avg_item_price_all_time, 0) >= 50 THEN 'value'
        WHEN COALESCE(cum.avg_item_price_all_time, 0) > 0 THEN 'budget'
        ELSE 'no_purchases'
    END as price_tier_preference,

    -- Payment behavior features
    COALESCE(pp.primary_payment_type, 'unknown') as primary_payment_type,
    COALESCE(pp.payment_methods_used, 0) as payment_methods_used,
    ROUND(COALESCE(pp.credit_card_rate, 0)::NUMERIC, 2) as credit_card_usage_rate,

    -- Engagement features
    COALESCE(rm.order_days_7d, 0) as order_days_7d,
    COALESCE(rm.order_days_30d, 0) as order_days_30d,

    -- Engagement tier using the classify_tier macro from Challenge 2!
    {{ classify_tier('COALESCE(rm.order_days_30d, 0)', 'volume') }} as engagement_tier,

    -- Diversity score (combined metric)
    CASE
        WHEN COALESCE(cum.cumulative_unique_products, 0) >= 10
         AND COALESCE(cum.cumulative_unique_sellers, 0) >= 5 THEN 'high_diversity'
        WHEN COALESCE(cum.cumulative_unique_products, 0) >= 5
         AND COALESCE(cum.cumulative_unique_sellers, 0) >= 3 THEN 'medium_diversity'
        WHEN COALESCE(cum.cumulative_unique_products, 0) > 0 THEN 'low_diversity'
        ELSE 'no_diversity'
    END as diversity_profile

FROM customer_dates cd
LEFT JOIN cumulative_diversity cum
    ON cd.customer_id = cum.customer_id
    AND cd.feature_date = cum.feature_date
LEFT JOIN rolling_metrics rm
    ON cd.customer_id = rm.customer_id
    AND cd.feature_date = rm.feature_date
LEFT JOIN payment_preferences pp
    ON cd.customer_id = pp.customer_id
    AND cd.feature_date = pp.feature_date
ORDER BY cd.customer_id, cd.feature_date
