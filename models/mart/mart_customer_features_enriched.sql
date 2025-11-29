{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_city_tier 
         ON {{ this }} (city_tier, economic_zone)",
        "ANALYZE {{ this }}"
    ]
) }}

 WITH latest_features_ranked AS (
    -- Get most recent features for each customer with ranking
    SELECT 
        customer_id,
        date,
        total_payment_value as customer_revenue,
        orders_7d,
        orders_14d as total_orders,
        days_since_landing,
        landing_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date DESC) as rn
    FROM {{ ref('int_customer_daily_features') }}
),

latest_features AS (
    -- Filter to only the most recent record per customer
    SELECT 
        customer_id,
        date,
        customer_revenue,
        orders_7d,
        total_orders,
        days_since_landing,
        landing_date
    FROM latest_features_ranked
    WHERE rn = 1
),

customer_locations AS (
    -- Get customer city/state information
    SELECT 
        customer_id,
        customer_city as city,
        customer_state as state
    FROM {{ ref('stg_customers') }}
)

SELECT 
    f.*,
    l.city,
    l.state,
    -- Geographic enrichment from brazil_cities seed
    COALESCE(c.region, 'Unknown') as region,
    COALESCE(c.population_tier, 'unknown') as city_tier,
    COALESCE(c.economic_zone, 'unknown') as economic_zone,
    COALESCE(c.logistics_hub, 0) as is_logistics_hub,
    
    -- Add derived features based on enrichment
    CASE 
        WHEN c.economic_zone = 'primary' THEN 'high_opportunity'
        WHEN c.economic_zone = 'secondary' THEN 'medium_opportunity'
        ELSE 'emerging_market'
    END as market_opportunity,
    
    CASE 
        WHEN COALESCE(c.logistics_hub, 0) = 1 AND f.customer_revenue > 1000 THEN 'premium_fast_delivery'
        WHEN COALESCE(c.logistics_hub, 0) = 1 THEN 'standard_fast_delivery'
        WHEN f.customer_revenue > 1000 THEN 'premium_standard'
        ELSE 'standard'
    END as service_tier,
    
    -- Business score combining revenue and geography
    CASE 
        WHEN f.customer_revenue > 2000 AND c.economic_zone = 'primary' THEN 100
        WHEN f.customer_revenue > 2000 THEN 85
        WHEN f.customer_revenue > 1000 AND c.economic_zone = 'primary' THEN 75
        WHEN f.customer_revenue > 1000 THEN 65
        WHEN f.customer_revenue > 500 THEN 50
        WHEN f.customer_revenue > 200 THEN 35
        ELSE 20
    END as business_priority_score
    
FROM latest_features f
LEFT JOIN customer_locations l 
    ON f.customer_id = l.customer_id
LEFT JOIN {{ ref('brazil_cities') }} c 
    ON LOWER(TRIM(l.city)) = LOWER(TRIM(c.city))