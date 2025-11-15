{% macro calculate_rfm_metrics(analysis_date='2018-10-31', lookback_days=365) %}

SELECT 
    o.customer_id,
    -- Recency: Days since last purchase (lower is better)
    '{{ analysis_date }}'::date - MAX(DATE(o.order_purchase_timestamp)) as recency_days,
    
    -- Frequency: Number of distinct orders
    COUNT(DISTINCT o.order_id) as frequency_orders,
    
    -- Monetary: Total amount spent
    SUM(p.payment_value) as monetary_value,
    
    '{{ analysis_date }}'::date as analysis_date
    
FROM {{ ref('stg_orders') }} o
INNER JOIN {{ ref('stg_order_payments') }} p
    ON o.order_id = p.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
  AND DATE(o.order_purchase_timestamp) >= ('{{ analysis_date }}'::date - {{ lookback_days }})
  AND DATE(o.order_purchase_timestamp) <= '{{ analysis_date }}'::date
GROUP BY o.customer_id

{% endmacro %}