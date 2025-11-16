{{ config(materialized='table') }}

-- Use variables with hierarchy demonstration
{% set lookback_days = var('lookback_days') %}
{% set max_date = var('max_date') %}
{% set min_order_value = var('min_order_value', 0) %}

-- Log variable values to see what's being used
{{ log("Using lookback_days: " ~ lookback_days, info=True) }}
{{ log("Max date: " ~ max_date, info=True) }}
{{ log("Min order value: " ~ min_order_value, info=True) }}

WITH customer_dates AS (
    SELECT 
        c.customer_id,
        c.landing_date,
        d.date_day as date
    FROM {{ ref('int_customer_landing') }} c
    CROSS JOIN (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="'2016-01-01'::date",
            end_date="'2018-12-31'::date"
        ) }}
    ) d
    WHERE d.date_day >= c.landing_date
      AND d.date_day <= '{{ max_date }}'::date
),

daily_payments AS (
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) as order_date,
        SUM(p.payment_value) as daily_payment_value
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
      AND p.payment_value >= {{ min_order_value }}  -- Using variable
    GROUP BY 1, 2
),

features AS (
    SELECT
        cd.customer_id,
        cd.date,
        cd.landing_date,
        
        -- Cumulative features
        COALESCE(
            SUM(dp.daily_payment_value) OVER (
                PARTITION BY cd.customer_id 
                ORDER BY cd.date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 0
        ) as total_payment_value,
        
        -- Dynamic rolling window features
        {% for days in lookback_days %}
        COALESCE(
            SUM(dp.daily_payment_value) OVER (
                PARTITION BY cd.customer_id 
                ORDER BY cd.date 
                ROWS BETWEEN {{ days - 1 }} PRECEDING AND CURRENT ROW
            ), 0
        ) as payment_{{ days }}d,
        
        COUNT(dp.order_date) OVER (
            PARTITION BY cd.customer_id 
            ORDER BY cd.date 
            ROWS BETWEEN {{ days - 1 }} PRECEDING AND CURRENT ROW
        ) as orders_{{ days }}d,
        {% endfor %}
        
        -- Days metrics
        cd.date - cd.landing_date as days_since_landing,
        cd.date - MAX(dp.order_date) OVER (
            PARTITION BY cd.customer_id 
            ORDER BY cd.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as days_since_last_order
        
    FROM customer_dates cd
    LEFT JOIN daily_payments dp
        ON cd.customer_id = dp.customer_id
        AND cd.date >= dp.order_date
)

SELECT * FROM features