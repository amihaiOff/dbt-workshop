{{ config(materialized='table') }}

{% set lookback_days = [3, 7, 14] %}

WITH customer_dates AS (
    -- Generate daily rows for each customer from landing to today
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
      AND d.date_day <= '2018-10-31'::date
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
        
        -- Days since landing
        cd.date - cd.landing_date as days_since_landing,
        
        -- Total orders
        COUNT(dp.order_date) OVER (
            PARTITION BY cd.customer_id 
            ORDER BY cd.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as total_orders,
        
        -- Rolling window features using Jinja
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
        
        -- Days since last order
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