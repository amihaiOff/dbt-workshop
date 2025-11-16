{{ config(materialized='table') }}

-- Point-in-time correct features using ingestion time
WITH prediction_dates AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="'2017-01-01'::date",
        end_date="'2017-03-31'::date"
    ) }}
),

point_in_time_payments AS (
    SELECT 
        pd.date_day as prediction_date,
        o.customer_id,
        COUNT(DISTINCT o.order_id) as orders_count,
        SUM(p.payment_value) as total_payments
    FROM prediction_dates pd
    CROSS JOIN {{ ref('stg_orders_with_ingestion') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
      -- CRITICAL: Only use data we "knew" at prediction time
      AND o.simulated_arrival_time <= pd.date_day + INTERVAL '1 day'
      AND DATE(o.order_purchase_timestamp) <= pd.date_day
    GROUP BY 1, 2
)

SELECT 
    prediction_date as date,
    customer_id,
    COALESCE(orders_count, 0) as orders_count,
    COALESCE(total_payments, 0) as total_payments,
    'ingestion_time_tracking' as method
FROM prediction_dates pd
LEFT JOIN point_in_time_payments pit
    ON pd.date_day = pit.prediction_date