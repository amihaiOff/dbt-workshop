{{ config(materialized='table') }}

-- Calculate daily payment features for customers using ingestion time tracking
-- This ensures point-in-time correctness: only count payments that had arrived by the feature date

WITH feature_dates AS (
    -- Generate all dates we want to calculate features for
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="'2018-01-01'::date",
        end_date="'2018-03-31'::date"
    ) }}
),

all_customers AS (
    -- Get all unique customers
    SELECT DISTINCT customer_id
    FROM {{ ref('stg_late_orders') }}
),

customer_feature_dates AS (
    -- For each customer, generate all feature dates
    SELECT
        ac.customer_id,
        fd.date_day as feature_date
    FROM all_customers ac
    CROSS JOIN feature_dates fd
),

point_in_time_payments AS (
    -- For each feature date, only include payments that:
    -- 1. Happened on or before the feature date (transaction_date <= feature_date)
    -- 2. Had arrived in our system by the feature date (ingestion_date <= feature_date)
    SELECT
        cfd.customer_id,
        cfd.feature_date,
        SUM(lo.payment_value) as total_payment_value,
        COUNT(DISTINCT lo.order_id) as total_orders
    FROM customer_feature_dates cfd
    LEFT JOIN {{ ref('stg_late_orders') }} lo
        ON cfd.customer_id = lo.customer_id
        -- Transaction must have happened by feature date
        AND lo.transaction_date <= cfd.feature_date
        -- Data must have arrived in our system by feature date (KEY LOGIC!)
        AND lo.ingestion_date <= cfd.feature_date
    GROUP BY 1, 2
)

SELECT
    customer_id,
    feature_date,
    COALESCE(total_payment_value, 0) as total_payment_value,
    COALESCE(total_orders, 0) as total_orders
FROM point_in_time_payments
ORDER BY customer_id, feature_date
