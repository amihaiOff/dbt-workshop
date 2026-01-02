#!/bin/bash

#==============================================================================
# Reset DBT Project State to End of Session
#==============================================================================
#
# Detect Docker Compose command (support both old and new versions)
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo "Error: Neither 'docker-compose' nor 'docker compose' is available"
    echo "Please install Docker Desktop which includes Docker Compose"
    exit 1
fi

#==============================================================================
#
# DESCRIPTION:
#   This script resets your dbt project and database to match the state at
#   the end of a specific training session. It creates all solution models
#   for that session and runs dbt to populate the database.
#
# USAGE:
#   ./reset_to_session.sh <session_number>
#
# EXAMPLES:
#   ./reset_to_session.sh 1    # Reset to end of Session 1 (foundations)
#   ./reset_to_session.sh 2    # Reset to end of Session 2 (advanced patterns)
#
# WHAT IT DOES:
#   1. Cleans up existing dbt model files
#   2. Drops dbt-created tables (keeps source data)
#   3. Creates all solution models for the specified session
#   4. Runs dbt to build tables
#   5. For Session 2: Also takes multiple snapshots to demonstrate tier changes
#
# SESSIONS AVAILABLE:
#   1 - Session 1: dbt Foundations
#       - Staging models (stg_orders, stg_customers, stg_order_items, stg_order_payments)
#       - Intermediate models (int_customer_landing, int_customer_daily_features)
#
#   2 - Session 2: Advanced Patterns (includes Session 1 + below)
#       - Snapshot models (stg_order_items_snapshot, int_seller_performance)
#       - Snapshots (snap_seller_tier with 4 time-based iterations)
#
#   3 - Session 3: Testing & Production (includes Session 1 & 2 + below)
#       - Seeds (brazil_cities.csv for geographic data)
#       - Macros (classify_tier for reusable tier logic)
#       - Customer tiers model (int_customer_tiers using classify_tier macro)
#
# NOTES:
#   - Source data (olist_* tables) is preserved
#   - Does NOT require database reset (faster than setup.sh --reset)
#   - Creates consistent state for all workshop participants
#
#==============================================================================

set -e  # Exit on error

SESSION=$1

if [ -z "$SESSION" ]; then
    echo "Usage: ./reset_to_session.sh <session_number|end>"
    echo "Example: ./reset_to_session.sh 1"
    echo ""
    echo "Available options:"
    echo "  1   - Session 1: dbt Foundations (staging + intermediate models)"
    echo "  2   - Session 2: Advanced Patterns (variables + snapshots + incremental)"
    echo "  3   - Session 3: Testing & Production (seeds + macros + testing)"
    echo "  end - Final State: All solutions completed (includes all session 3 challenges)"
    exit 1
fi

echo "======================================"
echo "Resetting to: $SESSION"
echo "======================================"
echo ""

# Restore all placeholder files from git (keeps future session files as placeholders)
echo "Step 1: Restoring placeholder files from git..."
git restore models/staging/*.sql models/intermediate/*.sql models/mart/*.sql snapshots/*.yml macros/*.sql seeds/*.csv models/staging/schema.yml 2>/dev/null || true
# Note: This preserves placeholder files for future sessions

# Clean up dbt-created tables in database (keep source tables)
echo ""
echo "Step 2: Cleaning up dbt tables in database..."
$DOCKER_COMPOSE exec -T postgres psql -U dbt_user -d dbt_workshop -q << 'EOSQL'
DO $$
DECLARE
    r RECORD;
BEGIN
    -- Drop all tables that start with stg_, int_, mart_, snap_ in olist_data schema
    -- These are dbt-created tables, not source tables (olist_*)
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'olist_data'
        AND (
            tablename LIKE 'stg_%' OR
            tablename LIKE 'int_%' OR
            tablename LIKE 'mart_%' OR
            tablename LIKE 'snap_%'
        )
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS olist_data.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;
EOSQL

echo "  ✓ Cleaned up dbt-created tables (kept source tables)"

# Create late-arriving orders source table for Session 2 Challenge 4
echo ""
echo "Step 2.5: Creating late-arriving orders source table..."
$DOCKER_COMPOSE exec -T postgres psql -U dbt_user -d dbt_workshop -q << 'EOSQL'
DROP TABLE IF EXISTS olist_data.olist_late_orders;

CREATE TABLE olist_data.olist_late_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp::timestamp as order_purchase_timestamp,
    -- Simulate late arrivals using deterministic hash-based delays
    -- This ensures consistent results across resets
    CASE
        -- Orders where hash mod 10 = 0: arrive 7-14 days late (10%)
        WHEN MOD(ABS(HASHTEXT(o.order_id)), 10) = 0 THEN
            o.order_purchase_timestamp::timestamp + INTERVAL '7 days' +
            (MOD(ABS(HASHTEXT(o.order_id || 'salt1')), 7) || ' days')::INTERVAL
        -- Orders where hash mod 20 = 1: arrive 14-30 days late (5%)
        WHEN MOD(ABS(HASHTEXT(o.order_id)), 20) = 1 THEN
            o.order_purchase_timestamp::timestamp + INTERVAL '14 days' +
            (MOD(ABS(HASHTEXT(o.order_id || 'salt2')), 16) || ' days')::INTERVAL
        -- Rest arrive within 1-2 days (normal)
        ELSE
            o.order_purchase_timestamp::timestamp + INTERVAL '1 day' +
            (MOD(ABS(HASHTEXT(o.order_id || 'salt3')), 24) || ' hours')::INTERVAL
    END as order_approved_at,
    o.order_status
FROM olist_data.olist_orders o
WHERE o.order_status NOT IN ('canceled', 'unavailable')
  AND o.order_purchase_timestamp IS NOT NULL
  AND o.order_purchase_timestamp::timestamp <= '2018-03-31'::timestamp
LIMIT 5000;
EOSQL

echo "  ✓ Created olist_late_orders with simulated late arrivals"

# Create models based on session
case $SESSION in
    1)
        echo ""
        echo "Step 3: Creating Session 1 models..."

        # Create schema.yml with source definitions (required for all sessions)
        cat > models/staging/schema.yml << 'EOF'
version: 2

sources:
  - name: olist_data
    schema: olist_data
    tables:
      - name: olist_orders
        columns:
          - name: order_id
            description: Unique order identifier
      - name: olist_customers
        columns:
          - name: customer_id
            description: Unique customer identifier
      - name: olist_order_items
        columns:
          - name: order_id
            description: Order identifier
      - name: olist_order_payments
        columns:
          - name: order_id
            description: Order identifier
      - name: olist_late_orders
        description: Orders with simulated late arrival times for testing ingestion time tracking
        columns:
          - name: order_id
            description: Order identifier
          - name: customer_id
            description: Customer identifier
          - name: order_purchase_timestamp
            description: When the order was actually purchased (transaction time)
          - name: order_approved_at
            description: When the order was approved/entered our system (ingestion time)
EOF

        # Staging models
        cat > models/staging/stg_orders.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp as order_purchase_timestamp,
    order_approved_at::timestamp as order_approved_at,
    order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
    order_delivered_customer_date::timestamp as order_delivered_customer_date,
    order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
    DATE(order_purchase_timestamp) as order_date
FROM {{ source('olist_data', 'olist_orders') }}
WHERE order_status != 'unavailable'  -- Filter test orders
  AND order_purchase_timestamp IS NOT NULL
EOF

        cat > models/staging/stg_customers.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('olist_data', 'olist_customers') }}
WHERE customer_id IS NOT NULL
EOF

        cat > models/staging/stg_order_items.sql << 'EOF'
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
EOF

        cat > models/staging/stg_order_payments.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    order_id,
    payment_sequential::int as payment_sequential,
    payment_type,
    payment_installments::int as payment_installments,
    payment_value::decimal(10,2) as payment_value
FROM {{ source('olist_data', 'olist_order_payments') }}
WHERE order_id IS NOT NULL
  AND payment_value > 0
EOF

        # Intermediate models
        cat > models/intermediate/int_customer_landing.sql << 'EOF'
{{ config(materialized='table') }}

WITH first_orders AS (
    SELECT
        o.customer_id,
        MIN(o.order_purchase_timestamp) as first_order_timestamp,
        DATE(MIN(o.order_purchase_timestamp)) as landing_date
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY o.customer_id
)

SELECT
    customer_id,
    first_order_timestamp,
    landing_date,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'landing_date']) }} as customer_unique_key
FROM first_orders
EOF

        cat > models/intermediate/int_customer_daily_features.sql << 'EOF'
{{ config(materialized='table') }}

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
      AND d.date_day <= '2018-10-31'::date  -- Latest date in dataset
),

daily_payments AS (
    -- Calculate daily payment totals per customer
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) as order_date,
        SUM(p.payment_value) as daily_payment_value,
        COUNT(DISTINCT o.order_id) as daily_order_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1, 2
)

SELECT
    cd.customer_id,
    cd.date,
    cd.landing_date,

    -- Cumulative payment value up to this date
    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_payment_value,

    -- Days since landing
    cd.date - cd.landing_date as days_since_landing,

    -- Cumulative order count
    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_orders

FROM customer_dates cd
LEFT JOIN daily_payments dp
    ON cd.customer_id = dp.customer_id
    AND cd.date = dp.order_date
EOF

        echo "Step 4: Running dbt..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run

        echo ""
        echo "✅ Session 1 setup complete!"
        echo "Created models:"
        echo "  - stg_orders, stg_customers, stg_order_items, stg_order_payments"
        echo "  - int_customer_landing"
        echo "  - int_customer_daily_features"
        ;;

    2)
        echo ""
        echo "Step 3: Creating Session 1 + Session 2 models..."

        # Create schema.yml with source definitions (required for all sessions)
        cat > models/staging/schema.yml << 'EOF'
version: 2

sources:
  - name: olist_data
    schema: olist_data
    tables:
      - name: olist_orders
        columns:
          - name: order_id
            description: Unique order identifier
      - name: olist_customers
        columns:
          - name: customer_id
            description: Unique customer identifier
      - name: olist_order_items
        columns:
          - name: order_id
            description: Order identifier
      - name: olist_order_payments
        columns:
          - name: order_id
            description: Order identifier
      - name: olist_late_orders
        description: Orders with simulated late arrival times for testing ingestion time tracking
        columns:
          - name: order_id
            description: Order identifier
          - name: customer_id
            description: Customer identifier
          - name: order_purchase_timestamp
            description: When the order was actually purchased (transaction time)
          - name: order_approved_at
            description: When the order was approved/entered our system (ingestion time)
EOF

        # First create all Session 1 models (same as case 1, but without running dbt yet)
        # Staging models
        cat > models/staging/stg_orders.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp as order_purchase_timestamp,
    order_approved_at::timestamp as order_approved_at,
    order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
    order_delivered_customer_date::timestamp as order_delivered_customer_date,
    order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
    DATE(order_purchase_timestamp) as order_date
FROM {{ source('olist_data', 'olist_orders') }}
WHERE order_status != 'unavailable'  -- Filter test orders
  AND order_purchase_timestamp IS NOT NULL
EOF

        cat > models/staging/stg_customers.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('olist_data', 'olist_customers') }}
WHERE customer_id IS NOT NULL
EOF

        cat > models/staging/stg_order_items.sql << 'EOF'
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
EOF

        cat > models/staging/stg_order_payments.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    order_id,
    payment_sequential::int as payment_sequential,
    payment_type,
    payment_installments::int as payment_installments,
    payment_value::decimal(10,2) as payment_value
FROM {{ source('olist_data', 'olist_order_payments') }}
WHERE order_id IS NOT NULL
  AND payment_value > 0
EOF

        cat > models/intermediate/int_customer_landing.sql << 'EOF'
{{ config(materialized='table') }}

WITH first_orders AS (
    SELECT
        o.customer_id,
        MIN(o.order_purchase_timestamp) as first_order_timestamp,
        DATE(MIN(o.order_purchase_timestamp)) as landing_date
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY o.customer_id
)

SELECT
    customer_id,
    first_order_timestamp,
    landing_date,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'landing_date']) }} as customer_unique_key
FROM first_orders
EOF

        cat > models/intermediate/int_customer_daily_features.sql << 'EOF'
{{ config(materialized='table') }}

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
      AND d.date_day <= '2018-10-31'::date  -- Latest date in dataset
),

daily_payments AS (
    -- Calculate daily payment totals per customer
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) as order_date,
        SUM(p.payment_value) as daily_payment_value,
        COUNT(DISTINCT o.order_id) as daily_order_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1, 2
)

SELECT
    cd.customer_id,
    cd.date,
    cd.landing_date,

    -- Cumulative payment value up to this date
    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_payment_value,

    -- Days since landing
    cd.date - cd.landing_date as days_since_landing,

    -- Cumulative order count
    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_orders

FROM customer_dates cd
LEFT JOIN daily_payments dp
    ON cd.customer_id = dp.customer_id
    AND cd.date = dp.order_date
EOF

        # Add Session 2 specific models
        cat > models/staging/stg_order_items_snapshot.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price::DECIMAL(10,2) as price,
    oi.freight_value::DECIMAL(10,2) as freight_value,
    o.order_purchase_timestamp::timestamp as order_purchase_timestamp,
    DATE(o.order_purchase_timestamp) as order_date,
    o.order_status
FROM {{ source('olist_data', 'olist_order_items') }} oi
INNER JOIN {{ source('olist_data', 'olist_orders') }} o
    ON oi.order_id = o.order_id
WHERE oi.order_id IS NOT NULL
  AND oi.seller_id IS NOT NULL
  AND oi.price > 0
  AND o.order_status NOT IN ('canceled', 'unavailable')
  AND o.order_purchase_timestamp IS NOT NULL
EOF

        cat > models/staging/stg_late_orders.sql << 'EOF'
{{ config(materialized='table') }}

-- Staging model for late-arriving orders
-- Joins orders with payments to get transaction amounts
-- Includes both transaction time and ingestion time for point-in-time correctness

SELECT
    lo.order_id,
    lo.customer_id,
    lo.order_purchase_timestamp,
    DATE(lo.order_purchase_timestamp) as transaction_date,
    lo.order_approved_at as ingestion_timestamp,
    DATE(lo.order_approved_at) as ingestion_date,
    p.payment_value::DECIMAL(10,2) as payment_value,

    -- Calculate how late this order arrived (for analysis)
    (lo.order_approved_at - lo.order_purchase_timestamp) as arrival_delay

FROM {{ source('olist_data', 'olist_late_orders') }} lo
INNER JOIN {{ source('olist_data', 'olist_order_payments') }} p
    ON lo.order_id = p.order_id
WHERE lo.order_id IS NOT NULL
  AND lo.customer_id IS NOT NULL
  AND p.payment_value > 0
EOF

        cat > models/intermediate/int_seller_performance.sql << 'EOF'
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
EOF

        cat > models/intermediate/int_customer_payment_features.sql << 'EOF'
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
EOF

        cat > snapshots/snapshots.yml << 'EOF'
version: 2

snapshots:
  - name: snap_seller_tier
    relation: ref('int_seller_performance')
    config:
      unique_key: seller_id
      strategy: timestamp
      updated_at: updated_at
EOF

        echo "Step 4: Running dbt models..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run

        echo ""
        echo "Step 5: Taking snapshots..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2017-01-01"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2017-06-30"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2018-01-31"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2018-10-17"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier

        echo ""
        echo "✅ Session 2 setup complete!"
        echo "Created additional models:"
        echo "  - stg_order_items_snapshot"
        echo "  - int_seller_performance"
        echo "  - snap_seller_tier (with 4 snapshot iterations)"
        ;;

    3)
        echo ""
        echo "Step 3: Creating Session 1 + Session 2 + Session 3 models..."

        # Create schema.yml with source definitions (required for all sessions)
        cat > models/staging/schema.yml << 'EOF'
version: 2

sources:
  - name: olist_data
    schema: olist_data
    tables:
      - name: olist_orders
        columns:
          - name: order_id
            description: Unique order identifier
      - name: olist_customers
        columns:
          - name: customer_id
            description: Unique customer identifier
      - name: olist_order_items
        columns:
          - name: order_id
            description: Order identifier
      - name: olist_order_payments
        columns:
          - name: order_id
            description: Order identifier
      - name: olist_late_orders
        description: Orders with simulated late arrival times for testing ingestion time tracking
        columns:
          - name: order_id
            description: Order identifier
          - name: customer_id
            description: Customer identifier
          - name: order_purchase_timestamp
            description: When the order was actually purchased (transaction time)
          - name: order_approved_at
            description: When the order was approved/entered our system (ingestion time)
EOF

        # Create all Session 1 staging models
        cat > models/staging/stg_orders.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp as order_purchase_timestamp,
    order_approved_at::timestamp as order_approved_at,
    order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
    order_delivered_customer_date::timestamp as order_delivered_customer_date,
    order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
    DATE(order_purchase_timestamp) as order_date
FROM {{ source('olist_data', 'olist_orders') }}
WHERE order_status != 'unavailable'  -- Filter test orders
  AND order_purchase_timestamp IS NOT NULL
EOF

        cat > models/staging/stg_customers.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('olist_data', 'olist_customers') }}
WHERE customer_id IS NOT NULL
EOF

        cat > models/staging/stg_order_items.sql << 'EOF'
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
EOF

        cat > models/staging/stg_order_payments.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    order_id,
    payment_sequential::int as payment_sequential,
    payment_type,
    payment_installments::int as payment_installments,
    payment_value::decimal(10,2) as payment_value
FROM {{ source('olist_data', 'olist_order_payments') }}
WHERE order_id IS NOT NULL
  AND payment_value > 0
EOF

        # Session 1 intermediate models
        cat > models/intermediate/int_customer_landing.sql << 'EOF'
{{ config(materialized='table') }}

WITH first_orders AS (
    SELECT
        o.customer_id,
        MIN(o.order_purchase_timestamp) as first_order_timestamp,
        DATE(MIN(o.order_purchase_timestamp)) as landing_date
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY o.customer_id
)

SELECT
    customer_id,
    first_order_timestamp,
    landing_date,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'landing_date']) }} as customer_unique_key
FROM first_orders
EOF

        cat > models/intermediate/int_customer_daily_features.sql << 'EOF'
{{ config(materialized='table') }}

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
      AND d.date_day <= '2018-10-31'::date  -- Latest date in dataset
),

daily_payments AS (
    -- Calculate daily payment totals per customer
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) as order_date,
        SUM(p.payment_value) as daily_payment_value,
        COUNT(DISTINCT o.order_id) as daily_order_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1, 2
)

SELECT
    cd.customer_id,
    cd.date,
    cd.landing_date,

    -- Cumulative payment value up to this date
    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_payment_value,

    -- Days since landing
    cd.date - cd.landing_date as days_since_landing,

    -- Cumulative order count
    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_orders,

    -- Rolling window features for Session 3
    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as payment_7d,

    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as orders_7d,

    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as payment_14d,

    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as orders_14d

FROM customer_dates cd
LEFT JOIN daily_payments dp
    ON cd.customer_id = dp.customer_id
    AND cd.date = dp.order_date
EOF

        # Session 2 models
        cat > models/staging/stg_order_items_snapshot.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price::DECIMAL(10,2) as price,
    oi.freight_value::DECIMAL(10,2) as freight_value,
    o.order_purchase_timestamp::timestamp as order_purchase_timestamp,
    DATE(o.order_purchase_timestamp) as order_date,
    o.order_status
FROM {{ source('olist_data', 'olist_order_items') }} oi
INNER JOIN {{ source('olist_data', 'olist_orders') }} o
    ON oi.order_id = o.order_id
WHERE oi.order_id IS NOT NULL
  AND oi.seller_id IS NOT NULL
  AND oi.price > 0
  AND o.order_status NOT IN ('canceled', 'unavailable')
  AND o.order_purchase_timestamp IS NOT NULL
EOF

        cat > models/staging/stg_late_orders.sql << 'EOF'
{{ config(materialized='table') }}

-- Staging model for late-arriving orders
-- Joins orders with payments to get transaction amounts
-- Includes both transaction time and ingestion time for point-in-time correctness

SELECT
    lo.order_id,
    lo.customer_id,
    lo.order_purchase_timestamp,
    DATE(lo.order_purchase_timestamp) as transaction_date,
    lo.order_approved_at as ingestion_timestamp,
    DATE(lo.order_approved_at) as ingestion_date,
    p.payment_value::DECIMAL(10,2) as payment_value,

    -- Calculate how late this order arrived (for analysis)
    (lo.order_approved_at - lo.order_purchase_timestamp) as arrival_delay

FROM {{ source('olist_data', 'olist_late_orders') }} lo
INNER JOIN {{ source('olist_data', 'olist_order_payments') }} p
    ON lo.order_id = p.order_id
WHERE lo.order_id IS NOT NULL
  AND lo.customer_id IS NOT NULL
  AND p.payment_value > 0
EOF

        cat > models/intermediate/int_seller_performance.sql << 'EOF'
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
        CURRENT_TIMESTAMP as updated_at
    FROM {{ ref('stg_order_items_snapshot') }}
    WHERE order_date <= '{{ snapshot_date }}'::date
    GROUP BY seller_id
)

SELECT
    seller_id,

    -- Use the classify_tier macro for consistent tier logic
    {{ classify_tier('total_orders', 'volume') }} as seller_tier,

    total_orders,
    unique_products,
    total_revenue,
    avg_order_value,
    first_sale_date,
    last_sale_date,
    updated_at
FROM seller_metrics
EOF

        cat > models/intermediate/int_customer_payment_features.sql << 'EOF'
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
EOF

        # Session 3: Customer Tiers Model
        cat > models/intermediate/int_customer_tiers.sql << 'EOF'
{{ config(materialized='table') }}

WITH customer_metrics AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(p.payment_value) as total_revenue,
        AVG(p.payment_value) as avg_order_value,
        MIN(DATE(o.order_purchase_timestamp)) as first_order_date,
        MAX(DATE(o.order_purchase_timestamp)) as last_order_date
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY o.customer_id
)

SELECT
    customer_id,

    -- Use the SAME classify_tier macro for customer tiers!
    {{ classify_tier('total_orders', 'volume') }} as tier_by_volume,
    {{ classify_tier('total_revenue', 'revenue') }} as tier_by_revenue,

    total_orders,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,

    -- Composite tier: take the better of the two
    CASE
        WHEN {{ classify_tier('total_orders', 'volume') }} = 'platinum'
          OR {{ classify_tier('total_revenue', 'revenue') }} = 'platinum' THEN 'platinum'
        WHEN {{ classify_tier('total_orders', 'volume') }} = 'gold'
          OR {{ classify_tier('total_revenue', 'revenue') }} = 'gold' THEN 'gold'
        WHEN {{ classify_tier('total_orders', 'volume') }} = 'silver'
          OR {{ classify_tier('total_revenue', 'revenue') }} = 'silver' THEN 'silver'
        ELSE 'bronze'
    END as composite_tier

FROM customer_metrics
EOF

        cat > snapshots/snapshots.yml << 'EOF'
version: 2

snapshots:
  - name: snap_seller_tier
    relation: ref('int_seller_performance')
    config:
      unique_key: seller_id
      strategy: timestamp
      updated_at: updated_at
EOF

        # Session 2 incremental model (carried forward to Session 3)
        cat > models/intermediate/int_customer_daily_features_inc.sql << 'EOF'
{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'date'],
    incremental_strategy='merge'
) }}

{% set max_date = var('max_date', '2018-10-31') %}

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
      
    {% if is_incremental() %}
      -- Only process recent feature dates for incremental runs
      AND d.date_day >= (
        SELECT MAX(date) - INTERVAL '14 days'
        FROM {{ this }}
      )
    {% endif %}
),

daily_payments AS (
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) as order_date,
        SUM(p.payment_value) as daily_payment_value,
        COUNT(DISTINCT o.order_id) as daily_order_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    
    {% if is_incremental() %}
      -- Look back for late-arriving data
      AND DATE(o.order_purchase_timestamp) >= (
        SELECT MAX(date) - INTERVAL '14 days'
        FROM {{ this }}
      )
    {% endif %}
    
    GROUP BY 1, 2
)

SELECT
    cd.customer_id,
    cd.date,
    cd.landing_date,
    
    -- Cumulative payment value
    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id 
        ORDER BY cd.date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_payment_value,
    
    -- Cumulative order count
    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id 
        ORDER BY cd.date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_orders,
    
    -- Days since landing
    cd.date - cd.landing_date as days_since_landing
    
FROM customer_dates cd
LEFT JOIN daily_payments dp
    ON cd.customer_id = dp.customer_id
    AND cd.date = dp.order_date
EOF

        # Session 3: Seeds
        mkdir -p seeds
        cat > seeds/brazil_cities.csv << 'EOF'
city,state,region,population_tier,economic_zone,logistics_hub
São Paulo,SP,Southeast,mega,primary,1
Rio de Janeiro,RJ,Southeast,mega,primary,1
Brasília,DF,Central-West,large,primary,1
Salvador,BA,Northeast,large,secondary,1
Fortaleza,CE,Northeast,large,secondary,1
Belo Horizonte,MG,Southeast,large,secondary,1
Manaus,AM,North,large,tertiary,1
Curitiba,PR,South,large,secondary,1
Recife,PE,Northeast,large,secondary,1
Porto Alegre,RS,South,large,secondary,1
Belém,PA,North,medium,tertiary,0
Goiânia,GO,Central-West,medium,tertiary,0
Guarulhos,SP,Southeast,medium,primary,1
Campinas,SP,Southeast,medium,primary,1
São Luís,MA,Northeast,medium,tertiary,0
EOF

        # Session 3: Tier Classification Macro
        mkdir -p macros
        cat > macros/classify_tier.sql << 'EOF'
{% macro classify_tier(metric_column, tier_type='volume') %}
    CASE
        {% if tier_type == 'volume' %}
            -- Volume-based tiers (order count)
            WHEN {{ metric_column }} >= 500 THEN 'platinum'
            WHEN {{ metric_column }} >= 100 THEN 'gold'
            WHEN {{ metric_column }} >= 20 THEN 'silver'
            ELSE 'bronze'
        {% elif tier_type == 'revenue' %}
            -- Revenue-based tiers (total value)
            WHEN {{ metric_column }} >= 10000 THEN 'platinum'
            WHEN {{ metric_column }} >= 5000 THEN 'gold'
            WHEN {{ metric_column }} >= 1000 THEN 'silver'
            ELSE 'bronze'
        {% else %}
            -- Default to volume if unknown type
            WHEN {{ metric_column }} >= 500 THEN 'platinum'
            WHEN {{ metric_column }} >= 100 THEN 'gold'
            WHEN {{ metric_column }} >= 20 THEN 'silver'
            ELSE 'bronze'
        {% endif %}
    END
{% endmacro %}
EOF

        # Session 3: Mart 1 - Customer Features Enriched
        mkdir -p models/mart
        cat > models/mart/mart_customer_features_enriched.sql << 'EOF'
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
        total_orders,
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
EOF

        # Session 3: Mart 2 - Customer Behavior Daily
        cat > models/mart/mart_customer_behavior_daily.sql << 'EOF'
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
EOF

        echo "Step 4: Running dbt models..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run

        echo ""
        echo "Step 5: Loading seeds..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt seed

        echo ""
        echo "Step 6: Taking snapshots..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2017-01-01"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2017-06-30"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2018-01-31"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2018-10-17"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier

        echo ""
        echo "✅ Session 3 setup complete!"
        echo "Created additional models:"
        echo "  - seeds/brazil_cities.csv"
        echo "  - macros/classify_tier.sql"
        echo "  - int_customer_tiers (using classify_tier macro)"
        echo "  - int_customer_daily_features_inc (incremental version)"
        echo "  - int_seller_performance (refactored to use classify_tier macro)"
        echo "  - mart_customer_features_enriched (geographic enrichment)"
        echo "  - mart_customer_behavior_daily (behavioral features)"
        echo "Ready for testing challenges!"
        ;;

    end)
        echo ""
        echo "Step 3: Creating FINAL STATE with all solutions..."

        # ============================================================
        # Include everything from Session 3, plus all challenge solutions
        # ============================================================

        # Create schema.yml with source definitions
        cat > models/staging/schema.yml << 'EOF'
version: 2

sources:
  - name: olist_data
    schema: olist_data
    tables:
      - name: olist_orders
        columns:
          - name: order_id
            description: Unique order identifier
      - name: olist_customers
        columns:
          - name: customer_id
            description: Unique customer identifier
      - name: olist_order_items
        columns:
          - name: order_id
            description: Order identifier
      - name: olist_order_payments
        columns:
          - name: order_id
            description: Order identifier
      - name: olist_late_orders
        description: Orders with simulated late arrival times for testing ingestion time tracking
        columns:
          - name: order_id
            description: Order identifier
          - name: customer_id
            description: Customer identifier
          - name: order_purchase_timestamp
            description: When the order was actually purchased (transaction time)
          - name: order_approved_at
            description: When the order was approved/entered our system (ingestion time)
EOF

        # Session 1 staging models
        cat > models/staging/stg_orders.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp as order_purchase_timestamp,
    order_approved_at::timestamp as order_approved_at,
    order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
    order_delivered_customer_date::timestamp as order_delivered_customer_date,
    order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
    DATE(order_purchase_timestamp) as order_date
FROM {{ source('olist_data', 'olist_orders') }}
WHERE order_status != 'unavailable'
  AND order_purchase_timestamp IS NOT NULL
EOF

        cat > models/staging/stg_customers.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('olist_data', 'olist_customers') }}
WHERE customer_id IS NOT NULL
EOF

        cat > models/staging/stg_order_items.sql << 'EOF'
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
EOF

        cat > models/staging/stg_order_payments.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    order_id,
    payment_sequential::int as payment_sequential,
    payment_type,
    payment_installments::int as payment_installments,
    payment_value::decimal(10,2) as payment_value
FROM {{ source('olist_data', 'olist_order_payments') }}
WHERE order_id IS NOT NULL
  AND payment_value > 0
EOF

        # Session 1 intermediate models
        cat > models/intermediate/int_customer_landing.sql << 'EOF'
{{ config(materialized='table') }}

WITH first_orders AS (
    SELECT
        o.customer_id,
        MIN(o.order_purchase_timestamp) as first_order_timestamp,
        DATE(MIN(o.order_purchase_timestamp)) as landing_date
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY o.customer_id
)

SELECT
    customer_id,
    first_order_timestamp,
    landing_date,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'landing_date']) }} as customer_unique_key
FROM first_orders
EOF

        cat > models/intermediate/int_customer_daily_features.sql << 'EOF'
{{ config(materialized='table') }}

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
      AND d.date_day <= '2018-10-31'::date
),

daily_payments AS (
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) as order_date,
        SUM(p.payment_value) as daily_payment_value,
        COUNT(DISTINCT o.order_id) as daily_order_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1, 2
)

SELECT
    cd.customer_id,
    cd.date,
    cd.landing_date,

    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_payment_value,

    cd.date - cd.landing_date as days_since_landing,

    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_orders,

    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as payment_7d,

    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as orders_7d,

    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as payment_14d,

    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id
        ORDER BY cd.date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as orders_14d

FROM customer_dates cd
LEFT JOIN daily_payments dp
    ON cd.customer_id = dp.customer_id
    AND cd.date = dp.order_date
EOF

        # Session 2 models
        cat > models/staging/stg_order_items_snapshot.sql << 'EOF'
{{ config(materialized='table') }}

SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price::DECIMAL(10,2) as price,
    oi.freight_value::DECIMAL(10,2) as freight_value,
    o.order_purchase_timestamp::timestamp as order_purchase_timestamp,
    DATE(o.order_purchase_timestamp) as order_date,
    o.order_status
FROM {{ source('olist_data', 'olist_order_items') }} oi
INNER JOIN {{ source('olist_data', 'olist_orders') }} o
    ON oi.order_id = o.order_id
WHERE oi.order_id IS NOT NULL
  AND oi.seller_id IS NOT NULL
  AND oi.price > 0
  AND o.order_status NOT IN ('canceled', 'unavailable')
  AND o.order_purchase_timestamp IS NOT NULL
EOF

        cat > models/staging/stg_late_orders.sql << 'EOF'
{{ config(materialized='table') }}

-- Staging model for late-arriving orders
-- Joins orders with payments to get transaction amounts
-- Includes both transaction time and ingestion time for point-in-time correctness

SELECT
    lo.order_id,
    lo.customer_id,
    lo.order_purchase_timestamp,
    DATE(lo.order_purchase_timestamp) as transaction_date,
    lo.order_approved_at as ingestion_timestamp,
    DATE(lo.order_approved_at) as ingestion_date,
    p.payment_value::DECIMAL(10,2) as payment_value,

    -- Calculate how late this order arrived (for analysis)
    (lo.order_approved_at - lo.order_purchase_timestamp) as arrival_delay

FROM {{ source('olist_data', 'olist_late_orders') }} lo
INNER JOIN {{ source('olist_data', 'olist_order_payments') }} p
    ON lo.order_id = p.order_id
WHERE lo.order_id IS NOT NULL
  AND lo.customer_id IS NOT NULL
  AND p.payment_value > 0
EOF

        cat > models/intermediate/int_seller_performance.sql << 'EOF'
{{ config(materialized='table') }}

{% set snapshot_date = var('snapshot_date', '2018-10-17') %}

WITH seller_metrics AS (
    SELECT
        seller_id,
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT product_id) as unique_products,
        SUM(price) as total_revenue,
        AVG(price) as avg_order_value,
        MIN(order_date) as first_sale_date,
        MAX(order_date) as last_sale_date,
        CURRENT_TIMESTAMP as updated_at
    FROM {{ ref('stg_order_items_snapshot') }}
    WHERE order_date <= '{{ snapshot_date }}'::date
    GROUP BY seller_id
)

SELECT
    seller_id,
    {{ classify_tier('total_orders', 'volume') }} as seller_tier,
    total_orders,
    unique_products,
    total_revenue,
    avg_order_value,
    first_sale_date,
    last_sale_date,
    updated_at
FROM seller_metrics
EOF

        cat > models/intermediate/int_customer_payment_features.sql << 'EOF'
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
EOF

        # Session 2: Incremental model WITH updated_at (final solution)
        cat > models/intermediate/int_customer_daily_features_inc.sql << 'EOF'
{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'date'],
    incremental_strategy='merge'
) }}

{% set max_date = var('max_date', '2018-10-31') %}

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
      
    {% if is_incremental() %}
      AND d.date_day >= (
        SELECT MAX(date) - INTERVAL '14 days'
        FROM {{ this }}
      )
    {% endif %}
),

daily_payments AS (
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) as order_date,
        SUM(p.payment_value) as daily_payment_value,
        COUNT(DISTINCT o.order_id) as daily_order_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    
    {% if is_incremental() %}
      AND DATE(o.order_purchase_timestamp) >= (
        SELECT MAX(date) - INTERVAL '14 days'
        FROM {{ this }}
      )
    {% endif %}
    
    GROUP BY 1, 2
)

SELECT
    cd.customer_id,
    cd.date,
    cd.landing_date,
    
    SUM(COALESCE(dp.daily_payment_value, 0)) OVER (
        PARTITION BY cd.customer_id 
        ORDER BY cd.date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_payment_value,
    
    SUM(COALESCE(dp.daily_order_count, 0)) OVER (
        PARTITION BY cd.customer_id 
        ORDER BY cd.date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_orders,
    
    cd.date - cd.landing_date as days_since_landing,
    
    -- Track when this row was processed (for verifying incremental behavior)
    CURRENT_TIMESTAMP as updated_at
    
FROM customer_dates cd
LEFT JOIN daily_payments dp
    ON cd.customer_id = dp.customer_id
    AND cd.date = dp.order_date
EOF

        # Session 3: Customer Tiers Model
        cat > models/intermediate/int_customer_tiers.sql << 'EOF'
{{ config(materialized='table') }}

WITH customer_metrics AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(p.payment_value) as total_revenue,
        AVG(p.payment_value) as avg_order_value,
        MIN(DATE(o.order_purchase_timestamp)) as first_order_date,
        MAX(DATE(o.order_purchase_timestamp)) as last_order_date
    FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY o.customer_id
)

SELECT
    customer_id,
    {{ classify_tier('total_orders', 'volume') }} as tier_by_volume,
    {{ classify_tier('total_revenue', 'revenue') }} as tier_by_revenue,
    total_orders,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    CASE
        WHEN {{ classify_tier('total_orders', 'volume') }} = 'platinum'
          OR {{ classify_tier('total_revenue', 'revenue') }} = 'platinum' THEN 'platinum'
        WHEN {{ classify_tier('total_orders', 'volume') }} = 'gold'
          OR {{ classify_tier('total_revenue', 'revenue') }} = 'gold' THEN 'gold'
        WHEN {{ classify_tier('total_orders', 'volume') }} = 'silver'
          OR {{ classify_tier('total_revenue', 'revenue') }} = 'silver' THEN 'silver'
        ELSE 'bronze'
    END as composite_tier
FROM customer_metrics
EOF

        cat > snapshots/snapshots.yml << 'EOF'
version: 2

snapshots:
  - name: snap_seller_tier
    relation: ref('int_seller_performance')
    config:
      unique_key: seller_id
      strategy: timestamp
      updated_at: updated_at
EOF

        # Seeds
        mkdir -p seeds
        cat > seeds/brazil_cities.csv << 'EOF'
city,state,region,population_tier,economic_zone,logistics_hub
São Paulo,SP,Southeast,mega,primary,1
Rio de Janeiro,RJ,Southeast,mega,primary,1
Brasília,DF,Central-West,large,primary,1
Salvador,BA,Northeast,large,secondary,1
Fortaleza,CE,Northeast,large,secondary,1
Belo Horizonte,MG,Southeast,large,secondary,1
Manaus,AM,North,large,tertiary,1
Curitiba,PR,South,large,secondary,1
Recife,PE,Northeast,large,secondary,1
Porto Alegre,RS,South,large,secondary,1
Belém,PA,North,medium,tertiary,0
Goiânia,GO,Central-West,medium,tertiary,0
Guarulhos,SP,Southeast,medium,primary,1
Campinas,SP,Southeast,medium,primary,1
São Luís,MA,Northeast,medium,tertiary,0
EOF

        # Tier Classification Macro
        mkdir -p macros
        cat > macros/classify_tier.sql << 'EOF'
{% macro classify_tier(metric_column, tier_type='volume') %}
    CASE
        {% if tier_type == 'volume' %}
            WHEN {{ metric_column }} >= 500 THEN 'platinum'
            WHEN {{ metric_column }} >= 100 THEN 'gold'
            WHEN {{ metric_column }} >= 20 THEN 'silver'
            ELSE 'bronze'
        {% elif tier_type == 'revenue' %}
            WHEN {{ metric_column }} >= 10000 THEN 'platinum'
            WHEN {{ metric_column }} >= 5000 THEN 'gold'
            WHEN {{ metric_column }} >= 1000 THEN 'silver'
            ELSE 'bronze'
        {% else %}
            WHEN {{ metric_column }} >= 500 THEN 'platinum'
            WHEN {{ metric_column }} >= 100 THEN 'gold'
            WHEN {{ metric_column }} >= 20 THEN 'silver'
            ELSE 'bronze'
        {% endif %}
    END
{% endmacro %}
EOF

        # Mart 1 - Customer Features Enriched
        mkdir -p models/mart
        cat > models/mart/mart_customer_features_enriched.sql << 'EOF'
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
        total_orders,
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
EOF

        # Mart 2 - Customer Behavior Daily
        cat > models/mart/mart_customer_behavior_daily.sql << 'EOF'
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
EOF

        echo "Step 4: Running dbt models..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run

        echo ""
        echo "Step 5: Loading seeds..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt seed

        echo ""
        echo "Step 6: Taking snapshots..."
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2017-01-01"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2017-06-30"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2018-01-31"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier
        $DOCKER_COMPOSE exec -T dbt-workshop dbt run --select int_seller_performance --vars '{"snapshot_date": "2018-10-17"}'
        $DOCKER_COMPOSE exec -T dbt-workshop dbt snapshot --select snap_seller_tier

        echo ""
        echo "✅ FINAL STATE setup complete!"
        echo "All solutions included:"
        echo "  - All Session 1, 2, and 3 models"
        echo "  - int_customer_daily_features_inc with updated_at column"
        echo "  - seeds/brazil_cities.csv"
        echo "  - macros/classify_tier.sql"
        echo "  - Snapshots with 4 iterations"
        echo "This represents the completed workshop state!"
        ;;

    *)
        echo "Invalid option: $SESSION"
        echo "Valid options: 1, 2, 3, end"
        exit 1
        ;;
esac

echo ""
echo "======================================"
echo "Database state summary:"
echo "======================================"
$DOCKER_COMPOSE exec -T postgres psql -U dbt_user -d dbt_workshop -c "
    SELECT
        CASE
            WHEN table_name LIKE 'stg_%' THEN 'Staging'
            WHEN table_name LIKE 'int_%' THEN 'Intermediate'
            WHEN table_name LIKE 'snap_%' THEN 'Snapshot'
            ELSE 'Other'
        END as layer,
        COUNT(*) as table_count
    FROM information_schema.tables
    WHERE table_schema = 'olist_data'
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE 'olist_%'
        AND table_name NOT LIKE 'product_%'
    GROUP BY 1
    ORDER BY 1;
"

if [ "$SESSION" = "end" ]; then
    echo ""
    echo "🎉 Workshop complete! All solutions are in place."
else
    echo ""
    echo "Ready to start Session $((SESSION + 1))!"
fi
