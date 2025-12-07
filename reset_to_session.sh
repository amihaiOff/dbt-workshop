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
#   3 - Session 3: Testing & Production (TBD)
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
    echo "Usage: ./reset_to_session.sh <session_number>"
    echo "Example: ./reset_to_session.sh 1"
    echo ""
    echo "Available sessions:"
    echo "  1 - Session 1: dbt Foundations (staging + intermediate models)"
    echo "  2 - Session 2: Advanced Patterns (variables + snapshots + macros + incremental)"
    echo "  3 - Session 3: Testing & Production (TBD)"
    exit 1
fi

echo "======================================"
echo "Resetting to end of Session $SESSION"
echo "======================================"
echo ""

# Clean up existing model files (but preserve schema.yml - we'll recreate it)
echo "Step 1: Cleaning up existing dbt model files..."
rm -f models/staging/stg_*.sql
rm -f models/intermediate/int_*.sql
rm -f models/mart/mart_*.sql
rm -f snapshots/*.sql
rm -f macros/calculate_*.sql
# Note: schema.yml will be recreated with source definitions

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

        cat > models/intermediate/int_seller_performance.sql << 'EOF'
{{ config(materialized='table') }}

-- Use variable to control analysis date for testing
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

        cat > snapshots/snap_seller_tier.sql << 'EOF'
{% snapshot snap_seller_tier %}
    {{
        config(
          target_schema='olist_data',
          strategy='timestamp',
          unique_key='seller_id',
          updated_at='updated_at',
        )
    }}

    SELECT * FROM {{ ref('int_seller_performance') }}

{% endsnapshot %}
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
        echo "Session 3 not yet implemented"
        exit 1
        ;;

    *)
        echo "Invalid session number: $SESSION"
        echo "Valid options: 1, 2, 3"
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

echo ""
echo "Ready to start Session $((SESSION + 1))!"
