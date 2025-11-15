{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('olist_data', 'olist_customers') }}
WHERE customer_id IS NOT NULL