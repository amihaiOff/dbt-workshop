{{ config(materialized='table') }}

-- Use the RFM macro with different time periods
{{ calculate_rfm_metrics(
    analysis_date='2018-10-31',
    lookback_days=365
) }}