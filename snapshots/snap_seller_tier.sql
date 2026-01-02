{% snapshot snap_seller_tier %}

{{
    config(
      target_schema='olist_data',
      strategy='timestamp',
      unique_key='seller_id',
      updated_at='updated_at'
    )
}}

select * from {{ ref('int_seller_performance') }}

{% endsnapshot %}
