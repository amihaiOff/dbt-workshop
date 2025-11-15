{% snapshot snap_payment_preference %}
    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='customer_id',
          updated_at='updated_at',
        )
    }}
    
    SELECT * FROM {{ ref('int_customer_payment_preference') }}
    
{% endsnapshot %}