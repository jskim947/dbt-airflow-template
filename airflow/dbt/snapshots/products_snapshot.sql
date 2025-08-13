{% snapshot products_snapshot %}

{{
    config(
      target_database='airflow',
      target_schema='raw_data',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('raw_data', 'products') }}

{% endsnapshot %}
