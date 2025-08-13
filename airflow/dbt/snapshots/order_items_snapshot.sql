{% snapshot order_items_snapshot %}

{{
    config(
      target_database='airflow',
      target_schema='raw_data',
      unique_key='item_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('raw_data', 'order_items') }}

{% endsnapshot %}
