{% snapshot test_snapshot %}

{{
    config(
      target_schema='snapshots',
      strategy='timestamp',
      unique_key='id',
      updated_at='updated_at'
    )
}}

select
    1 as id,
    'test' as name,
    current_timestamp as updated_at

{% endsnapshot %}
