{% snapshot simple_test_snapshot %}

{{
    config(
      name='simple_test_snapshot',
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
