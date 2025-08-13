{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw_data', 'orders') }}
),

staged as (
    select
        order_id,
        user_id,
        order_date,
        total_amount,
        status,
        created_at,
        updated_at,
        -- Add data quality checks
        case
            when total_amount > 0 then true
            else false
        end as is_valid_amount,
        case
            when order_date <= current_date then true
            else false
        end as is_valid_date
    from source
)

select * from staged
