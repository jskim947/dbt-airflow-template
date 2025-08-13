{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw_data', 'order_items') }}
),

staged as (
    select
        item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        total_price,
        created_at,
        updated_at,
        -- Add data quality checks
        case
            when quantity > 0 then true
            else false
        end as is_valid_quantity,
        case
            when unit_price > 0 then true
            else false
        end as is_valid_unit_price,
        case
            when total_price = (quantity * unit_price) then true
            else false
        end as is_price_calculated_correctly
    from source
)

select * from staged
