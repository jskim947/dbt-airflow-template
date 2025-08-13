{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw_data', 'products') }}
),

staged as (
    select
        product_id,
        name,
        description,
        price,
        category,
        stock_quantity,
        created_at,
        updated_at,
        -- Add data quality checks
        case
            when price > 0 then true
            else false
        end as is_valid_price,
        case
            when stock_quantity >= 0 then true
            else false
        end as is_valid_stock
    from source
)

select * from staged
