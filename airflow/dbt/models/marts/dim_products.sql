{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with stg_products as (
    select * from {{ ref('stg_products') }}
),

dim_products as (
    select
        product_id as product_key,
        name as product_name,
        description,
        price,
        category,
        stock_quantity,
        created_at,
        updated_at,
        is_valid_price,
        is_valid_stock,
        -- Add business logic
        case
            when stock_quantity = 0 then 'Out of Stock'
            when stock_quantity <= 10 then 'Low Stock'
            when stock_quantity <= 50 then 'Medium Stock'
            else 'High Stock'
        end as stock_status,
        case
            when price < 10 then 'Budget'
            when price < 50 then 'Mid-Range'
            else 'Premium'
        end as price_tier
    from stg_products
)

select * from dim_products
