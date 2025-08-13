{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with stg_orders as (
    select * from {{ ref('stg_orders') }}
),

stg_order_items as (
    select * from {{ ref('stg_order_items') }}
),

stg_users as (
    select * from {{ ref('stg_users') }}
),

stg_products as (
    select * from {{ ref('stg_products') }}
),

fact_orders as (
    select
        o.order_id as order_key,
        o.user_id as user_key,
        oi.product_id as product_key,
        o.order_date,
        o.total_amount,
        o.status,
        oi.quantity,
        oi.unit_price,
        oi.total_price as item_total,
        o.created_at,
        o.updated_at,
        -- Add business logic
        case
            when o.status = 'completed' then true
            else false
        end as is_completed,
        case
            when o.total_amount > 100 then 'High Value'
            when o.total_amount > 50 then 'Medium Value'
            else 'Low Value'
        end as order_value_tier,
        -- Add calculated fields
        oi.quantity * oi.unit_price as calculated_item_total,
        case
            when oi.total_price = (oi.quantity * oi.unit_price) then true
            else false
        end as is_price_correct
    from stg_orders o
    inner join stg_order_items oi on o.order_id = oi.order_id
    inner join stg_users u on o.user_id = u.id
    inner join stg_products p on oi.product_id = p.product_id
)

select * from fact_orders
