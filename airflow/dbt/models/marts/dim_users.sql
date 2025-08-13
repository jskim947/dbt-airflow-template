{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with stg_users as (
    select * from {{ ref('stg_users') }}
),

dim_users as (
    select
        id as user_key,
        username,
        email,
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        created_at,
        updated_at,
        is_valid_email,
        is_valid_username,
        -- Add business logic
        case
            when created_at >= current_date - interval '30 days' then 'New'
            when created_at >= current_date - interval '90 days' then 'Recent'
            else 'Established'
        end as user_segment
    from stg_users
)

select * from dim_users
