{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw_data', 'users') }}
),

staged as (
    select
        id,
        username,
        email,
        first_name,
        last_name,
        created_at,
        updated_at,
        -- Add data quality checks
        case
            when email is not null and email like '%@%' then true
            else false
        end as is_valid_email,
        case
            when username is not null and length(username) >= 3 then true
            else false
        end as is_valid_username
    from source
)

select * from staged
