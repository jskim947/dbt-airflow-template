{{
    config(
        materialized='table',
        schema='business',
        tags=['business', 'production', 'dimension']
    )
}}

with source as (
    select * from {{ source('raw_data', '인포맥스종목마스터') }}
),

staged as (
    select
        인포맥스코드,
        팩셋거래소,
        gts_exnm,
        티커,
        국가코드,
        isin,
        국문명,
        영문명,
        일자,
        종가,
        상장일,
        gts_티커,
        curr,
        -- 데이터 품질 검증
        case
            when 인포맥스코드 is not null and length(인포맥스코드) > 0 then true
            else false
        end as is_valid_code,
        case
            when 팩셋거래소 is not null and length(팩셋거래소) > 0 then true
            else false
        end as is_valid_exchange,
        case
            when 종가 is not null and 종가 > 0 then true
            else false
        end as is_valid_price,
        -- 비즈니스 로직
        case
            when 국가코드 = 'US' then '미국'
            when 국가코드 = 'KR' then '한국'
            when 국가코드 = 'JP' then '일본'
            when 국가코드 = 'CN' then '중국'
            else '기타'
        end as 국가명,
        case
            when 종가 >= 100 then '고가주'
            when 종가 >= 50 then '중가주'
            else '저가주'
        end as 가격대분류
    from source
)

select * from staged
