{% snapshot infomax_stock_master_snapshot %}

{{
    config(
      name='infomax_stock_master_snapshot',
      target_database='airflow',
      target_schema='snapshots',
      unique_key='pk_인포맥스종목마스터',
      strategy='check',
      check_cols=['국가코드', 'isin', '국문명', '영문명', '일자', '종가', '상장일', 'gts_티커', 'curr'],
      invalidate_hard_deletes=True
    )
}}

select
    *,
    인포맥스코드 || '|' || 팩셋거래소 || '|' || gts_exnm || '|' || 티커 as pk_인포맥스종목마스터
from {{ source('raw_data', '인포맥스종목마스터') }}

{% endsnapshot %}
