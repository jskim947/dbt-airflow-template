# Business Models - 인포맥스 데이터 처리

## 현재 활성화된 모델
- `dim_infomax_stock_master.sql` - 인포맥스 종목 마스터 차원 테이블

## 향후 확장 가이드

### 새 테이블 추가 시
1. **모델 파일 생성**: `models/business/` 디렉토리에 새 모델 파일 생성
2. **태그 설정**: `+tags: ["business", "production", "infomax"]` 추가
3. **스키마 설정**: `+schema: "business"` 설정

### 예시: 새 종목 데이터 모델
```sql
{{
    config(
        materialized='table',
        schema='business',
        tags=['business', 'production', 'infomax', 'new_stock']
    )
}}

with source as (
    select * from {{ source('raw_data', '새종목테이블') }}
),

staged as (
    select
        -- 필요한 컬럼들
    from source
)

select * from staged
```

### 실행 방법
- **현재**: `dbt run --select tag:infomax` (인포맥스 관련 모델만)
- **향후**: `dbt run --select tag:new_stock` (새 종목 관련 모델만)
- **전체**: `dbt run` (모든 활성화된 모델)

### 태그 체계
- `infomax`: 인포맥스 관련 모델
- `new_stock`: 새 종목 관련 모델 (예시)
- `production`: 프로덕션 환경 모델
- `development`: 개발 중인 모델
