# 자동 데이터 타입 변환 시스템 (보안 강화 버전)

## 개요

이 시스템은 PostgreSQL 테이블 복사 시 데이터 타입을 자동으로 감지하고 적절한 변환을 적용하는 **보안이 강화된** 일반화된 솔루션입니다. SQL Injection 공격을 방지하고 PostgreSQL 공식 문서에 따른 모범 사례를 따릅니다.

## 🔒 **보안 강화 기능**

### 1. **SQL Injection 방지**
- 컬럼명과 테이블명에 대한 입력 검증
- 위험한 SQL 패턴 자동 감지 및 차단
- 안전한 식별자 처리 (따옴표 사용)

### 2. **PostgreSQL 표준 준수**
- 공식 데이터 타입 매핑 사용
- 식별자 명명 규칙 준수
- 예약어 충돌 방지

### 3. **입력 검증**
- 컬럼명 유효성 검사
- 테이블명 형식 검증
- WHERE 절 안전성 검증

## 주요 특징

### 1. **스키마 기반 자동 감지**
- 테이블 스키마를 자동으로 읽어서 모든 컬럼의 데이터 타입을 파악
- 컬럼별로 적절한 변환 규칙을 자동 적용

### 2. **PostgreSQL 공식 데이터 타입 매핑**
- **정수형**: `smallint`, `integer`, `bigint`, `serial`, `bigserial`
- **소수점형**: `decimal`, `numeric`, `real`, `double precision`
- **문자열형**: `character varying`, `varchar`, `text`, `char`, `character`, `uuid`, `json`, `jsonb`
- **날짜형**: `date`, `time`, `timestamp`, `interval`

### 3. **자동 변환 로직 (보안 강화)**

#### 숫자형 컬럼
```sql
CASE
    WHEN "column_name" IS NULL THEN NULL
    WHEN "column_name"::TEXT = '' THEN NULL
    WHEN "column_name"::TEXT ~ '^[0-9]+\.[0]+$' THEN CAST("column_name" AS BIGINT)::TEXT
    ELSE "column_name"::TEXT
END AS "column_name"
```

#### 문자열형 컬럼
```sql
CASE
    WHEN "column_name" IS NULL THEN NULL
    WHEN "column_name" = '' THEN NULL
    WHEN "column_name" ~ '^[0-9]+$' THEN CAST("column_name" AS BIGINT)::TEXT
    WHEN "column_name" ~ '^[0-9]{8}$' AND "column_name" ~ '^(19|20)[0-9]{6}$'
        THEN TO_CHAR(TO_DATE("column_name", 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE "column_name"
END AS "column_name"
```

## 보안 검증 규칙

### 1. **컬럼명 검증**
```python
# PostgreSQL 식별자 규칙 준수
valid_pattern = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# 예약어 충돌 방지
reserved_words = {'select', 'from', 'where', 'order', 'by', ...}
```

### 2. **테이블명 검증**
```python
# schema.table 형식 검증
# 각 부분이 유효한 식별자인지 확인
```

### 3. **WHERE 절 안전성 검증**
```python
# 위험한 패턴 자동 감지
dangerous_patterns = [
    r'\b(union|select|insert|update|delete|drop|create|alter)\b',
    r'--',  # SQL 주석
    r'/\*.*\*/',  # 블록 주석
    r'xp_',  # 확장 저장 프로시저
    r'exec\s*\(',  # 실행 함수
]
```

## 사용법

### 1. **테이블 설정 (보안 강화)**
```python
TABLES_CONFIG = [
    {
        "source": "m23.edi_690",  # 자동으로 검증됨
        "target": "raw_data.edi_690",
        "primary_key": ["eventcd", "eventid", "optionid", "serialid", "scexhid", "sedolid"],
        "sync_mode": "incremental_sync",
        "incremental_field": "changed",  # 자동으로 검증됨
        "incremental_field_type": "yyyymmdd",
        "custom_where": "changed >= '20250812'",  # 자동으로 검증됨
        "batch_size": 10000
        # column_transformations 설정 불필요!
    }
]
```

### 2. **자동 보안 검증**
- **컬럼명**: 자동으로 유효성 검사 및 예약어 충돌 방지
- **테이블명**: 스키마 형식 검증
- **WHERE 절**: 위험한 SQL 패턴 자동 감지
- **ORDER BY**: 컬럼명 안전성 검증

## 장점

### 1. **보안성**
- SQL Injection 공격 방지
- 입력값 자동 검증
- 위험한 패턴 자동 차단

### 2. **확장성**
- 새로운 테이블 추가 시 설정만 변경하면 됨
- 컬럼 변환 규칙을 별도로 정의할 필요 없음
- 보안 규칙이 자동으로 적용됨

### 3. **유지보수성**
- 데이터 타입 변환 로직이 한 곳에 집중
- 보안 검증 로직이 통합되어 관리 용이
- 새로운 변환 규칙 추가 시 `_apply_auto_transformation` 함수만 수정

### 4. **일관성**
- 모든 테이블에 동일한 변환 규칙 적용
- 모든 테이블에 동일한 보안 규칙 적용
- 테이블별 예외 처리 불필요

## 보안 모범 사례

### 1. **식별자 처리**
- 모든 컬럼명과 테이블명을 따옴표로 감싸기
- PostgreSQL 식별자 규칙 준수
- 예약어와의 충돌 방지

### 2. **입력 검증**
- 사용자 입력에 대한 철저한 검증
- 정규식을 통한 패턴 매칭
- 위험한 SQL 키워드 자동 감지

### 3. **에러 처리**
- 보안 위반 시 적절한 로깅
- 위험한 입력 감지 시 경고 메시지
- 실패 시 안전한 기본값 사용

## 예시

### Before (보안 취약)
```python
# SQL Injection 위험
select_sql = f"SELECT * FROM {table_name}"  # 위험!
where_clause = f"WHERE {user_input}"        # 위험!
```

### After (보안 강화)
```python
# 자동 보안 검증
select_sql = _build_safe_sql_query(table_config, source_schema, "select")
# 모든 입력이 자동으로 검증되고 안전하게 처리됨
```

## 새로운 테이블 추가 시

1. **TABLES_CONFIG에 테이블 정보만 추가**
2. **보안 검증은 자동으로 적용됨**
3. **별도의 컬럼별 설정 불필요**

```python
{
    "source": "new_schema.new_table",  # 자동 검증됨
    "target": "raw_data.new_table",
    "primary_key": ["id"],
    "sync_mode": "full_sync",
    "batch_size": 10000
    # 끝! 보안과 변환은 자동 처리
}
```

## 확장 가능성

### 새로운 변환 규칙 추가
`_apply_auto_transformation` 함수에 새로운 데이터 타입과 변환 로직을 추가하면 됩니다:

```python
elif data_type in ['json', 'jsonb']:
    # JSON 타입 특별 처리 (보안 강화)
    return f'COALESCE("{column_name}"::TEXT, \'\') AS "{column_name}"'
```

### 새로운 보안 규칙 추가
`_is_safe_where_clause` 함수에 추가 보안 패턴을 정의할 수 있습니다:

```python
# 추가 위험 패턴
additional_patterns = [
    r'waitfor\s+delay',  # 시간 지연 공격
    r'benchmark\s*\(',   # 성능 공격
]
```

이 시스템을 사용하면 테이블별로 필드를 하나씩 정의하는 번거로움 없이, **보안이 강화된** 일관된 데이터 타입 변환을 자동으로 처리할 수 있습니다.
