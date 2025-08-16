# Airflow DAGs

이 디렉토리는 PostgreSQL 데이터 복사 및 DBT 처리를 위한 Airflow DAG들을 포함합니다.

## 📋 DAG 목록

### 1. `postgres_data_copy_dag.py`
**메인 데이터 복사 DAG**

- **기능**: PostgreSQL 소스에서 타겟으로 데이터 복사
- **스케줄**: 매일 실행 (`@daily`)
- **주요 특징**:
  - 다중 테이블 순차 처리
  - 증분 동기화 지원
  - 자동 스키마 감지 및 생성
  - 데이터 무결성 검증
  - DBT 스냅샷 자동 생성

### 2. `postgres_data_copy_dag_refactored.py`
**리팩토링된 데이터 복사 DAG**

- **기능**: 개선된 구조와 모니터링을 갖춘 데이터 복사
- **스케줄**: 매일 오전 3시, 오후 4시 (`0 3,16 * * *`)
- **주요 특징**:
  - 모듈화된 구조
  - 향상된 에러 핸들링
  - 상세한 모니터링 및 체크포인트
  - DBT 파이프라인 통합

### 3. `dbt_processing_dag.py`
**DBT 처리 DAG**

- **기능**: DBT 모델, 스냅샷, 테스트 실행
- **스케줄**: 수동 실행 또는 의존성 기반
- **주요 특징**:
  - DBT 명령어 실행
  - 모델 간 의존성 관리
  - 테스트 결과 검증

### 4. `main_orchestration_dag.py`
**메인 오케스트레이션 DAG**

- **기능**: 전체 데이터 파이프라인 조율
- **스케줄**: 수동 실행
- **주요 특징**:
  - 데이터 복사 → DBT 처리 → 검증 순차 실행
  - 전체 워크플로우 상태 관리

### 5. `data_copy_dag.py`
**기본 데이터 복사 DAG**

- **기능**: 단순한 데이터 복사 작업
- **스케줄**: 수동 실행
- **주요 특징**:
  - 기본적인 데이터 복사 기능
  - 간단한 설정

### 6. `simple_postgres_copy_dag.py`
**간단한 PostgreSQL 복사 DAG**

- **기능**: 단일 테이블 복사
- **스케줄**: 수동 실행
- **주요 특징**:
  - 최소한의 설정으로 빠른 복사
  - 학습 및 테스트용

### 7. `variable_setup_dag.py`
**변수 설정 DAG**

- **기능**: Airflow 변수 및 연결 설정
- **스케줄**: 수동 실행
- **주요 특징**:
  - 환경별 설정 관리
  - 데이터베이스 연결 정보 설정

### 8. `cosmos_dbt_dag.py` / `cosmos_dbt_docs_dag.py`
**Cosmos DBT DAG들**

- **기능**: Cosmos를 사용한 DBT 실행
- **스케줄**: 수동 실행
- **주요 특징**:
  - Cosmos DBT 통합
  - 문서 생성

## 🏗️ 공통 모듈 (`common/`)

### `data_copy_engine.py`
데이터 복사 엔진의 핵심 로직을 담당합니다.

**주요 기능**:
- 스마트 스키마 감지
- 데이터 타입 자동 변환
- CSV 읽기/쓰기 최적화
- 배치 처리
- MERGE 작업 수행

**핵심 메서드**:
```python
def copy_data_with_custom_sql(self, source_table: str, target_table: str, ...)
def _validate_and_convert_data_types(self, df: pd.DataFrame, table_name: str)
def create_temp_table_and_import_csv(self, target_table: str, ...)
```

### `database_operations.py`
데이터베이스 작업을 위한 유틸리티 함수들을 제공합니다.

**주요 기능**:
- 테이블 스키마 조회
- 테이블 생성 및 검증
- 행 수 계산
- 데이터 무결성 검증

**핵심 메서드**:
```python
def get_table_schema(self, table_name: str) -> dict
def create_table_if_not_exists(self, target_table: str, source_schema: dict)
def verify_data_integrity(self, source_table: str, target_table: str, ...)
```

## ⚙️ 설정 및 사용법

### 기본 설정

각 DAG는 다음과 같은 설정을 지원합니다:

```python
table_config = {
    "source": "schema.table_name",           # 소스 테이블
    "target": "schema.table_name",           # 타겟 테이블
    "primary_key": ["id"],                   # 기본키
    "sync_mode": "incremental_sync",         # 동기화 모드
    "incremental_field": "updated_at",       # 증분 필드
    "incremental_field_type": "timestamp",   # 증분 필드 타입
    "batch_size": 10000,                     # 배치 크기
    "custom_where": "status = 'active'",     # 사용자 정의 WHERE 조건
    "custom_select": "SELECT * FROM ...",    # 사용자 정의 SELECT
    "custom_count": "SELECT COUNT(*) FROM ..." # 사용자 정의 COUNT
}
```

### 동기화 모드

1. **`full_sync`**: 전체 데이터 복사
   - 기존 데이터 삭제 후 새로 삽입
   - 데이터 무결성 보장

2. **`incremental_sync`**: 증분 데이터 복사
   - 변경된 데이터만 복사
   - 성능 최적화

### 증분 필드 타입

- **`timestamp`**: 타임스탬프 기반
- **`yyyymmdd`**: 날짜 기반 (YYYYMMDD 형식)
- **`date`**: 날짜 기반
- **`datetime`**: 날짜시간 기반

## 📊 모니터링 및 로깅

### 체크포인트 시스템

리팩토링된 DAG는 체크포인트 시스템을 통해 각 단계의 진행 상황을 추적합니다:

```python
monitoring.add_checkpoint("스키마 조회", "소스 테이블 스키마 조회 완료")
monitoring.add_checkpoint("데이터 복사", "데이터 복사 완료")
monitoring.add_checkpoint("DBT 실행", "DBT 스냅샷 생성 완료")
```

### 로그 레벨

- **INFO**: 일반적인 진행 상황
- **WARNING**: 주의가 필요한 상황
- **ERROR**: 오류 상황
- **DEBUG**: 상세한 디버깅 정보

## 🔧 고급 기능

### 사용자 정의 SQL

소스 데이터에 복잡한 변환이 필요한 경우 사용자 정의 SQL을 사용할 수 있습니다:

```python
table_config = {
    "custom_select": """
        SELECT 
            id,
            name,
            CASE 
                WHEN priority ~ '^[0-9]+\\.[0-9]+$' 
                THEN CAST(CAST(priority AS NUMERIC) AS BIGINT)::TEXT
                ELSE priority 
            END AS priority,
            updated_at
        FROM source_table
        WHERE status = 'active'
    """,
    "custom_count": "SELECT COUNT(*) FROM source_table WHERE status = 'active'"
}
```

### 데이터 타입 변환

엔진은 자동으로 데이터 타입을 변환합니다:

- **BIGINT/INTEGER**: 소수점 값 → 정수 변환
- **VARCHAR**: 빈 문자열 → NULL 변환
- **TIMESTAMP**: 날짜 형식 자동 변환

### 배치 처리 최적화

대용량 데이터 처리를 위한 배치 처리:

```python
# 배치 크기 조정
table_config["batch_size"] = 5000  # 메모리 사용량에 따라 조정

# 진행률 로깅
logger.info(f"INSERT 진행률: {total_inserted}/{len(df_reordered)}")
```

## 🚨 에러 핸들링

### 재시도 메커니즘

```python
def _verify_table_schema_with_retry(self, target_table: str, source_schema: dict, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            # 스키마 검증 로직
            return target_schema
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.warning(f"스키마 검증 실패 (시도 {attempt + 1}): {e}, {wait_time}초 후 재시도")
                time.sleep(wait_time)
            else:
                raise
```

### 상세한 에러 로깅

```python
logger.error(f"데이터 복사 실패: {source_table} -> {target_table}")
logger.error(f"오류 상세: {e}")
logger.error(f"컨텍스트: {context}")
```

## 📈 성능 최적화

### 메모리 사용량 최적화

1. **배치 크기 조정**: 메모리 사용량과 성능의 균형
2. **데이터프레임 청소**: 불필요한 컬럼 제거
3. **가비지 컬렉션**: 주기적인 메모리 정리

### 데이터베이스 연결 최적화

1. **연결 풀링**: 효율적인 연결 관리
2. **세션 격리**: 하나의 연결에서 여러 작업 수행
3. **트랜잭션 관리**: 적절한 커밋 시점

## 🔍 디버깅

### 로그 분석

```bash
# Airflow 로그 확인
airflow tasks logs postgres_multi_table_copy copy_data_with_dynamic_sql 2024-01-01T00:00:00

# 특정 태스크의 로그
airflow tasks logs postgres_multi_table_copy validate_data_integrity 2024-01-01T00:00:00
```

### 데이터 검증

```python
# 데이터 무결성 검증
validation_result = validate_data_integrity(table_config, **context)
logger.info(f"검증 결과: {validation_result}")
```

## 📚 추가 리소스

- [Airflow 공식 문서](https://airflow.apache.org/docs/)
- [DBT 공식 문서](https://docs.getdbt.com/)
- [PostgreSQL 공식 문서](https://www.postgresql.org/docs/)
- [프로젝트 메인 README](../../README.md)

---

**참고**: 이 DAG들은 프로덕션 환경에서 사용하기 전에 충분한 테스트가 필요합니다. 특히 데이터 크기와 성능 요구사항을 고려하여 설정을 조정하세요.
