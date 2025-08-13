# Airflow DAGs for Data Pipeline

이 프로젝트는 재활용 가능한 구조로 설계된 Airflow DAG들을 포함합니다. 각 DAG는 특정 기능을 담당하며, Airflow Variables를 통해 설정을 관리합니다.

## DAG 구조

### 1. Variable Setup DAG (`variable_setup_dag.py`)
- **목적**: Airflow Variables에 테이블과 변수 설정을 저장
- **실행**: `@once` (수동 실행)
- **기능**:
  - 테이블별 복사 방법 설정 (full_copy, incremental, cdc)
  - 데이터베이스 연결 설정
  - dbt 설정 (스냅샷, 모델, 테스트)

### 2. Data Copy DAG (`data_copy_dag.py`)
- **목적**: Variable을 받아서 복사 방법별로 데이터 복사
- **실행**: `@daily`
- **복사 방법**:
  - **Full Copy**: 테이블 전체 복사 (truncate 후 insert)
  - **Incremental**: 증분 복사 (변경된 데이터만)
  - **CDC**: Change Data Capture (변경 추적)

### 3. DBT Processing DAG (`dbt_processing_dag.py`)
- **목적**: dbt 스냅샷, 모델, 테스트 등 dbt 처리
- **실행**: `@daily`
- **기능**:
  - 스냅샷 생성 및 관리
  - 모델 실행 (staging, marts)
  - 테스트 실행
  - 문서 생성

### 4. Main Orchestration DAG (`main_orchestration_dag.py`)
- **목적**: 전체 워크플로우 오케스트레이션
- **실행**: `@daily`
- **기능**:
  - 변수 존재 여부 확인
  - 설정 유효성 검증
  - 하위 DAG들 순차 실행

## 공통 설정 모듈

### `common/config.py`
- DAG 설정과 프로필 설정을 담당
- 재사용 가능한 설정 함수들 제공
- 기본값과 공통 설정 관리

## dbt 모델 구조

### Staging Layer
- `stg_users.sql`: 사용자 정보 스테이징
- `stg_orders.sql`: 주문 정보 스테이징
- `stg_products.sql`: 상품 정보 스테이징
- `stg_order_items.sql`: 주문 항목 스테이징

### Marts Layer
- `dim_users.sql`: 사용자 차원 테이블
- `dim_products.sql`: 상품 차원 테이블
- `fact_orders.sql`: 주문 팩트 테이블

### Snapshots
- `users_snapshot.sql`: 사용자 스냅샷
- `orders_snapshot.sql`: 주문 스냅샷
- `products_snapshot.sql`: 상품 스냅샷
- `order_items_snapshot.sql`: 주문 항목 스냅샷

## 사용 방법

### 1. 초기 설정
```bash
# Variable Setup DAG를 먼저 실행하여 필요한 설정 변수들을 생성
airflow dags trigger variable_setup_dag
```

### 2. 메인 파이프라인 실행
```bash
# 메인 오케스트레이션 DAG 실행
airflow dags trigger main_orchestration_dag
```

### 3. 개별 DAG 실행
```bash
# 데이터 복사만 실행
airflow dags trigger data_copy_dag

# dbt 처리만 실행
airflow dags trigger dbt_processing_dag
```

## 설정 변수

### table_configs
테이블별 복사 설정:
```json
{
  "users": {
    "source_schema": "public",
    "target_schema": "raw_data",
    "copy_method": "full_copy",
    "primary_key": "id",
    "batch_size": 1000,
    "enabled": true
  }
}
```

### copy_method_configs
복사 방법별 설정:
```json
{
  "full_copy": {
    "truncate_before_copy": true,
    "parallel_workers": 4,
    "timeout_minutes": 30
  }
}
```

### db_configs
데이터베이스 연결 설정:
```json
{
  "source_db": {
    "host": "source_postgres",
    "port": 5432,
    "database": "source_db",
    "connection_id": "source_postgres"
  }
}
```

### dbt_configs
dbt 처리 설정:
```json
{
  "snapshot_tables": ["users", "orders", "products", "order_items"],
  "models_to_run": ["staging", "marts"],
  "test_after_run": true
}
```

## 확장 방법

### 새로운 테이블 추가
1. `variable_setup_dag.py`의 `TABLE_CONFIGS`에 테이블 설정 추가
2. 필요한 경우 dbt 모델 생성
3. 스냅샷 설정 추가

### 새로운 복사 방법 추가
1. `data_copy_dag.py`에 새로운 복사 함수 구현
2. `copy_table()` 함수에 새로운 방법 추가
3. `copy_method_configs`에 설정 추가

### 새로운 dbt 모델 추가
1. `models/` 디렉토리에 새 모델 파일 생성
2. `sources.yml`에 소스 테이블 정의 추가
3. 필요한 경우 의존성 설정

## 모니터링 및 로깅

- 각 DAG는 상세한 로깅을 제공
- XCom을 통한 태스크 간 데이터 전달
- 에러 처리 및 재시도 로직 포함
- 성공/실패 상태 추적

## 주의사항

1. **데이터베이스 연결**: 소스 및 타겟 데이터베이스 연결이 올바르게 설정되어야 함
2. **권한**: dbt 실행을 위한 적절한 데이터베이스 권한 필요
3. **스키마**: 타겟 스키마가 존재하고 적절한 권한이 있어야 함
4. **변수 설정**: Variable Setup DAG를 먼저 실행하여 필요한 설정 변수들을 생성해야 함
