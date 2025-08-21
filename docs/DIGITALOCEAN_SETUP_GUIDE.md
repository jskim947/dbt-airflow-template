# DigitalOcean PostgreSQL 연결 설정 및 활용 가이드

## 📋 개요

이 가이드는 Airflow에서 새로 추가된 DigitalOcean PostgreSQL 연결(`digitalocean_postgres`)을 활용하여 데이터를 복사하고 동기화하는 방법을 설명합니다.

## 🔗 연결 정보

- **연결 ID**: `digitalocean_postgres`
- **호스트**: 157.230.246.12
- **포트**: 5432
- **데이터베이스 타입**: PostgreSQL
- **설명**: DigitalOcean Cloud 서버

## 🛠️ 설정 단계

### 1. Airflow Connection 설정 (권장)

Airflow UI에서 다음 정보로 연결을 설정합니다:

```bash
# 연결 ID
digitalocean_postgres

# 연결 타입
PostgreSQL

# 호스트
157.230.246.12

# 스키마
[데이터베이스명]

# 포트
5432

# 로그인
[사용자명]

# 비밀번호
[비밀번호]
```

**중요**: 외부 데이터베이스 연결은 Airflow Connection을 통해 관리하는 것이 권장됩니다.

### 2. 환경변수 설정 (.env 파일)

`.env` 파일에는 Docker Compose 내부 데이터베이스 정보만 설정합니다:

```bash
# Docker Compose 환경변수 설정 (내부 DB만)
POSTGRES_HOST=10.150.2.150
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_PORT=15432

# 내부 DB 연결 ID (기본값)
SOURCE_POSTGRES_CONN_ID=fs2_postgres
TARGET_POSTGRES_CONN_ID=postgres_default
AIRFLOW_DB_CONN_ID=airflow_db

# 외부 DB 연결은 .env에 설정하지 않음 - Airflow Connection 사용
```

### 3. 연결 우선순위

코드에서 연결 정보를 가져올 때의 우선순위:

1. **Airflow Connection** (최우선) - 외부 DB 연결
2. **Airflow Variables** (두 번째) - 설정 정보
3. **환경변수** (세 번째) - 내부 DB 정보
4. **기본값** (최후) - 하드코딩된 기본값

### 4. 연결 테스트

연결이 제대로 설정되었는지 확인:

```python
from common.connection_manager import ConnectionManager

# 연결 테스트
is_connected = ConnectionManager.test_connection("digitalocean_postgres")
if is_connected:
    print("✅ DigitalOcean PostgreSQL 연결 성공!")
else:
    print("❌ DigitalOcean PostgreSQL 연결 실패!")

# 연결 정보 확인
conn_info = ConnectionManager.get_connection_info("digitalocean_postgres")
print(f"연결 정보: {conn_info}")
```

## 🚀 활용 방법

### 1. 중앙 집중식 설정 관리

프로젝트에서는 `variable_setup_dag.py`를 통해 모든 DAG의 설정을 중앙에서 관리합니다:

```python
# DAG 설정 가져오기
from common.dag_config_manager import DAGConfigManager

# 특정 DAG 설정 가져오기
dag_config = DAGConfigManager.get_dag_config("digitalocean_data_copy_dag")

# 연결 정보 가져오기
source_conn = dag_config.get("source_connection")
target_conn = dag_config.get("target_connection")

# 테이블 설정 가져오기
table_configs = DAGConfigManager.get_table_configs("digitalocean_data_copy_dag")

# 모니터링 설정 가져오기
monitoring_config = DAGConfigManager.get_monitoring_config()
```

### 2. 설정 변수 확인

Airflow Variables에서 다음 설정들을 확인할 수 있습니다:

- **`dag_configs`**: 모든 DAG의 설정 정보
- **`connection_configs`**: 연결 메타데이터 (설명, 사용량, 의존성 등)
- **`table_configs`**: 테이블별 복사 설정
- **`execution_monitoring_configs`**: 모니터링 및 알림 설정
- **`chunk_mode_configs`**: 청크 방식 처리 설정
- **`dbt_configs`**: dbt 관련 설정

**중요**: `connection_configs`는 연결의 메타데이터만 포함하며, 실제 연결 정보(호스트, 포트, 비밀번호 등)는 Airflow Connection에서 관리됩니다.

### 3. 연결 정보와 메타데이터 구분

#### 연결 정보 (Airflow Connection에서 관리)
```python
from common.connection_manager import ConnectionManager

# 실제 연결 정보 가져오기
conn_info = ConnectionManager.get_connection_info("digitalocean_postgres")
print(f"Host: {conn_info['host']}")
print(f"Port: {conn_info['port']}")
print(f"Database: {conn_info['database']}")
```

#### 연결 메타데이터 (Variable에서 관리)
```python
# 연결 메타데이터 가져오기
metadata = ConnectionManager.get_connection_metadata("digitalocean_postgres")
print(f"Description: {metadata['description']}")
print(f"Usage Count: {metadata['usage_count']}")
print(f"DAG Dependencies: {metadata['dag_dependencies']}")
```

### 4. 단일 방향 데이터 복사 (DigitalOcean → 로컬)

`digitalocean_data_copy_dag.py`를 사용하여 DigitalOcean PostgreSQL에서 로컬 PostgreSQL로 데이터를 복사합니다.

**사용 시나리오**:
- DigitalOcean의 운영 데이터를 로컬 분석 환경으로 복사
- 클라우드 데이터를 로컬에서 백업 및 분석
- 개발/테스트 환경에 프로덕션 데이터 복사

**설정 예시**:
```python
DIGITALOCEAN_TABLES_CONFIG = [
    {
        "source": "public.users",
        "target": "raw_data.digitalocean_users",
        "primary_key": ["id"],
        "sync_mode": "full_sync",
        "batch_size": 10000,
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 3,
        "description": "DigitalOcean 사용자 테이블 - 전체 동기화"
    }
]
```

### 2. 하이브리드 데이터 복사 (DigitalOcean → 다중 타겟)

`hybrid_data_copy_dag.py`를 사용하여 DigitalOcean PostgreSQL에서 여러 타겟 데이터베이스로 동시에 데이터를 복사합니다.

**사용 시나리오**:
- 하나의 소스에서 여러 환경으로 데이터 분산
- 데이터 웨어하우스 구축 시 다중 타겟 동기화
- 백업 및 분석 환경 동시 구축

**설정 예시**:
```python
HYBRID_TABLES_CONFIG = [
    {
        "source": "public.users",
        "targets": [
            {
                "conn_id": "postgres_default",
                "table": "raw_data.digitalocean_users_local",
                "sync_mode": "full_sync",
                "batch_size": 10000,
                "description": "로컬 PostgreSQL로 복사"
            },
            {
                "conn_id": "fs2_postgres",
                "table": "raw_data.digitalocean_users_fs2",
                "sync_mode": "incremental_sync",
                "batch_size": 5000,
                "description": "팩셋 데이터베이스로 복사"
            }
        ],
        "primary_key": ["id"],
        "incremental_field": "updated_at",
        "incremental_field_type": "timestamp",
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 3,
        "source_description": "DigitalOcean 사용자 테이블"
    }
]
```

### 3. 기존 DAG 수정

기존 DAG에서 DigitalOcean PostgreSQL을 소스나 타겟으로 사용하려면:

```python
# 소스로 사용
SOURCE_CONN_ID = "digitalocean_postgres"

# 타겟으로 사용
TARGET_CONN_ID = "digitalocean_postgres"
```

## 📊 동기화 모드

### 1. 전체 동기화 (Full Sync)
- 소스 테이블의 모든 데이터를 타겟으로 복사
- 기존 타겟 데이터를 완전히 교체
- 초기 데이터 로드나 정기적인 전체 갱신에 적합

```python
"sync_mode": "full_sync"
```

### 2. 증분 동기화 (Incremental Sync)
- 변경된 데이터만 타겟으로 복사
- 기존 데이터 유지하면서 새 데이터 추가/업데이트
- 대용량 테이블의 효율적인 동기화에 적합

```python
"sync_mode": "incremental_sync",
"incremental_field": "updated_at",
"incremental_field_type": "timestamp"
```

### 3. XMIN 기반 증분 동기화
- PostgreSQL의 XMIN 시스템 컬럼을 활용한 증분 동기화
- 트랜잭션 기반의 정확한 변경 감지
- 대용량 테이블의 고성능 증분 동기화에 적합

```python
"sync_mode": "xmin_incremental"
```

## 🔧 고급 설정

### 1. 청크 방식 처리
대용량 테이블을 안전하게 처리하기 위한 청크 방식 설정:

```python
"chunk_mode": True,           # 청크 방식 활성화
"enable_checkpoint": True,    # 체크포인트 활성화
"batch_size": 10000,         # 배치 크기
"max_retries": 5             # 최대 재시도 횟수
```

### 2. 커스텀 WHERE 조건
특정 조건에 맞는 데이터만 복사:

```python
"custom_where": "status = 'active' AND created_at >= '2024-01-01'"
```

### 3. 성능 최적화
메모리 및 성능 최적화 설정:

```python
"batch_size": 20000,         # 큰 배치 크기로 성능 향상
"parallel_workers": 4,       # 병렬 워커 수
"memory_limit_mb": 2048      # 메모리 제한
```

## 📈 모니터링 및 알림

### 1. 로그 모니터링
각 단계별 상세한 로그 확인:

```bash
# Airflow 로그 확인
airflow tasks logs [DAG_ID] [TASK_ID] [EXECUTION_DATE]
```

### 2. 성능 메트릭
복사 성능 및 리소스 사용량 모니터링:

- 복사 속도 (행/초)
- 메모리 사용량
- 처리 시간
- 오류율

### 3. 알림 설정
실패 시 이메일 알림 설정:

```python
default_args = {
    "email_on_failure": True,
    "email": ["admin@example.com", "data-team@example.com"]
}
```

## 🚨 문제 해결

### 1. 연결 오류
- 방화벽 설정 확인
- 네트워크 연결 상태 확인
- 사용자 권한 확인

### 2. 권한 오류
- 테이블 읽기 권한 확인
- 스키마 생성 권한 확인
- 데이터 삽입/업데이트 권한 확인

### 3. 성능 문제
- 배치 크기 조정
- 청크 방식 활성화
- 병렬 처리 설정

## 📚 추가 리소스

- [PostgreSQL 공식 문서](https://www.postgresql.org/docs/)
- [Airflow PostgreSQL Provider 문서](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/)
- [dbt PostgreSQL 어댑터 문서](https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile)

## 🔄 업데이트 내역

- **2024-01-XX**: 초기 가이드 작성
- **2024-01-XX**: DigitalOcean 연결 추가
- **2024-01-XX**: 하이브리드 DAG 추가

---

이 가이드를 통해 DigitalOcean PostgreSQL 연결을 효과적으로 활용하여 데이터 파이프라인을 구축할 수 있습니다. 