# DBT-Airflow Template 사용 튜토리얼

이 튜토리얼은 DBT-Airflow Template을 사용하여 PostgreSQL 데이터베이스 간 데이터 복사 및 변환 파이프라인을 구축하는 방법을 단계별로 설명합니다.

## 📋 목차

1. [환경 설정](#1-환경-설정)
2. [데이터베이스 연결 설정](#2-데이터베이스-연결-설정)
3. [첫 번째 데이터 복사 파이프라인 실행](#3-첫-번째-데이터-복사-파이프라인-실행)
4. [DBT 통합 및 스냅샷 생성](#4-dbt-통합-및-스냅샷-생성)
5. [증분 동기화 설정](#5-증분-동기화-설정)
6. [모니터링 및 알림 설정](#6-모니터링-및-알림-설정)
7. [문제 해결](#7-문제-해결)
8. [고급 기능](#8-고급-기능)

## 1. 환경 설정

### 1.1 시스템 요구사항

- **Python**: 3.12 이상
- **PostgreSQL**: 12 이상
- **메모리**: 최소 8GB RAM (권장 16GB)
- **디스크**: 최소 10GB 여유 공간

### 1.2 프로젝트 클론 및 설정

```bash
# 저장소 클론
git clone <repository-url>
cd dbt-airflow-template

# 가상환경 생성 및 활성화
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# 또는
.venv\Scripts\activate     # Windows

# 의존성 설치
pip install -r requirements.txt
```

### 1.3 환경 변수 설정

```bash
# .env 파일 생성
cp .env.example .env

# .env 파일 편집
nano .env
```

`.env` 파일 내용:
```env
# 데이터베이스 연결 정보
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=5432
SOURCE_DB_NAME=source_db
SOURCE_DB_USER=source_user
# 환경변수 설정
SOURCE_DB_PASSWORD=your_source_password_here

TARGET_DB_HOST=localhost
TARGET_DB_PORT=5432
TARGET_DB_NAME=target_db
TARGET_DB_USER=target_user
# 데이터베이스 연결 예시
TARGET_DB_PASSWORD=your_target_password_here

# Airflow 설정
AIRFLOW_HOME=./airflow
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost:5432/airflow

# DBT 설정
DBT_PROJECT_PATH=./airflow/dbt
DBT_PROFILES_DIR=./airflow/dbt
```

## 2. 데이터베이스 연결 설정

### 2.1 Airflow 연결 설정

Airflow 웹 UI에서 데이터베이스 연결을 설정합니다:

1. **Airflow 웹 UI 접속**: http://localhost:8080
2. **Admin → Connections** 메뉴로 이동
3. **+ 버튼** 클릭하여 새 연결 추가

#### 소스 데이터베이스 연결
```
Connection Id: source_postgres
Connection Type: Postgres
Host: localhost
Schema: source_db
Login: source_user
Password: your_source_password_here
Port: 5432
```

#### 타겟 데이터베이스 연결
```
Connection Id: target_postgres
Connection Type: Postgres
Host: localhost
Schema: target_db
Login: target_user
Password: your_target_password_here
Port: 5432
```

### 2.2 연결 테스트

```bash
# Airflow CLI로 연결 테스트
airflow connections test source_postgres
airflow connections test target_postgres
```

## 3. 첫 번째 데이터 복사 파이프라인 실행

### 3.1 테이블 설정 구성

`airflow/dags/postgres_data_copy_dag.py` 파일에서 테이블 설정을 수정합니다:

```python
# 테이블 설정 예시
TABLE_CONFIGS = [
    {
        "source": "public.users",           # 소스 테이블
        "target": "raw_data.users",         # 타겟 테이블
        "primary_key": ["id"],              # 기본키
        "sync_mode": "full_sync",           # 동기화 모드
        "batch_size": 10000,                # 배치 크기
        "enabled": True                     # 활성화 여부
    }
]
```

### 3.2 DAG 활성화 및 실행

1. **Airflow 웹 UI에서 DAG 활성화**
   - `postgres_multi_table_copy` DAG 찾기
   - **Toggle** 버튼 클릭하여 활성화

2. **수동 실행**
   - **Trigger DAG** 버튼 클릭
   - 실행 날짜 선택 후 **Trigger** 클릭

3. **실행 상태 모니터링**
   - **Graph View**에서 태스크별 실행 상태 확인
   - **Log** 버튼으로 상세 로그 확인

### 3.3 실행 결과 확인

```bash
# Airflow 로그 확인
airflow tasks logs postgres_multi_table_copy copy_data_with_dynamic_sql 2024-01-01T00:00:00

# 데이터베이스에서 결과 확인
psql -h localhost -U target_user -d target_db -c "SELECT COUNT(*) FROM raw_data.users;"
```

## 4. DBT 통합 및 스냅샷 생성

### 4.1 DBT 프로젝트 초기화

```bash
# DBT 프로젝트 디렉토리로 이동
cd airflow/dbt

# DBT 프로젝트 초기화 (이미 초기화되어 있음)
# dbt init dbt_airflow_template

# DBT 프로필 설정
dbt debug
```

### 4.2 DBT 스냅샷 설정

`airflow/dbt/snapshots/users_snapshot.sql` 파일 생성:

```sql
{% snapshot users_snapshot %}

{{
    config(
      target_schema='snapshots',
      strategy='timestamp',
      unique_key='id',
      updated_at='updated_at',
    )
}}

select * from {{ source('raw_data', 'users') }}

{% endsnapshot %}
```

### 4.3 DBT 스냅샷 실행

```bash
# DBT 스냅샷 실행
dbt snapshot

# 또는 Airflow DAG를 통한 실행
airflow dags trigger dbt_processing
```

### 4.4 스냅샷 결과 확인

```sql
-- 스냅샷 테이블 확인
SELECT 
    id,
    name,
    email,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM snapshots.users_snapshot
ORDER BY id, dbt_valid_from;
```

## 5. 증분 동기화 설정

### 5.1 증분 동기화 테이블 설정

```python
TABLE_CONFIGS = [
    {
        "source": "public.orders",
        "target": "raw_data.orders",
        "primary_key": ["order_id"],
        "sync_mode": "incremental_sync",           # 증분 동기화
        "incremental_field": "updated_at",         # 증분 필드
        "incremental_field_type": "timestamp",     # 필드 타입
        "batch_size": 5000,
        "enabled": True
    }
]
```

### 5.2 증분 동기화 로직

```python
def build_incremental_filter(incremental_field: str, incremental_field_type: str) -> str:
    """
    증분 동기화를 위한 WHERE 조건 생성
    """
    if incremental_field_type == "timestamp":
        return f"{incremental_field} > (SELECT MAX({incremental_field}) FROM target_table)"
    elif incremental_field_type == "yyyymmdd":
        return f"{incremental_field} >= '20240101'"
    else:
        return f"{incremental_field} > '2024-01-01'"
```

### 5.3 증분 동기화 테스트

```bash
# 첫 번째 실행 (전체 데이터)
airflow dags trigger postgres_multi_table_copy

# 데이터 수정 후 두 번째 실행 (증분 데이터만)
# 소스 테이블에서 일부 데이터 수정
psql -h localhost -U source_user -d source_db -c "
UPDATE public.orders 
SET updated_at = CURRENT_TIMESTAMP 
WHERE order_id = 1;
"

# 증분 동기화 실행
airflow dags trigger postgres_multi_table_copy
```

## 6. 모니터링 및 알림 설정

### 6.1 로그 레벨 설정

`airflow.cfg` 파일에서 로그 레벨 설정:

```ini
[logging]
logging_level = INFO
fab_logging_level = WARN
logging_config_class = 
colored_console_log = True
colored_log_format = [%%(blue)s%%(asctime)s%%(reset)s] {%%(blue)s%%(filename)s:%%(reset)s%%(lineno)d} %%(log_color)s%%(levelname)s%%(reset)s - %%(message)s
colored_formatter_class = airflow.utils.log.colored_log.CustomTTYColoredFormatter
log_format = [%%(asctime)s] {%%(filename)s:%%(lineno)d} %%(levelname)s - %%(message)s
simple_log_format = %%(asctime)s %%(levelname)s - %%(message)s
```

### 6.2 Slack 알림 설정

```python
# Slack 웹훅 URL 설정
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL", default_var="")

def send_slack_notification(message: str, channel: str = "#data-pipeline"):
    """
    Slack으로 알림 전송
    """
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack 웹훅 URL이 설정되지 않음")
        return
    
    payload = {
        "text": message,
        "channel": channel,
        "username": "Data Pipeline Bot"
    }
    
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    response.raise_for_status()
```

### 6.3 성능 메트릭 수집

```python
import time
import psutil

def collect_performance_metrics():
    """
    성능 메트릭 수집
    """
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    
    # 작업 수행
    # ...
    
    end_time = time.time()
    end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    
    execution_time = end_time - start_time
    memory_usage = end_memory - start_memory
    
    logger.info(f"성능 메트릭:")
    logger.info(f"  - 실행 시간: {execution_time:.2f}초")
    logger.info(f"  - 메모리 사용량: {memory_usage:.2f}MB")
    
    return {
        "execution_time": execution_time,
        "memory_usage": memory_usage
    }
```

## 7. 문제 해결

### 7.1 일반적인 문제들

#### 데이터베이스 연결 실패

```bash
# 연결 테스트
psql -h localhost -U username -d database -c "SELECT 1;"

# 방화벽 설정 확인
sudo ufw status
sudo ufw allow 5432

# PostgreSQL 서비스 상태 확인
sudo systemctl status postgresql
```

#### 권한 문제

```sql
-- 사용자 권한 확인
SELECT usename, usecreatedb, usesuper, usebypassrls 
FROM pg_user 
WHERE usename = 'your_username';

-- 테이블 권한 확인
SELECT grantee, privilege_type 
FROM information_schema.role_table_grants 
WHERE table_name = 'your_table';
```

#### 메모리 부족

```python
# 배치 크기 줄이기
table_config["batch_size"] = 1000  # 기본값 10000에서 줄임

# 메모리 사용량 모니터링
import psutil
memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  # MB
logger.info(f"현재 메모리 사용량: {memory_usage:.2f}MB")
```

### 7.2 디버깅 방법

#### 상세 로그 확인

```bash
# Airflow 태스크 로그
airflow tasks logs <dag_id> <task_id> <execution_date>

# DBT 로그
dbt run --log-level debug

# Python 스크립트 디버깅
python -m pdb your_script.py
```

#### 데이터 검증

```python
def validate_data_integrity(source_table: str, target_table: str):
    """
    데이터 무결성 검증
    """
    # 행 수 비교
    source_count = get_table_row_count(source_table)
    target_count = get_table_row_count(target_table)
    
    logger.info(f"소스 테이블 행 수: {source_count}")
    logger.info(f"타겟 테이블 행 수: {target_count}")
    
    if source_count != target_count:
        logger.error(f"행 수 불일치: 소스 {source_count}, 타겟 {target_count}")
        return False
    
    # 샘플 데이터 비교
    sample_size = min(1000, source_count)
    source_sample = get_table_sample(source_table, sample_size)
    target_sample = get_table_sample(target_table, sample_size)
    
    # 데이터 비교 로직
    # ...
    
    return True
```

## 8. 고급 기능

### 8.1 사용자 정의 SQL

복잡한 데이터 변환이 필요한 경우 사용자 정의 SQL을 사용할 수 있습니다:

```python
table_config = {
    "source": "public.complex_table",
    "target": "raw_data.complex_table",
    "primary_key": ["id"],
    "sync_mode": "full_sync",
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
        FROM public.complex_table
        WHERE status = 'active'
    """,
    "custom_count": "SELECT COUNT(*) FROM public.complex_table WHERE status = 'active'"
}
```

### 8.2 병렬 처리

여러 테이블을 병렬로 처리하여 성능을 향상시킬 수 있습니다:

```python
from multiprocessing import Pool, cpu_count

def parallel_table_processing(table_configs: list[dict], max_workers: int = None):
    """
    여러 테이블을 병렬로 처리
    """
    if max_workers is None:
        max_workers = min(cpu_count(), len(table_configs))
    
    with Pool(processes=max_workers) as pool:
        results = pool.map(process_single_table, table_configs)
    
    return results

def process_single_table(table_config: dict):
    """
    단일 테이블 처리
    """
    try:
        # 데이터 복사 실행
        result = copy_table_data(table_config)
        return {"table": table_config["source"], "status": "success", "result": result}
    except Exception as e:
        return {"table": table_config["source"], "status": "error", "error": str(e)}
```

### 8.3 자동 재시도

네트워크 불안정이나 일시적 오류에 대응하기 위한 자동 재시도 메커니즘:

```python
import time
from functools import wraps

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0):
    """
    지수 백오프를 사용한 재시도 데코레이터
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"{func.__name__} 실행 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                        logger.info(f"{delay}초 후 재시도...")
                        time.sleep(delay)
                    else:
                        logger.error(f"{func.__name__} 최종 실패: {e}")
            
            raise last_exception
        
        return wrapper
    return decorator

@retry_with_backoff(max_retries=3, base_delay=1.0)
def copy_data_with_retry(table_config: dict):
    """
    재시도 로직이 포함된 데이터 복사
    """
    return copy_data_with_dynamic_sql(table_config)
```

### 8.4 데이터 품질 검증

데이터 품질을 자동으로 검증하는 기능:

```python
def validate_data_quality(table_name: str, schema: dict):
    """
    데이터 품질 검증
    """
    quality_checks = []
    
    for column in schema["columns"]:
        col_name = column["name"]
        col_type = column["type"]
        
        # NULL 값 검증
        null_check = f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL"
        null_count = execute_query(null_check)
        
        if null_count > 0:
            quality_checks.append({
                "column": col_name,
                "check": "null_values",
                "status": "warning",
                "message": f"{null_count}개의 NULL 값 발견"
            })
        
        # 데이터 타입 검증
        if col_type in ["BIGINT", "INTEGER"]:
            decimal_check = f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE {col_name}::TEXT ~ '\\.'
            """
            decimal_count = execute_query(decimal_check)
            
            if decimal_count > 0:
                quality_checks.append({
                    "column": col_name,
                    "check": "decimal_in_integer",
                    "status": "error",
                    "message": f"{decimal_count}개의 소수점 값 발견"
                })
    
    return quality_checks
```

## 📚 추가 리소스

### 유용한 명령어

```bash
# Airflow DAG 상태 확인
airflow dags list

# 특정 DAG의 태스크 상태 확인
airflow tasks list postgres_multi_table_copy

# DAG 실행 이력 확인
airflow dags state postgres_multi_table_copy 2024-01-01T00:00:00

# DBT 모델 상태 확인
dbt ls

# DBT 의존성 그래프 생성
dbt deps
dbt list --select +model_name
```

### 모니터링 대시보드

- **Airflow UI**: http://localhost:8080
- **PostgreSQL 관리 도구**: pgAdmin, DBeaver
- **시스템 모니터링**: htop, iotop, nethogs

### 로그 분석

```bash
# 실시간 로그 모니터링
tail -f airflow/logs/dag_id/task_id/execution_date/attempt_number.log

# 에러 로그 검색
grep -r "ERROR" airflow/logs/

# 성능 로그 분석
grep -r "execution_time" airflow/logs/ | awk '{print $NF}' | sort -n
```

---

이 튜토리얼을 통해 DBT-Airflow Template의 기본 사용법을 익혔습니다. 
더 자세한 정보는 [아키텍처 문서](ARCHITECTURE.md)와 [프로젝트 README](../../README.md)를 참조하세요.

문제가 있거나 추가 질문이 있으시면 프로젝트 이슈 페이지에 문의해 주세요. 