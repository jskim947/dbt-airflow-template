# Common Package for DAGs

이 패키지는 Airflow DAG에서 사용할 수 있는 공통 모듈들을 포함합니다.

## 📦 **모듈 구성**

### 1. **`database_operations.py`** - 데이터베이스 작업
- 데이터베이스 연결 관리
- 테이블 스키마 조회
- 테이블 생성 및 검증
- 데이터 무결성 검증

### 2. **`data_copy_engine.py`** - 데이터 복사 엔진
- CSV 내보내기/가져오기
- 임시 테이블 관리
- MERGE 작업 수행
- 데이터 타입 변환

### 3. **`dbt_integration.py`** - dbt 통합
- dbt 프로젝트 검증
- 스냅샷 실행
- 파이프라인 관리
- 오류 처리

### 4. **`monitoring.py`** - 모니터링 및 추적
- 진행 상황 추적
- 성능 메트릭 수집
- 알림 기능
- 로깅 관리

### 5. **`dag_utils.py`** - DAG 유틸리티
- DAG 설정 관리
- 연결 검증
- 태스크 팩토리
- 문서화 헬퍼

### 6. **`settings.py`** - 설정 관리
- DAG 설정
- 연결 정보
- 모니터링 설정
- 배치 처리 설정

## 🚀 **사용법**

### **기본 import**
```python
from common import (
    DatabaseOperations,
    DataCopyEngine,
    DBTIntegration,
    MonitoringManager,
    ProgressTracker,
    DAGConfigManager,
    ConnectionValidator,
    TaskFactory,
    DAGSettings,
    ConnectionManager,
)
```

### **1. DAG 설정 관리**
```python
# DAG 설정 생성
default_args = DAGConfigManager.get_default_args(
    owner="data_team",
    retries=3,
    retry_delay_minutes=10
)

# DAG 생성
dag = DAGConfigManager.create_dag(
    dag_id="my_data_copy_dag",
    description="데이터 복사 DAG",
    schedule_interval="0 2 * * *",
    tags=["data-copy", "postgres"]
)
```

### **2. 연결 검증**
```python
# 연결 검증기 생성
validator = ConnectionValidator("source_db", "target_db")

# 기본 연결 검증 (EDI DAG용)
result = validator.validate_connections_basic(**context)

# 고급 연결 검증 (기본 DAG용)
result = validator.validate_connections_advanced(**context)
```

### **3. 태스크 생성**
```python
# 일반 복사 태스크 생성
copy_tasks = TaskFactory.create_copy_tasks(
    table_configs=TABLES_CONFIG,
    copy_function=copy_table_data,
    dag=dag
)

# EDI 복사 태스크 생성
edi_tasks = TaskFactory.create_edi_copy_tasks(
    table_configs=EDI_TABLES_CONFIG,
    copy_function=copy_table_data,
    dag=dag
)
```

### **4. 설정 관리**
```python
# 연결 ID 가져오기
source_conn = ConnectionManager.get_source_connection_id()
target_conn = ConnectionManager.get_target_connection_id()

# dbt 설정 가져오기
dbt_settings = DAGSettings.get_dbt_settings()
dbt_path = dbt_settings["project_path"]

# 환경별 설정 가져오기
env_config = DAGSettings.get_environment_config()
batch_size = env_config["batch_size"]
```

### **5. 모니터링 및 추적**
```python
# 모니터링 시작
monitoring = MonitoringManager("테이블 복사")
monitoring.start_monitoring()

# 진행 상황 추적
progress = ProgressTracker(5, "테이블 복사")
progress.start_step("스키마 조회", "테이블 스키마 조회 중")

# 체크포인트 추가
monitoring.add_checkpoint("스키마 조회", "스키마 조회 완료")
progress.complete_step("스키마 조회")

# 모니터링 종료
monitoring.stop_monitoring("completed")
```

## 🔧 **실제 DAG에서 사용 예시**

### **기본 데이터 복사 DAG**
```python
from common import (
    DAGConfigManager,
    ConnectionValidator,
    TaskFactory,
    DatabaseOperations,
    DataCopyEngine,
    MonitoringManager,
    ProgressTracker,
)

# DAG 생성
dag = DAGConfigManager.create_dag(
    dag_id="data_copy_dag",
    description="데이터 복사 DAG",
    schedule_interval="0 2 * * *"
)

# 연결 검증 태스크
validate_task = PythonOperator(
    task_id="validate_connections",
    python_callable=ConnectionValidator(
        "source_db", "target_db"
    ).validate_connections_advanced,
    dag=dag
)

# 복사 태스크들 생성
copy_tasks = TaskFactory.create_copy_tasks(
    table_configs=TABLES_CONFIG,
    copy_function=copy_table_data,
    dag=dag
)

# 의존성 설정
validate_task >> copy_tasks
```

### **EDI 데이터 복사 DAG**
```python
from common import (
    DAGConfigManager,
    ConnectionValidator,
    TaskFactory,
    DAGSettings,
)

# DAG 생성
dag = DAGConfigManager.create_dag(
    dag_id="edi_copy_dag",
    description="EDI 데이터 복사 DAG",
    schedule_interval="0 9 * * *",
    tags=["edi", "data-copy"]
)

# EDI 테이블 설정 가져오기
edi_configs = DAGSettings.get_edi_table_configs()

# EDI 복사 태스크들 생성
edi_tasks = TaskFactory.create_edi_copy_tasks(
    table_configs=edi_configs,
    copy_function=copy_edi_table_data,
    dag=dag
)
```

## ⚙️ **환경 변수 설정**

### **기본 환경 변수**
```bash
# 데이터베이스 연결
SOURCE_POSTGRES_CONN_ID=fs2_postgres
TARGET_POSTGRES_CONN_ID=postgres_default
AIRFLOW_DB_CONN_ID=airflow_db

# dbt 설정
DBT_PROJECT_PATH=/opt/airflow/dbt
DBT_PROFILE_NAME=postgres_data_copy
DBT_TARGET_NAME=dev

# 환경 설정
AIRFLOW_ENV=production
ENABLE_MONITORING_NOTIFICATIONS=true
ALERT_EMAILS=admin@example.com,ops@example.com

# 배치 처리 설정
MAX_BATCH_SIZE=50000
PARALLEL_WORKERS=4
MEMORY_LIMIT_MB=2048
```

### **연결별 환경 변수**
```bash
# 소스 데이터베이스
SOURCE_POSTGRES_HOST=source_db.example.com
SOURCE_POSTGRES_PORT=5432
SOURCE_POSTGRES_DB=source_database

# 타겟 데이터베이스
POSTGRES_HOST=target_db.example.com
POSTGRES_PORT=5432
POSTGRES_DB=target_database
```

## 📊 **성능 최적화**

### **배치 크기 조정**
```python
from common import BatchSettings

# 테이블별 배치 크기 가져오기
batch_size = BatchSettings.get_table_specific_batch_size("인포맥스종목마스터")

# 환경별 배치 설정
batch_config = BatchSettings.get_batch_config()
max_batch_size = batch_config["max_batch_size"]
parallel_workers = batch_config["parallel_workers"]
```

### **모니터링 임계값 설정**
```python
from common import MonitoringSettings

# 모니터링 설정 가져오기
monitoring_config = MonitoringSettings.get_monitoring_config()
copy_time_threshold = monitoring_config["performance_thresholds"]["copy_time_seconds"]
memory_threshold = monitoring_config["performance_thresholds"]["memory_usage_mb"]
```

## 🧪 **테스트 및 검증**

### **단위 테스트**
```python
import pytest
from common import DatabaseOperations

def test_database_operations():
    db_ops = DatabaseOperations("test_source", "test_target")
    
    # 연결 테스트
    results = db_ops.test_connections()
    assert results["source"] == True
    assert results["target"] == True
```

### **통합 테스트**
```python
def test_connection_validator():
    validator = ConnectionValidator("test_source", "test_target")
    
    # 기본 연결 검증 테스트
    result = validator.validate_connections_basic()
    assert result["status"] == "success"
```

## 🔄 **마이그레이션 가이드**

### **기존 DAG에서 공통 모듈 사용으로 전환**

#### **1단계: import 문 변경**
```python
# 기존
from postgres_data_copy_dag import ensure_target_table_exists

# 변경 후
from common import DatabaseOperations
db_ops = DatabaseOperations(SOURCE_CONN_ID, TARGET_CONN_ID)
db_ops.ensure_target_table_exists(table_config, **context)
```

#### **2단계: 설정 하드코딩 제거**
```python
# 기존
SOURCE_CONN_ID = "fs2_postgres"
TARGET_CONN_ID = "postgres_default"

# 변경 후
from common import ConnectionManager
SOURCE_CONN_ID = ConnectionManager.get_source_connection_id()
TARGET_CONN_ID = ConnectionManager.get_target_connection_id()
```

#### **3단계: DAG 설정 통합**
```python
# 기존
default_args = {
    "owner": "data_team",
    "retries": 2,
    # ... 기타 설정
}

# 변경 후
from common import DAGConfigManager
default_args = DAGConfigManager.get_default_args(
    owner="data_team",
    retries=2
)
```

## 📝 **주의사항**

1. **의존성 관리**: 모든 공통 모듈은 독립적으로 작동해야 합니다.
2. **에러 처리**: 각 모듈은 적절한 에러 처리와 로깅을 포함해야 합니다.
3. **성능**: 공통 모듈은 성능에 영향을 주지 않아야 합니다.
4. **호환성**: 기존 DAG와의 호환성을 유지해야 합니다.

## 🤝 **기여 방법**

1. 새로운 공통 기능이 필요한 경우 이슈를 생성하세요.
2. 코드 변경 시 테스트를 포함하세요.
3. 문서화를 업데이트하세요.
4. 기존 DAG와의 호환성을 확인하세요. 