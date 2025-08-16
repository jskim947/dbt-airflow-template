# DBT-Airflow Template 아키텍처

## 🏗️ 전체 아키텍처 개요

이 프로젝트는 PostgreSQL 데이터베이스 간 데이터 복사 및 변환을 위한 종합적인 데이터 파이프라인 솔루션입니다.

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Source DB     │    │     Airflow     │    │   Target DB     │
│  (PostgreSQL)   │───▶│      DAGs       │───▶│  (PostgreSQL)   │
│                 │    │                 │    │                 │
│ • EDI_690       │    │ • Data Copy     │    │ • Raw Data      │
│ • Stock Master  │    │ • DBT Process   │    │ • Transformed   │
│ • Other Tables  │    │ • Validation    │    │ • Analytics     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │      DBT        │
                       │                 │
                       │ • Snapshots     │
                       │ • Models        │
                       │ • Tests         │
                       │ • Documentation │
                       └─────────────────┘
```

## 🔄 데이터 플로우

### 1. 데이터 수집 단계
```
Source DB → Airflow DAG → CSV Export → Target DB
```

### 2. 데이터 변환 단계
```
Target DB → DBT Snapshots → DBT Models → Analytics Layer
```

### 3. 검증 및 모니터링 단계
```
Data Validation → Quality Checks → Monitoring → Alerts
```

## 🎯 핵심 컴포넌트

### 1. 데이터 복사 엔진 (`DataCopyEngine`)

#### 주요 책임
- 소스 데이터베이스에서 데이터 추출
- CSV 파일을 통한 중간 저장
- 타겟 데이터베이스로 데이터 로드
- 데이터 타입 변환 및 검증

#### 핵심 메서드

```python
class DataCopyEngine:
    def copy_data_with_custom_sql(
        self, 
        source_table: str, 
        target_table: str, 
        primary_keys: list[str],
        sync_mode: str = "full_sync",
        custom_where: str = None,
        custom_select: str = None,
        custom_count: str = None,
        batch_size: int = 10000
    ) -> dict[str, Any]:
        """
        사용자 정의 SQL을 사용하여 데이터 복사 수행
        
        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            primary_keys: 기본키 컬럼 리스트
            sync_mode: 동기화 모드 (full_sync/incremental_sync)
            custom_where: 사용자 정의 WHERE 조건
            custom_select: 사용자 정의 SELECT 쿼리
            custom_count: 사용자 정의 COUNT 쿼리
            batch_size: 배치 크기
            
        Returns:
            실행 결과 딕셔너리
        """
```

#### 데이터 처리 파이프라인

1. **스키마 검증**
   ```python
   def _validate_and_convert_data_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
       """
       데이터프레임의 모든 컬럼에 대해 데이터 타입 검증 및 변환 수행
       """
   ```

2. **데이터 타입 변환**
   ```python
   def _convert_column_by_type(self, column_series: pd.Series, col_type: str, col_name: str) -> pd.Series:
       """
       컬럼 타입에 따라 데이터 변환 수행
       """
   ```

3. **임시 테이블 생성 및 데이터 로드**
   ```python
   def create_temp_table_and_import_csv(
       self, 
       target_table: str, 
       source_schema: dict[str, Any], 
       csv_path: str, 
       batch_size: int = 1000
   ) -> tuple[str, int]:
       """
       임시 테이블 생성 및 CSV 데이터 가져오기
       """
   ```

4. **MERGE 작업 수행**
   ```python
   def _execute_merge_operation(
       self, 
       source_table: str, 
       target_table: str, 
       primary_keys: list[str], 
       sync_mode: str
   ) -> dict[str, Any]:
       """
       MERGE 작업을 통한 데이터 동기화
       """
   ```

### 2. 데이터베이스 작업 모듈 (`DatabaseOperations`)

#### 주요 책임
- 데이터베이스 연결 관리
- 테이블 스키마 조회 및 생성
- 데이터 무결성 검증
- 성능 메트릭 수집

#### 핵심 메서드

```python
class DatabaseOperations:
    def get_table_schema(self, table_name: str) -> dict[str, Any]:
        """
        테이블의 스키마 정보 조회
        """
    
    def create_table_if_not_exists(self, target_table: str, source_schema: dict[str, Any]) -> None:
        """
        타겟 테이블이 없으면 소스 스키마 기반으로 생성
        """
    
    def verify_data_integrity(
        self, 
        source_table: str, 
        target_table: str, 
        primary_keys: list[str],
        sample_size: int = 1000
    ) -> dict[str, Any]:
        """
        소스와 타겟 테이블 간 데이터 무결성 검증
        """
```

### 3. 모니터링 시스템 (`Monitoring`)

#### 주요 책임
- 파이프라인 실행 상태 추적
- 체크포인트 관리
- 성능 메트릭 수집
- 에러 로깅 및 알림

#### 핵심 메서드

```python
class Monitoring:
    def __init__(self, dag_id: str, task_id: str):
        self.start_time = time.time()
        self.checkpoints = []
        self.status = "running"
    
    def add_checkpoint(self, step: str, message: str) -> None:
        """
        체크포인트 추가
        """
    
    def stop_monitoring(self, final_status: str) -> dict[str, Any]:
        """
        모니터링 종료 및 결과 반환
        """
```

## 🚀 Airflow DAG 구조

### 1. 메인 데이터 복사 DAG (`postgres_data_copy_dag.py`)

#### DAG 구조
```
start → connection_test → schema_validation → data_copy → 
merge_operation → data_validation → dbt_snapshot → cleanup → end
```

#### 주요 태스크

1. **`test_source_connection`**
   - 소스 데이터베이스 연결 테스트
   - 연결 정보 유효성 검증

2. **`test_target_connection`**
   - 타겟 데이터베이스 연결 테스트
   - 권한 및 접근 가능성 확인

3. **`get_source_data_count`**
   - 소스 테이블의 데이터 수 조회
   - 증분 동기화 조건 확인

4. **`get_source_schema`**
   - 소스 테이블 스키마 정보 조회
   - 컬럼 타입 및 제약조건 분석

5. **`create_target_table`**
   - 타겟 테이블 생성 또는 검증
   - 스키마 일치성 확인

6. **`copy_data_with_dynamic_sql`**
   - 동적 SQL을 사용한 데이터 복사
   - 배치 처리 및 진행률 모니터링

7. **`execute_merge_operation`**
   - MERGE 작업을 통한 데이터 동기화
   - 충돌 해결 및 데이터 무결성 보장

8. **`validate_data_integrity`**
   - 복사된 데이터의 무결성 검증
   - 샘플 데이터 비교 및 통계 수집

9. **`run_dbt_snapshot`**
   - DBT 스냅샷 실행
   - 데이터 변경사항 추적

10. **`cleanup_temp_table`**
    - 임시 테이블 정리
    - 리소스 정리 및 정리

### 2. 리팩토링된 데이터 복사 DAG (`postgres_data_copy_dag_refactored.py`)

#### 개선된 구조
- 모듈화된 태스크 함수
- 향상된 에러 핸들링
- 상세한 모니터링 및 체크포인트
- DBT 파이프라인 통합

#### 주요 태스크

1. **`copy_table_data`**
   ```python
   def copy_table_data(table_config: dict, **context) -> str:
       """
       개별 테이블 데이터 복사
       """
       try:
           # 모니터링 시작
           monitoring = Monitoring(context["dag"].dag_id, context["task"].task_id)
           
           # 1. 소스 스키마 조회
           source_schema = get_source_schema(table_config, **context)
           monitoring.add_checkpoint("스키마 조회", f"소스 테이블 스키마 조회 완료: {len(source_schema['columns'])}개 컬럼")
           
           # 2. 타겟 테이블 생성/검증
           create_target_table(table_config, source_schema, **context)
           monitoring.add_checkpoint("테이블 생성", "타겟 테이블 생성/검증 완료")
           
           # 3. 데이터 복사
           copy_result = copy_data_with_dynamic_sql(table_config, **context)
           monitoring.add_checkpoint("데이터 복사", f"데이터 복사 완료: {copy_result.get('exported_rows', 0)}행 처리")
           
           # 4. MERGE 작업
           merge_result = execute_merge_operation(table_config, **context)
           monitoring.add_checkpoint("MERGE 작업", f"MERGE 작업 완료: {merge_result.get('message', '')}")
           
           # 5. DBT 파이프라인 실행
           pipeline_result = run_dbt_pipeline(table_config, **context)
           monitoring.add_checkpoint("DBT 실행", f"DBT 파이프라인 완료: {pipeline_result.get('message', '')}")
           
           # 모니터링 종료
           monitoring.stop_monitoring("success")
           return f"테이블 {table_config['source']} 처리 완료"
           
       except Exception as e:
           error_msg = f"테이블 {table_config['source']} 처리 실패: {e}"
           logger.error(error_msg)
           monitoring.stop_monitoring("failed")
           raise Exception(error_msg)
   ```

2. **`run_dbt_pipeline`**
   ```python
   def run_dbt_pipeline(table_config: dict, **context) -> dict[str, Any]:
       """
       DBT 파이프라인 실행
       """
       try:
           # DBT 설정
           pipeline_config = {
               "run_snapshot": True,      # 스냅샷 실행
               "run_models": False,       # 모델 실행 (향후 확장)
               "run_tests": False,        # 테스트 실행 (향후 확장)
               "cleanup": True,           # 정리 작업
           }
           
           # DBT 명령어 실행
           dbt_result = execute_dbt_commands(pipeline_config, **context)
           
           return {
               "status": "success",
               "message": "DBT 파이프라인 실행 완료",
               "results": dbt_result
           }
           
       except Exception as e:
           return {
               "status": "error",
               "message": f"DBT 파이프라인 실행 실패: {e}",
               "results": None
           }
   ```

### 3. DBT 처리 DAG (`dbt_processing_dag.py`)

#### DAG 구조
```
start → dbt_snapshot → dbt_run → dbt_test → dbt_docs_generate → end
```

#### 주요 태스크

1. **`run_dbt_snapshot`**
   - DBT 스냅샷 실행
   - 데이터 변경사항 추적

2. **`run_dbt_models`**
   - DBT 모델 실행
   - 데이터 변환 및 집계

3. **`run_dbt_tests`**
   - DBT 테스트 실행
   - 데이터 품질 검증

4. **`generate_dbt_docs`**
   - DBT 문서 생성
   - 데이터 계보 및 메타데이터 관리

## 🔧 DBT 통합

### 1. 스냅샷 관리

#### 스냅샷 설정
```yaml
# dbt_project.yml
snapshots:
  +target_schema: snapshots
  +strategy: timestamp
  +updated_at: updated_at
  +unique_key: id
```

#### 스냅샷 실행
```python
def execute_dbt_snapshot(table_name: str, **context) -> dict[str, Any]:
    """
    DBT 스냅샷 실행
    """
    try:
        # DBT 프로젝트 경로 설정
        dbt_project_path = Variable.get("DBT_PROJECT_PATH", default_var="airflow/dbt")
        
        # 스냅샷 실행 명령어
        snapshot_cmd = f"cd {dbt_project_path} && dbt snapshot --select {table_name}"
        
        # 명령어 실행
        result = subprocess.run(
            snapshot_cmd,
            shell=True,
            capture_output=True,
            text=True,
            cwd=dbt_project_path
        )
        
        if result.returncode == 0:
            return {
                "status": "success",
                "message": f"스냅샷 {table_name} 생성 완료",
                "output": result.stdout
            }
        else:
            return {
                "status": "error",
                "message": f"스냅샷 {table_name} 생성 실패",
                "error": result.stderr
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"스냅샷 실행 중 오류 발생: {e}",
            "error": str(e)
        }
```

### 2. 모델 실행

#### 모델 구조
```
models/
├── staging/           # 스테이징 레이어
│   ├── stg_edi_690.sql
│   └── stg_stock_master.sql
├── marts/            # 마트 레이어
│   ├── dim_securities.sql
│   └── fact_events.sql
└── intermediate/     # 중간 처리 레이어
    └── int_event_summary.sql
```

#### 모델 실행
```python
def execute_dbt_models(model_selection: str = "all", **context) -> dict[str, Any]:
    """
    DBT 모델 실행
    """
    try:
        # DBT 프로젝트 경로 설정
        dbt_project_path = Variable.get("DBT_PROJECT_PATH", default_var="airflow/dbt")
        
        # 모델 선택
        if model_selection == "all":
            run_cmd = f"cd {dbt_project_path} && dbt run"
        else:
            run_cmd = f"cd {dbt_project_path} && dbt run --select {model_selection}"
        
        # 명령어 실행
        result = subprocess.run(
            run_cmd,
            shell=True,
            capture_output=True,
            text=True,
            cwd=dbt_project_path
        )
        
        if result.returncode == 0:
            return {
                "status": "success",
                "message": f"모델 실행 완료: {model_selection}",
                "output": result.stdout
            }
        else:
            return {
                "status": "error",
                "message": f"모델 실행 실패: {model_selection}",
                "error": result.stderr
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"모델 실행 중 오류 발생: {e}",
            "error": str(e)
        }
```

## 📊 데이터 처리 최적화

### 1. 배치 처리

#### 배치 크기 최적화
```python
def optimize_batch_size(table_size: int, memory_limit: int = 8) -> int:
    """
    테이블 크기와 메모리 제한을 고려한 배치 크기 최적화
    """
    # 메모리 제한 (GB)
    memory_limit_gb = memory_limit
    
    # 기본 배치 크기
    base_batch_size = 10000
    
    # 테이블 크기에 따른 조정
    if table_size > 1000000:  # 100만 행 이상
        batch_size = base_batch_size // 2
    elif table_size > 100000:  # 10만 행 이상
        batch_size = base_batch_size
    else:
        batch_size = base_batch_size * 2
    
    # 메모리 제한에 따른 조정
    if memory_limit_gb < 4:
        batch_size = batch_size // 2
    elif memory_limit_gb > 16:
        batch_size = batch_size * 2
    
    return max(batch_size, 1000)  # 최소 1000행 보장
```

### 2. 병렬 처리

#### 멀티프로세싱 활용
```python
from multiprocessing import Pool, cpu_count

def parallel_data_processing(table_configs: list[dict], max_workers: int = None) -> list[dict]:
    """
    여러 테이블을 병렬로 처리
    """
    if max_workers is None:
        max_workers = min(cpu_count(), len(table_configs))
    
    with Pool(processes=max_workers) as pool:
        results = pool.map(process_single_table, table_configs)
    
    return results

def process_single_table(table_config: dict) -> dict:
    """
    단일 테이블 처리 (별도 프로세스에서 실행)
    """
    try:
        # 데이터 복사 실행
        copy_result = copy_table_data(table_config)
        return {
            "table": table_config["source"],
            "status": "success",
            "result": copy_result
        }
    except Exception as e:
        return {
            "table": table_config["source"],
            "status": "error",
            "error": str(e)
        }
```

### 3. 메모리 최적화

#### 데이터프레임 메모리 관리
```python
def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """
    데이터프레임 메모리 사용량 최적화
    """
    # 숫자 컬럼 최적화
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    # 부동소수점 컬럼 최적화
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    # 문자열 컬럼 최적화
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype('category')
    
    return df
```

## 🔍 모니터링 및 알림

### 1. 성능 메트릭

#### 실행 시간 추적
```python
import time
from contextlib import contextmanager

@contextmanager
def performance_monitoring(operation_name: str):
    """
    성능 모니터링 컨텍스트 매니저
    """
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    
    try:
        yield
    finally:
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        execution_time = end_time - start_time
        memory_usage = end_memory - start_memory
        
        logger.info(f"{operation_name} 완료:")
        logger.info(f"  - 실행 시간: {execution_time:.2f}초")
        logger.info(f"  - 메모리 사용량: {memory_usage:.2f}MB")
```

#### 데이터 처리 통계
```python
def collect_processing_stats(
    source_count: int,
    target_count: int,
    execution_time: float,
    memory_usage: float
) -> dict[str, Any]:
    """
    데이터 처리 통계 수집
    """
    return {
        "source_records": source_count,
        "target_records": target_count,
        "processing_rate": target_count / execution_time if execution_time > 0 else 0,
        "memory_efficiency": target_count / memory_usage if memory_usage > 0 else 0,
        "execution_time": execution_time,
        "memory_usage": memory_usage
    }
```

### 2. 알림 시스템

#### Slack 알림
```python
def send_slack_notification(
    message: str,
    channel: str = "#data-pipeline",
    webhook_url: str = None
) -> None:
    """
    Slack으로 알림 전송
    """
    if webhook_url is None:
        webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var="")
    
    if not webhook_url:
        logger.warning("Slack 웹훅 URL이 설정되지 않음")
        return
    
    try:
        payload = {
            "text": message,
            "channel": channel,
            "username": "Data Pipeline Bot",
            "icon_emoji": ":robot_face:"
        }
        
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        
        logger.info(f"Slack 알림 전송 완료: {channel}")
        
    except Exception as e:
        logger.error(f"Slack 알림 전송 실패: {e}")
```

#### 이메일 알림
```python
def send_email_notification(
    subject: str,
    message: str,
    recipients: list[str],
    smtp_config: dict = None
) -> None:
    """
    이메일로 알림 전송
    """
    if smtp_config is None:
        smtp_config = {
            "host": Variable.get("SMTP_HOST", default_var="localhost"),
            "port": Variable.get("SMTP_PORT", default_var=587),
            "username": Variable.get("SMTP_USERNAME", default_var=""),
            "password": Variable.get("SMTP_PASSWORD", default_var=""),
            "use_tls": Variable.get("SMTP_USE_TLS", default_var=True)
        }
    
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        # 이메일 메시지 생성
        msg = MIMEMultipart()
        msg['From'] = smtp_config["username"]
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject
        
        msg.attach(MIMEText(message, 'plain'))
        
        # SMTP 서버 연결 및 전송
        with smtplib.SMTP(smtp_config["host"], smtp_config["port"]) as server:
            if smtp_config["use_tls"]:
                server.starttls()
            
            if smtp_config["username"] and smtp_config["password"]:
                server.login(smtp_config["username"], smtp_config["password"])
            
            server.send_message(msg)
        
        logger.info(f"이메일 알림 전송 완료: {', '.join(recipients)}")
        
    except Exception as e:
        logger.error(f"이메일 알림 전송 실패: {e}")
```

## 🚨 에러 핸들링 및 복구

### 1. 재시도 메커니즘

#### 지수 백오프 재시도
```python
import time
from functools import wraps

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0
):
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
                        # 지수 백오프 계산
                        delay = min(base_delay * (exponential_base ** attempt), max_delay)
                        
                        logger.warning(
                            f"{func.__name__} 실행 실패 (시도 {attempt + 1}/{max_retries}): {e}"
                        )
                        logger.info(f"{delay:.2f}초 후 재시도...")
                        
                        time.sleep(delay)
                    else:
                        logger.error(f"{func.__name__} 최종 실패: {e}")
            
            raise last_exception
        
        return wrapper
    return decorator
```

### 2. 회복 전략

#### 데이터 복구
```python
def recover_failed_pipeline(
    table_config: dict,
    failure_point: str,
    **context
) -> dict[str, Any]:
    """
    실패한 파이프라인 복구
    """
    try:
        if failure_point == "data_copy":
            # 데이터 복사 단계에서 실패한 경우
            logger.info("데이터 복사 단계에서 실패, 복구 시도...")
            
            # 임시 테이블 정리
            cleanup_temp_tables(table_config, **context)
            
            # 데이터 복사 재시도
            copy_result = copy_data_with_dynamic_sql(table_config, **context)
            
            return {
                "status": "recovered",
                "message": "데이터 복사 복구 완료",
                "result": copy_result
            }
            
        elif failure_point == "merge_operation":
            # MERGE 작업에서 실패한 경우
            logger.info("MERGE 작업에서 실패, 복구 시도...")
            
            # MERGE 작업 재시도
            merge_result = execute_merge_operation(table_config, **context)
            
            return {
                "status": "recovered",
                "message": "MERGE 작업 복구 완료",
                "result": merge_result
            }
            
        else:
            return {
                "status": "error",
                "message": f"알 수 없는 실패 지점: {failure_point}"
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"복구 실패: {e}"
        }
```

## 📈 확장성 및 유지보수

### 1. 플러그인 아키텍처

#### 데이터 소스 플러그인
```python
from abc import ABC, abstractmethod

class DataSourcePlugin(ABC):
    """데이터 소스 플러그인 기본 클래스"""
    
    @abstractmethod
    def connect(self, config: dict) -> bool:
        """데이터 소스 연결"""
        pass
    
    @abstractmethod
    def extract_data(self, query: str, **kwargs) -> pd.DataFrame:
        """데이터 추출"""
        pass
    
    @abstractmethod
    def get_schema(self, table_name: str) -> dict:
        """스키마 정보 조회"""
        pass

class PostgreSQLPlugin(DataSourcePlugin):
    """PostgreSQL 데이터 소스 플러그인"""
    
    def connect(self, config: dict) -> bool:
        # PostgreSQL 연결 로직
        pass
    
    def extract_data(self, query: str, **kwargs) -> pd.DataFrame:
        # PostgreSQL 데이터 추출 로직
        pass
    
    def get_schema(self, table_name: str) -> dict:
        # PostgreSQL 스키마 조회 로직
        pass
```

### 2. 설정 기반 동작

#### 동적 설정 로딩
```python
def load_table_configs(environment: str = "production") -> list[dict]:
    """
    환경별 테이블 설정 로딩
    """
    config_path = f"configs/{environment}/table_configs.json"
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            configs = json.load(f)
        
        logger.info(f"{environment} 환경 설정 로딩 완료: {len(configs)}개 테이블")
        return configs
        
    except FileNotFoundError:
        logger.warning(f"설정 파일을 찾을 수 없음: {config_path}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"설정 파일 파싱 오류: {e}")
        return []
```

## 🔮 향후 발전 방향

### 1. 실시간 처리
- Apache Kafka를 통한 실시간 데이터 스트리밍
- Change Data Capture (CDC) 구현
- 실시간 데이터 변환 및 집계

### 2. 머신러닝 통합
- 데이터 품질 자동 감지
- 이상 패턴 자동 탐지
- 예측 모델 자동 학습 및 배포

### 3. 클라우드 네이티브
- Kubernetes 기반 오케스트레이션
- 서버리스 함수 통합
- 멀티 클라우드 지원

---

이 아키텍처 문서는 DBT-Airflow Template의 핵심 구조와 동작 방식을 설명합니다. 
프로젝트의 발전에 따라 지속적으로 업데이트되어야 합니다. 