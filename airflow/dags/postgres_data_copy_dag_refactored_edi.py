"""
PostgreSQL EDI Data Copy DAG (Refactored)
EDI 관련 PostgreSQL 테이블을 복사하는 DAG - 리팩토링 버전

이 DAG는 다음 작업을 수행합니다:
1. EDI 소스 PostgreSQL에서 데이터 추출 (동기화 모드별)
2. EDI 데이터 변환 및 검증
3. 타겟 PostgreSQL에 EDI 데이터 로드
4. EDI 데이터 무결성 검사
5. dbt 스냅샷 생성

동기화 모드:
- incremental_sync: 증분 동기화 (기존 데이터 유지)
- full_sync: 전체 동기화 (소스에 없는 데이터 삭제)

EDI 특화 기능:
- EDI 이벤트 데이터 처리
- 우선순위 필드 자동 변환
- EDI 상태 추적
"""

import logging
from datetime import datetime, timedelta
from typing import Any

# 공통 모듈 import
from common import (
    DatabaseOperations,
    DataCopyEngine,
    DBTIntegration,
    MonitoringManager,
    ProgressTracker,
)

from airflow import DAG
from airflow.operators.python import PythonOperator

# 로거 설정
logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["admin@example.com"],
}

# DAG 정의
dag = DAG(
    "postgres_edi_data_copy_refactored",
    default_args=default_args,
    description="Copy EDI data from PostgreSQL tables and create dbt snapshots (Refactored)",
    schedule_interval="0 9 * * *",  # 매일 9시(KST 18시)에 실행
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "data-copy", "etl", "edi", "dbt-snapshot", "refactored"],
    max_active_runs=1,
)

# 연결 ID 설정
SOURCE_CONN_ID = "fs2_postgres"  # EDI 소스 데이터베이스 (기존 DAG와 동일)
TARGET_CONN_ID = "postgres_default"  # 타겟 데이터베이스

# dbt 프로젝트 경로 설정
DBT_PROJECT_PATH = "/opt/airflow/dbt"

# EDI 테이블 설정 (순차 처리)
EDI_TABLES_CONFIG = [
    {
        "source": "raw_data.edi_690",
        "target": "raw_data.edi_690",
        "primary_key": ["eventcd", "eventid", "optionid"],
        "sync_mode": "full_sync",  # 'incremental_sync' 또는 'full_sync'
        "batch_size": 10000,
        "incremental_field": "eventdate",
        "incremental_field_type": "yyyymmdd",
        "custom_where": "eventdate >= '20250812'",  # EDI 데이터 필터링
    },
    # 추가 EDI 테이블 설정은 여기에 추가
]


def validate_connections(**context) -> dict[str, Any]:
    """데이터베이스 연결 검증"""
    try:
        # 모니터링 시작
        monitoring = MonitoringManager(
            f"연결 검증 - {context['task_instance'].task_id}"
        )
        monitoring.start_monitoring()

        # 진행 상황 추적
        progress = ProgressTracker(2, f"연결 검증 - {context['task_instance'].task_id}")

        # 1단계: 소스 데이터베이스 연결 검증
        progress.start_step("소스 연결 검증", f"소스 데이터베이스 {SOURCE_CONN_ID} 연결 테스트")
        db_ops = DatabaseOperations(SOURCE_CONN_ID, TARGET_CONN_ID)
        source_conn = db_ops.get_source_hook().get_conn()
        source_conn.close()
        progress.complete_step("소스 연결 검증")

        monitoring.add_checkpoint("소스 연결", f"소스 데이터베이스 {SOURCE_CONN_ID} 연결 성공")

        # 2단계: 타겟 데이터베이스 연결 검증
        progress.start_step("타겟 연결 검증", f"타겟 데이터베이스 {TARGET_CONN_ID} 연결 테스트")
        target_conn = db_ops.get_target_hook().get_conn()
        target_conn.close()
        progress.complete_step("타겟 연결 검증")

        monitoring.add_checkpoint("타겟 연결", f"타겟 데이터베이스 {TARGET_CONN_ID} 연결 성공")

        # 모니터링 종료
        monitoring.stop_monitoring("completed")

        # 결과 반환
        return {
            "status": "success",
            "message": "모든 데이터베이스 연결 검증 완료",
            "monitoring_summary": monitoring.get_summary(),
            "progress_summary": progress.get_progress(),
        }

    except Exception as e:
        error_msg = f"연결 검증 실패: {e}"
        logger.error(error_msg)
        
        # 모니터링 종료 (실패 상태)
        if 'monitoring' in locals():
            monitoring.add_error(f"연결 검증 실패: {str(e)}")
            monitoring.stop_monitoring("failed")
        
        raise Exception(error_msg)


def copy_table_data(table_config: dict[str, Any], **context) -> dict[str, Any]:
    """개별 EDI 테이블 데이터 복사"""
    try:
        task_id = context["task_instance"].task_id
        table_name = table_config["source"]

        # 모니터링 시작
        monitoring = MonitoringManager(f"EDI 테이블 복사 - {table_name}")
        monitoring.start_monitoring()

        # 진행 상황 추적
        progress = ProgressTracker(6, f"EDI 테이블 복사 - {table_name}")

        # 1단계: 데이터베이스 작업 객체 생성
        progress.start_step(
            "데이터베이스 작업 객체 생성", "DatabaseOperations 및 DataCopyEngine 초기화"
        )
        db_ops = DatabaseOperations(SOURCE_CONN_ID, TARGET_CONN_ID)
        copy_engine = DataCopyEngine(db_ops)
        progress.complete_step("데이터베이스 작업 객체 생성")

        monitoring.add_checkpoint(
            "객체 생성", "DatabaseOperations 및 DataCopyEngine 객체 생성 완료"
        )

        # 2단계: 소스 테이블 스키마 조회 (먼저 수행)
        progress.start_step(
            "소스 테이블 스키마 조회", f"EDI 테이블 {table_name}의 스키마 정보 조회"
        )
        source_schema = db_ops.get_table_schema(table_config["source"])
        progress.complete_step(
            "소스 테이블 스키마 조회", {"columns_count": len(source_schema["columns"])}
        )

        monitoring.add_checkpoint(
            "스키마 조회", f"EDI 소스 테이블 스키마 조회 완료: {len(source_schema['columns'])}개 컬럼"
        )

        # 3단계: 타겟 테이블 생성 및 스키마 검증 (한 번에 처리)
        progress.start_step(
            "타겟 테이블 생성 및 검증",
            f"EDI 타겟 테이블 {table_config['target']} 생성 및 스키마 검증",
        )
        
        # 새로운 메서드 사용: 테이블 생성과 스키마 검증을 한 번에 처리
        verified_target_schema = copy_engine.create_target_table_and_verify_schema(
            table_config["target"], source_schema
        )
        
        progress.complete_step("타겟 테이블 생성 및 검증")

        monitoring.add_checkpoint(
            "테이블 준비", f"EDI 타겟 테이블 {table_config['target']} 준비 완료"
        )

        # 4단계: 데이터 복사 실행
        progress.start_step(
            "EDI 데이터 복사 실행",
            f"EDI 테이블 {table_name}에서 {table_config['target']}로 데이터 복사",
        )
        
        # 검증된 타겟 스키마를 사용하여 데이터 복사
        copy_result = copy_engine.copy_data_with_custom_sql(
            table_config["source"],
            table_config["target"],
            table_config.get("primary_key", []),
            table_config.get("sync_mode", "incremental_sync"),
            table_config.get("incremental_field"),
            table_config.get("incremental_field_type"),
            table_config.get("custom_where"),
            table_config.get("batch_size", 10000),
            verified_target_schema  # 검증된 스키마 전달
        )
        
        progress.complete_step("EDI 데이터 복사 실행")

        monitoring.add_checkpoint(
            "데이터 복사", f"EDI 데이터 복사 완료: {copy_result.get('exported_rows', 0)}행 처리"
        )

        # 5단계: 데이터 무결성 검증
        progress.start_step(
            "EDI 데이터 무결성 검증", "복사된 EDI 데이터의 무결성 검증"
        )
        
        # 기본키 컬럼 추출 (테이블 설정에서 가져오거나 기본값 사용)
        primary_keys = table_config.get("primary_key", ["id"])
        if not primary_keys:
            # 기본키가 설정되지 않은 경우 첫 번째 컬럼을 기본키로 사용
            primary_keys = ["id"]

        validation_result = db_ops.validate_data_integrity(
            table_config["source"], table_config["target"], primary_keys
        )
        
        progress.complete_step("EDI 데이터 무결성 검증")

        # 6단계: 정리 작업
        progress.start_step("정리 작업", "데이터베이스 연결 및 리소스 정리")
        db_ops.close_connections()
        progress.complete_step("정리 작업")

        monitoring.add_checkpoint(
            "정리 완료", "데이터베이스 연결 정리 완료"
        )

        # 모니터링 종료
        monitoring.stop_monitoring("completed")

        # 결과 반환
        return {
            "status": "success",
            "table_config": table_config,
            "copy_result": copy_result,
            "validation_result": validation_result,
            "monitoring_summary": monitoring.get_summary(),
            "progress_summary": progress.get_progress(),
        }

    except Exception as e:
        error_msg = f"EDI 테이블 복사 실패: {table_name}, 오류: {e}"
        logger.error(error_msg)
        
        # 모니터링 종료 (실패 상태)
        if 'monitoring' in locals():
            monitoring.add_error(f"EDI 테이블 복사 실패: {str(e)}")
            monitoring.stop_monitoring("failed")
        
        raise Exception(error_msg)


def create_dbt_snapshot(**context) -> dict[str, Any]:
    """dbt 스냅샷 생성"""
    try:
        # 모니터링 시작
        monitoring = MonitoringManager("dbt 스냅샷 생성")
        monitoring.start_monitoring()

        # 진행 상황 추적
        progress = ProgressTracker(3, "dbt 스냅샷 생성")

        # 1단계: dbt 통합 객체 생성
        progress.start_step("dbt 통합 객체 생성", "DBTIntegration 객체 초기화")
        dbt_integration = DBTIntegration(DBT_PROJECT_PATH)
        progress.complete_step("dbt 통합 객체 생성")

        monitoring.add_checkpoint("dbt 객체 생성", "DBTIntegration 객체 생성 완료")

        # 2단계: 스냅샷 실행
        progress.start_step("dbt 스냅샷 실행", "EDI 관련 테이블의 dbt 스냅샷 생성")
        snapshot_result = dbt_integration.run_snapshot()
        progress.complete_step("dbt 스냅샷 실행")

        monitoring.add_checkpoint("스냅샷 실행", f"dbt 스냅샷 실행 완료: {snapshot_result}")

        # 3단계: 모델 실행
        progress.start_step("dbt 모델 실행", "EDI 관련 모델 실행")
        model_result = dbt_integration.run_models()
        progress.complete_step("dbt 모델 실행")

        monitoring.add_checkpoint("모델 실행", f"dbt 모델 실행 완료: {model_result}")

        # 모니터링 종료
        monitoring.stop_monitoring("completed")

        # 결과 반환
        return {
            "status": "success",
            "snapshot_result": snapshot_result,
            "model_result": model_result,
            "monitoring_summary": monitoring.get_summary(),
            "progress_summary": progress.get_progress(),
        }

    except Exception as e:
        error_msg = f"dbt 스냅샷 생성 실패: {e}"
        logger.error(error_msg)
        
        # 모니터링 종료 (실패 상태)
        if 'monitoring' in locals():
            monitoring.add_error(f"dbt 스냅샷 생성 실패: {str(e)}")
            monitoring.stop_monitoring("failed")
        
        raise Exception(error_msg)


# 태스크 정의
validate_connections_task = PythonOperator(
    task_id="validate_connections",
    python_callable=validate_connections,
    dag=dag,
)

# EDI 테이블별 복사 태스크 생성
edi_copy_tasks = []
for i, table_config in enumerate(EDI_TABLES_CONFIG):
    task_id = f"copy_edi_table_{i+1}_{table_config['source'].replace('.', '_').replace('raw_data_', '')}"
    
    copy_task = PythonOperator(
        task_id=task_id,
        python_callable=copy_table_data,
        op_kwargs={"table_config": table_config},
        dag=dag,
    )
    
    edi_copy_tasks.append(copy_task)

create_dbt_snapshot_task = PythonOperator(
    task_id="create_dbt_snapshot",
    python_callable=create_dbt_snapshot,
    dag=dag,
)

# 태스크 의존성 설정
validate_connections_task >> edi_copy_tasks >> create_dbt_snapshot_task

# DAG 문서화
dag.doc_md = __doc__

# 태스크별 문서화
validate_connections_task.doc_md = """
데이터베이스 연결 검증 태스크

이 태스크는 다음을 수행합니다:
1. 소스 PostgreSQL 데이터베이스 연결 테스트
2. 타겟 PostgreSQL 데이터베이스 연결 테스트
3. 연결 상태 모니터링 및 로깅
"""

for i, task in enumerate(edi_copy_tasks):
    table_config = EDI_TABLES_CONFIG[i]
    task.doc_md = f"""
EDI 테이블 복사 태스크 - {table_config['source']}

이 태스크는 다음을 수행합니다:
1. EDI 소스 테이블 {table_config['source']} 스키마 조회
2. 타겟 테이블 {table_config['target']} 생성 및 스키마 검증
3. EDI 데이터 복사 및 변환
4. 데이터 무결성 검증
5. 모니터링 및 진행 상황 추적

설정:
- 기본키: {table_config.get('primary_key', [])}
- 동기화 모드: {table_config.get('sync_mode', 'incremental_sync')}
- 배치 크기: {table_config.get('batch_size', 10000)}
"""

create_dbt_snapshot_task.doc_md = """
dbt 스냅샷 생성 태스크

이 태스크는 다음을 수행합니다:
1. EDI 관련 테이블의 dbt 스냅샷 생성
2. EDI 관련 모델 실행
3. 데이터 품질 및 변환 결과 검증
"""
