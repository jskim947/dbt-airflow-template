"""
PostgreSQL Multi-Table Data Copy DAG (Refactored)
여러 PostgreSQL 테이블을 순차적으로 복사하는 DAG - 리팩토링 버전

이 DAG는 다음 작업을 수행합니다:
1. 소스 PostgreSQL에서 데이터 추출 (동기화 모드별)
2. 데이터 변환 및 검증
3. 타겟 PostgreSQL에 데이터 로드
4. 데이터 무결성 검사
5. dbt 스냅샷 생성

동기화 모드:
- incremental_sync: 증분 동기화 (기존 데이터 유지)
- full_sync: 전체 동기화 (소스에 없는 데이터 삭제)
"""

import logging
from datetime import datetime, timedelta
from typing import Any

# 공통 모듈 import
from common import (
    DAGConfigManager,
    DAGSettings,
    ConnectionManager,
    DataCopyEngine,
    DatabaseOperations,
    MonitoringManager,
    ProgressTracker,
)

# 청크 방식 설정 헬퍼 함수
def get_chunk_mode_settings(table_config: dict) -> dict:
    """
    테이블 설정에서 청크 방식 설정을 추출하는 헬퍼 함수
    
    Args:
        table_config: 테이블 설정 딕셔너리
        
    Returns:
        청크 방식 설정 딕셔너리
    """
    return {
        "chunk_mode": table_config.get("chunk_mode", True),
        "enable_checkpoint": table_config.get("enable_checkpoint", True),
        "max_retries": table_config.get("max_retries", 3)
    }

def get_safe_table_field(table_config: dict, field_name: str, fallback_field: str = None) -> str:
    """
    테이블 설정에서 안전하게 필드값을 가져오는 헬퍼 함수
    
    Args:
        table_config: 테이블 설정 딕셔너리
        field_name: 기본 필드명
        fallback_field: 대체 필드명
        
    Returns:
        필드값
    """
    if field_name in table_config:
        return table_config[field_name]
    elif fallback_field and fallback_field in table_config:
        return table_config[fallback_field]
    else:
        available_fields = list(table_config.keys())
        raise ValueError(f"필드 '{field_name}' 또는 '{fallback_field}'를 찾을 수 없습니다. 사용 가능한 필드: {available_fields}")

from airflow import DAG
from airflow.operators.python import PythonOperator

# 로거 설정
logger = logging.getLogger(__name__)

# 공통 모듈 import
from common import DAGConfigManager, DAGSettings, ConnectionManager

# DAG 기본 설정
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["admin@example.com"],
}

# 연결 ID 설정 (DAGConfigManager에서 가져오기)
dag_config = DAGConfigManager.get_dag_config("postgres_multi_table_copy_refactored2")
SOURCE_CONN_ID = dag_config.get("source_connection", "fs2_postgres")
TARGET_CONN_ID = dag_config.get("target_connection", "postgres_default")

# DAG 정의
dag = DAG(
    dag_id="postgres_multi_table_copy_refactored2",
    default_args=default_args,
    description=dag_config.get("description", "Copy data from multiple PostgreSQL tables sequentially and create dbt snapshots (Refactored)"),
    schedule_interval=DAGConfigManager.get_dag_schedule("postgres_multi_table_copy_refactored2"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=dag_config.get("tags", ["postgres", "data-copy", "etl", "refactored", "multi-table", "dbt-snapshot"]),
    max_active_runs=1,
)

# dbt 프로젝트 경로 설정
DBT_PROJECT_PATH = "/opt/airflow/dbt"

# 테이블 설정 (DAGConfigManager에서 가져오기)
TABLES_CONFIG = DAGConfigManager.get_table_configs("postgres_multi_table_copy_refactored2")

# 설정이 비어있거나 잘못된 경우 기본값 사용
if not TABLES_CONFIG or not isinstance(TABLES_CONFIG, list):
    logger.warning("DAGSettings에서 테이블 설정을 가져올 수 없어 기본값을 사용합니다.")
    TABLES_CONFIG = [
        {
            "source": "인포맥스종목마스터",
            "target": "인포맥스종목마스터",
            "primary_key": ["종목코드"],
            "sync_mode": "incremental_sync",
            "batch_size": 10000,
            "chunk_mode": True,
            "enable_checkpoint": True,
            "max_retries": 3,
            "incremental_field": "updated_at",
            "incremental_field_type": "timestamp",
            "description": "인포맥스 종목 마스터 테이블"
        },
        {
            "source": "ff_v3_ff_sec_entity",
            "target": "ff_v3_ff_sec_entity",
            "primary_key": ["entity_id"],
            "sync_mode": "incremental_sync",
            "batch_size": 20000,
            "chunk_mode": True,
            "enable_checkpoint": True,
            "max_retries": 3,
            "incremental_field": "last_modified",
            "incremental_field_type": "timestamp",
            "description": "FF 보안 엔티티 테이블"
        },
        {
            "source": "sym_v1_sym_ticker_exchange",
            "target": "sym_v1_sym_ticker_exchange",
            "primary_key": ["ticker", "exchange"],
            "sync_mode": "incremental_sync",
            "batch_size": 15000,
            "chunk_mode": True,
            "enable_checkpoint": True,
            "max_retries": 3,
            "incremental_field": "modified_date",
            "incremental_field_type": "date",
            "description": "심볼 티커 거래소 테이블"
        }
    ]


def validate_connections(**context) -> dict[str, Any]:
    """데이터베이스 연결 검증"""
    try:
        # 모니터링 시작
        monitoring = MonitoringManager(
            f"연결 검증 - {context['task_instance'].task_id}"
        )
        monitoring.start_monitoring()

        # 데이터베이스 연결 검증
        db_ops = DatabaseOperations(SOURCE_CONN_ID, TARGET_CONN_ID)
        connection_results = db_ops.test_connections()

        monitoring.add_checkpoint(
            "연결 검증", "소스 및 타겟 데이터베이스 연결 테스트 완료"
        )

        # 연결 결과 확인
        if not all(connection_results.values()):
            failed_connections = [
                conn for conn, status in connection_results.items() if not status
            ]
            error_msg = f"연결 실패: {', '.join(failed_connections)}"
            monitoring.add_error(error_msg)
            monitoring.stop_monitoring("failed")
            raise Exception(error_msg)

        monitoring.add_checkpoint("연결 성공", "모든 데이터베이스 연결 성공")
        monitoring.stop_monitoring("completed")

        return {
            "status": "success",
            "connections": connection_results,
            "monitoring_summary": monitoring.get_summary(),
        }

    except Exception as e:
        logger.error(f"연결 검증 실패: {str(e)}")
        raise


def copy_table_data(table_config: dict[str, Any], **context) -> dict[str, Any]:
    """개별 테이블 데이터 복사"""
    try:
        task_id = context["task_instance"].task_id
        
        # table_config 구조 디버깅
        logger.info(f"table_config 구조: {table_config}")
        logger.info(f"table_config 키들: {list(table_config.keys())}")
        
        # 안전한 테이블명 추출
        table_name = get_safe_table_field(table_config, "source_table", "source")

        # 모니터링 시작
        monitoring = MonitoringManager(f"테이블 복사 - {table_name}")
        monitoring.start_monitoring()

        # 진행 상황 추적
        progress = ProgressTracker(6, f"테이블 복사 - {table_name}")

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

        # 2단계: 타겟 테이블 존재 확인 및 생성
        target_table = get_safe_table_field(table_config, "target_table", "target")
        progress.start_step(
            "타겟 테이블 확인 및 생성",
            f"타겟 테이블 {target_table} 존재 확인 및 생성",
        )
        
        # 공통 모듈의 DatabaseOperations 사용
        db_ops.ensure_target_table_exists(table_config, **context)
        progress.complete_step("타겟 테이블 확인 및 생성")

        monitoring.add_checkpoint(
            "테이블 준비", f"타겟 테이블 {target_table} 준비 완료"
        )

        # 3단계: 소스 테이블 스키마 조회
        source_table = get_safe_table_field(table_config, "source_table", "source")
        progress.start_step(
            "소스 테이블 스키마 조회", f"테이블 {source_table}의 스키마 정보 조회"
        )
        source_schema = db_ops.get_table_schema(source_table)
        progress.complete_step(
            "소스 테이블 스키마 조회", {"columns_count": len(source_schema["columns"])}
        )

        monitoring.add_checkpoint(
            "스키마 조회",
            f"소스 테이블 스키마 조회 완료: {len(source_schema['columns'])}개 컬럼",
        )

        # 4단계: 데이터 복사 실행
        progress.start_step(
            "데이터 복사 실행",
            f"테이블 {table_name}에서 {target_table}로 데이터 복사",
        )

        # custom_where 조건 로깅 추가
        custom_where = table_config.get("custom_where")
        if custom_where:
            logger.info(f"커스텀 WHERE 조건 적용: {custom_where}")
            monitoring.add_checkpoint("WHERE 조건", f"커스텀 조건 적용: {custom_where}")
        else:
            logger.info("커스텀 WHERE 조건 없음 - 전체 데이터 처리")
            monitoring.add_checkpoint("WHERE 조건", "전체 데이터 처리")

        # 청크 방식 설정 가져오기 (헬퍼 함수 사용)
        chunk_settings = get_chunk_mode_settings(table_config)
        
        # 청크 방식 설정 로깅
        if chunk_settings["chunk_mode"]:
            logger.info(f"청크 방식 데이터 복사 활성화: {table_name}")
            logger.info(f"체크포인트: {'활성화' if chunk_settings['enable_checkpoint'] else '비활성화'}")
            logger.info(f"최대 재시도: {chunk_settings['max_retries']}회")
            monitoring.add_checkpoint("청크 방식", f"활성화 (체크포인트: {'활성화' if chunk_settings['enable_checkpoint'] else '비활성화'})")
        else:
            logger.info(f"기존 방식 데이터 복사: {table_name}")
            monitoring.add_checkpoint("데이터 복사 방식", "기존 방식 (메모리 누적)")
        
        # 성능 최적화 설정에 따라 스트리밍 파이프 또는 기존 방식 선택
        try:
            from common.settings import BatchSettings
            performance_config = BatchSettings.get_performance_optimization_config()
            use_streaming = performance_config.get("enable_streaming_pipe", False)
        except ImportError:
            use_streaming = False
        
        if use_streaming:
            logger.info(f"스트리밍 파이프 방식으로 데이터 복사 시작: {table_name}")
            monitoring.add_checkpoint("복사 방식", "스트리밍 파이프 (중간 파일 없음)")
            
            # 스트리밍 파이프 방식 사용
            copy_result = copy_engine.copy_data_with_streaming_pipe(
                source_table=source_table,
                target_table=target_table,
                primary_keys=table_config["primary_key"],
                sync_mode=table_config["sync_mode"],
                batch_size=table_config["batch_size"],
                custom_where=table_config.get("custom_where"),
                chunk_mode=chunk_settings["chunk_mode"],
                enable_checkpoint=chunk_settings["enable_checkpoint"],
                max_retries=chunk_settings["max_retries"]
            )
        else:
            logger.info(f"스트리밍 파이프 방식으로 데이터 복사 시작: {table_name}")
            monitoring.add_checkpoint("복사 방식", "스트리밍 파이프 (청크 방식 지원)")
            
            # 스트리밍 파이프 방식 사용 (청크 방식 지원)
            copy_result = copy_engine.copy_data_with_streaming_pipe(
                source_table=source_table,
                target_table=target_table,
                primary_keys=table_config["primary_key"],
                sync_mode=table_config["sync_mode"],
                batch_size=table_config["batch_size"],
                custom_where=table_config.get("custom_where"),
                incremental_field=table_config.get("incremental_field"),
                incremental_field_type=table_config.get("incremental_field_type"),
                chunk_mode=chunk_settings["chunk_mode"],
                enable_checkpoint=chunk_settings["enable_checkpoint"],
                max_retries=chunk_settings["max_retries"]
            )

        # 데이터 복사 결과 검증
        if copy_result["status"] == "error":
            progress.fail_step("데이터 복사 실행", copy_result["error"])
            monitoring.add_error(f"데이터 복사 실패: {copy_result['error']}")
            monitoring.stop_monitoring("failed")
            raise Exception(f"데이터 복사 실패: {copy_result['error']}")

        progress.complete_step("데이터 복사 실행", copy_result)

        monitoring.add_checkpoint(
            "데이터 복사", f"데이터 복사 완료: {copy_result['exported_rows']}행 처리"
        )
        monitoring.add_performance_metric(
            "복사 시간", copy_result["total_execution_time"], "초"
        )

        # 5단계: 데이터 무결성 검증
        progress.start_step("데이터 무결성 검증", "복사된 데이터의 무결성 검증")
        
        # WHERE 조건이 있는 경우 무결성 검증에도 적용
        where_clause = table_config.get("custom_where")
        validation_result = db_ops.validate_data_integrity(
            source_table, 
            target_table, 
            table_config["primary_key"],
            where_clause=where_clause
        )
        progress.complete_step("데이터 무결성 검증", validation_result)

        if not validation_result["is_valid"]:
            monitoring.add_warning(
                f"데이터 무결성 검증 실패: {validation_result['message']}"
            )
        else:
            monitoring.add_checkpoint("무결성 검증", "데이터 무결성 검증 성공")

        # 6단계: 정리 작업
        progress.start_step("정리 작업", "데이터베이스 연결 및 리소스 정리")
        db_ops.close_connections()
        progress.complete_step("정리 작업")

        monitoring.add_checkpoint("정리 완료", "데이터베이스 연결 정리 완료")

        # 모니터링 종료
        monitoring.stop_monitoring("completed")

        return {
            "status": "success",
            "table_config": table_config,
            "copy_result": copy_result,
            "validation_result": validation_result,
            "monitoring_summary": monitoring.get_summary(),
            "progress_summary": progress.get_progress(),
        }

    except Exception as e:
        logger.error(f"테이블 복사 실패: {table_name} - {str(e)}")
        raise


def execute_dbt_pipeline(**context) -> dict[str, Any]:
    """dbt 파이프라인 실행 (스냅샷 -> run -> test) - 임시로 비활성화"""
    try:
        # 모니터링 시작
        monitoring = MonitoringManager("dbt 파이프라인 실행")
        monitoring.start_monitoring()

        # 진행 상황 추적
        progress = ProgressTracker(1, "dbt 파이프라인 실행")

        # 1단계: dbt 기능 임시 비활성화
        progress.start_step("dbt 기능 확인", "DBTIntegration 클래스가 아직 구현되지 않음")
        
        # 임시 메시지
        message = "DBT 기능이 아직 구현되지 않았습니다. 데이터 복사만 진행합니다."
        logger.info(message)
        progress.complete_step("dbt 기능 확인", {"message": message})

        monitoring.add_checkpoint("dbt 상태", "DBT 기능 임시 비활성화")

        # 모니터링 종료
        monitoring.stop_monitoring("completed")

        return {
            "status": "success",
            "message": message,
            "monitoring_summary": monitoring.get_summary(),
            "progress_summary": progress.get_progress(),
        }

    except Exception as e:
        logger.error(f"dbt 파이프라인 실행 실패: {e!s}")
        # 실패 상태로 모니터링 종료 시도 (이미 종료되었을 수 있으므로 예외 무시)
        try:
            monitoring.stop_monitoring("failed")
        except Exception:
            pass
        raise


def generate_copy_tasks():
    """테이블별 복사 태스크 생성 (TaskFactory 사용)"""
    from common import TaskFactory
    
    copy_tasks = TaskFactory.create_copy_tasks(
        table_configs=TABLES_CONFIG,
        copy_function=copy_table_data,
        dag=dag,
        task_prefix="copy_table"
    )
    
    # 디버깅: 생성된 태스크 정보 출력
    logger.info(f"생성된 복사 태스크 수: {len(copy_tasks)}")
    for i, task in enumerate(copy_tasks):
        logger.info(f"태스크 {i+1}: {task.task_id} -> {task.python_callable.__name__}")
    
    return copy_tasks


# 태스크 생성
validate_connections_task = PythonOperator(
    task_id="validate_connections", python_callable=validate_connections, dag=dag
)

copy_tasks = generate_copy_tasks()

dbt_pipeline_task = PythonOperator(
    task_id="execute_dbt_pipeline", python_callable=execute_dbt_pipeline, dag=dag
)

# 태스크 의존성 설정 - 순차 실행을 위한 체인 생성
if copy_tasks:
    # 첫 번째 복사 태스크는 연결 검증 후 실행
    validate_connections_task >> copy_tasks[0]
    
    # 중간 복사 태스크들은 순차적으로 연결
    for i in range(len(copy_tasks) - 1):
        copy_tasks[i] >> copy_tasks[i + 1]
    
    # 마지막 복사 태스크는 dbt 파이프라인 실행
    copy_tasks[-1] >> dbt_pipeline_task
else:
    # 복사 태스크가 없는 경우 직접 연결
    validate_connections_task >> dbt_pipeline_task
