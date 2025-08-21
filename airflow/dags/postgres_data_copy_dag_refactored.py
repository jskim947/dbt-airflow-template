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

# DAG 정의
dag = DAG(
    dag_id="postgres_multi_table_copy_refactored",
    default_args=default_args,
    description="Copy data from multiple PostgreSQL tables sequentially and create dbt snapshots (Refactored)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "data-copy", "etl", "refactored", "multi-table", "dbt-snapshot"],
    max_active_runs=1,
)

# 연결 ID 설정 (DAGConfigManager에서 가져오기)
dag_config = DAGConfigManager.get_dag_config("postgres_multi_table_copy_refactored")
SOURCE_CONN_ID = dag_config.get("source_connection", "fs2_postgres")
TARGET_CONN_ID = dag_config.get("target_connection", "postgres_default")

# dbt 프로젝트 경로 설정
DBT_PROJECT_PATH = "/opt/airflow/dbt"

# 테이블 설정 (공통 설정 사용)
TABLES_CONFIG = DAGSettings.get_table_configs()


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
        from common import ErrorHandler
        error_handler = ErrorHandler(context)
        error_handler.handle_connection_error("연결 검증", "database", e)


def copy_table_data(table_config: dict[str, Any], **context) -> dict[str, Any]:
    """개별 테이블 데이터 복사"""
    try:
        task_id = context["task_instance"].task_id
        table_name = table_config["source"]

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
        progress.start_step(
            "타겟 테이블 확인 및 생성",
            f"타겟 테이블 {table_config['target']} 존재 확인 및 생성",
        )
        
        # 공통 모듈의 DatabaseOperations 사용
        db_ops.ensure_target_table_exists(table_config, **context)
        progress.complete_step("타겟 테이블 확인 및 생성")

        monitoring.add_checkpoint(
            "테이블 준비", f"타겟 테이블 {table_config['target']} 준비 완료"
        )

        # 3단계: 소스 테이블 스키마 조회
        progress.start_step(
            "소스 테이블 스키마 조회", f"테이블 {table_name}의 스키마 정보 조회"
        )
        source_schema = db_ops.get_table_schema(table_config["source"])
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
            f"테이블 {table_name}에서 {table_config['target']}로 데이터 복사",
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
                source_table=table_config["source"],
                target_table=table_config["target"],
                primary_keys=table_config["primary_key"],
                sync_mode=table_config["sync_mode"],
                batch_size=table_config["batch_size"],
                custom_where=table_config.get("custom_where"),
                chunk_mode=chunk_settings["chunk_mode"],
                enable_checkpoint=chunk_settings["enable_checkpoint"],
                max_retries=chunk_settings["max_retries"]
            )
        else:
            logger.info(f"기존 방식으로 데이터 복사 시작: {table_name}")
            monitoring.add_checkpoint("복사 방식", "기존 방식 (CSV 기반)")
            
            # 기존 방식 사용 (청크 방식 지원)
            copy_result = copy_engine.copy_table_data(
                source_table=table_config["source"],
                target_table=table_config["target"],
                primary_keys=table_config["primary_key"],
                sync_mode=table_config["sync_mode"],
                batch_size=table_config["batch_size"],
                custom_where=table_config.get("custom_where"),
                chunk_mode=chunk_settings["chunk_mode"],
                enable_checkpoint=chunk_settings["enable_checkpoint"],
                max_retries=chunk_settings["max_retries"]
            )

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
            table_config["source"], 
            table_config["target"], 
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
        from common import ErrorHandler
        error_handler = ErrorHandler(context)
        error_handler.handle_data_error("테이블 복사", table_config['source'], e)


def execute_dbt_pipeline(**context) -> dict[str, Any]:
    """dbt 파이프라인 실행"""
    try:
        # 모니터링 시작
        monitoring = MonitoringManager("dbt 파이프라인 실행")
        monitoring.start_monitoring()

        # 진행 상황 추적
        progress = ProgressTracker(4, "dbt 파이프라인 실행")

        # 1단계: dbt 통합 객체 생성
        progress.start_step("dbt 통합 객체 생성", "DBTIntegration 객체 초기화")
        dbt_integration = DBTIntegration(DBT_PROJECT_PATH)
        progress.complete_step("dbt 통합 객체 생성")

        monitoring.add_checkpoint("dbt 객체 생성", "DBTIntegration 객체 생성 완료")

        # 2단계: dbt 프로젝트 검증
        progress.start_step("dbt 프로젝트 검증", "dbt 프로젝트 설정 및 연결 검증")
        validation_result = dbt_integration.validate_dbt_project()
        progress.complete_step("dbt 프로젝트 검증", validation_result)

        if not validation_result["is_valid"]:
            progress.fail_step("dbt 프로젝트 검증", validation_result["error"])
            monitoring.add_error(
                f"dbt 프로젝트 검증 실패: {validation_result['error']}"
            )
            monitoring.stop_monitoring("failed")
            raise Exception(f"dbt 프로젝트 검증 실패: {validation_result['error']}")

        monitoring.add_checkpoint("dbt 검증", "dbt 프로젝트 검증 성공")

        # 3단계: dbt 파이프라인 실행
        progress.start_step("dbt 파이프라인 실행", "스냅샷 -> run -> test 순서로 실행")

        pipeline_config = {
            "run_snapshot": True,  # ✅ 스냅샷 실행 (데이터 변경사항 추적)
            "run_models": False,  # ❌ 현재는 모델 실행 비활성화 (향후 확장 시 True로 변경)
            "run_tests": False,  # ❌ 현재는 테스트 실행 비활성화 (향후 확장 시 True로 변경)
            "cleanup": True,  # ✅ 정리 작업
            # 향후 확장을 위한 선택적 실행 설정
            "snapshot_select": "tag:infomax",  # 인포맥스 관련 스냅샷만 실행
            "run_select": "tag:infomax",  # 인포맥스 관련 모델만 실행 (향후 사용)
            "test_select": "tag:infomax",  # 인포맥스 관련 테스트만 실행 (향후 사용)
        }

        pipeline_result = dbt_integration.execute_dbt_pipeline(pipeline_config)
        progress.complete_step("dbt 파이프라인 실행", pipeline_result)

        if pipeline_result["status"] == "error":
            # 스냅샷/런/테스트 중 하나라도 실패하면 태스크를 실패로 처리
            monitoring.add_error(
                f"dbt 파이프라인 실행 실패: {pipeline_result['message']}"
            )
            monitoring.stop_monitoring("failed")
            raise Exception(
                f"dbt 파이프라인 실패: {pipeline_result['message']} | details={pipeline_result.get('results')}"
            )
        else:
            monitoring.add_checkpoint("dbt 파이프라인", "dbt 파이프라인 실행 완료")

        # 4단계: 결과 요약
        progress.start_step("결과 요약", "dbt 파이프라인 실행 결과 정리")

        # 모델 상태 조회
        models_status = dbt_integration.get_dbt_models_status()
        progress.complete_step("결과 요약", models_status)

        monitoring.add_checkpoint(
            "결과 요약", f"dbt 모델 {models_status.get('count', 0)}개 상태 조회 완료"
        )

        # 모니터링 종료
        monitoring.stop_monitoring("completed")

        return {
            "status": "success",
            "validation_result": validation_result,
            "pipeline_result": pipeline_result,
            "models_status": models_status,
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
