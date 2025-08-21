"""
PostgreSQL EDI Data Copy DAG (Refactored)
EDI 관련 PostgreSQL 테이블을 복사하는 DAG - 리팩토링 버전

이 DAG는 다음 작업을 수행합니다:
1. EDI 소스 PostgreSQL에서 데이터 추출 (동기화 모드별)
2. EDI 데이터 변환 및 검증
3. 타겟 PostgreSQL에 EDI 데이터 로드
4. EDI 데이터 무결성 검사

동기화 모드:
- incremental_sync: 증분 동기화 (기존 데이터 유지)
- full_sync: 전체 동기화 (소스에 없는 데이터 삭제)

EDI 특화 기능:
- EDI 이벤트 데이터 처리
- 우선순위 필드 자동 변환
- EDI 상태 추적

참고: 이 DAG는 dbt 스냅샷을 포함하지 않으며, 순수한 EDI 데이터 복사에 집중합니다.
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
    dag_id="postgres_edi_data_copy_refactored",
    default_args=default_args,
    description="Copy EDI data from PostgreSQL tables (Refactored)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "data-copy", "etl", "refactored", "edi"],
    max_active_runs=1,
)

# 연결 ID 설정 (DAGConfigManager에서 가져오기)
dag_config = DAGConfigManager.get_dag_config("postgres_multi_table_copy_refactored")
SOURCE_CONN_ID = dag_config.get("source_connection", "fs2_postgres")
TARGET_CONN_ID = dag_config.get("target_connection", "postgres_default")

# EDI 테이블 설정 (공통 설정 사용)
EDI_TABLES_CONFIG = DAGSettings.get_edi_table_configs()


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
        from common import ErrorHandler
        error_handler = ErrorHandler(context)
        
        # 모니터링 종료 (실패 상태)
        if 'monitoring' in locals():
            monitoring.add_error(f"연결 검증 실패: {str(e)}")
            monitoring.stop_monitoring("failed")
        
        error_handler.handle_connection_error("연결 검증", "database", e)


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
        
        # EDI 데이터 복사에서 청크 방식 설정 (헬퍼 함수 사용)
        chunk_settings = get_chunk_mode_settings(table_config)
        
        # 청크 방식 설정 로깅 (EDI)
        if chunk_settings["chunk_mode"]:
            logger.info(f"EDI 청크 방식 데이터 복사 활성화: {table_name}")
            logger.info(f"체크포인트: {'활성화' if chunk_settings['enable_checkpoint'] else '비활성화'}")
            logger.info(f"최대 재시도: {chunk_settings['max_retries']}회")
            monitoring.add_checkpoint("EDI 청크 방식", f"활성화 (체크포인트: {'활성화' if chunk_settings['enable_checkpoint'] else '비활성화'})")
        else:
            logger.info(f"EDI 기존 방식 데이터 복사: {table_name}")
            monitoring.add_checkpoint("EDI 데이터 복사 방식", "기존 방식 (메모리 누적)")
        
        # 성능 최적화 설정에 따라 스트리밍 파이프 또는 기존 방식 선택
        try:
            from common.settings import BatchSettings
            performance_config = BatchSettings.get_performance_optimization_config()
            use_streaming = performance_config.get("enable_streaming_pipe", False)
        except ImportError:
            use_streaming = False
        
        if use_streaming:
            logger.info(f"EDI 스트리밍 파이프 방식으로 데이터 복사 시작: {table_name}")
            monitoring.add_checkpoint("EDI 복사 방식", "스트리밍 파이프 (중간 파일 없음)")
            
            # 스트리밍 파이프 방식 사용
            copy_result = copy_engine.copy_data_with_streaming_pipe(
                source_table=table_config["source"],
                target_table=table_config["target"],
                primary_keys=table_config.get("primary_key", []),
                sync_mode=table_config.get("sync_mode", "incremental_sync"),
                batch_size=table_config.get("batch_size", 10000),
                custom_where=table_config.get("custom_where"),
                chunk_mode=chunk_settings["chunk_mode"],
                enable_checkpoint=chunk_settings["enable_checkpoint"],
                max_retries=chunk_settings["max_retries"]
            )
        else:
            logger.info(f"EDI 기존 방식으로 데이터 복사 시작: {table_name}")
            monitoring.add_checkpoint("EDI 복사 방식", "기존 방식 (CSV 기반)")
            
            # 기존 방식 사용 (청크 방식 지원)
            copy_result = copy_engine.copy_data_with_custom_sql(
                table_config["source"],
                table_config["target"],
                table_config.get("primary_key", []),
                table_config.get("sync_mode", "incremental_sync"),
                table_config.get("incremental_field"),
                table_config.get("incremental_field_type"),
                table_config.get("custom_where"),
                table_config.get("batch_size", 10000),
                verified_target_schema,  # 검증된 스키마 전달
                chunk_mode=chunk_settings["chunk_mode"],
                enable_checkpoint=chunk_settings["enable_checkpoint"],
                max_retries=chunk_settings["max_retries"]
            )
        
        progress.complete_step("EDI 데이터 복사 실행")

        monitoring.add_checkpoint(
            "데이터 복사", f"EDI 데이터 복사 완료: {copy_result.get('exported_rows', 0)}행 처리"
        )

        # 5단계: 데이터 무결성 검증
        progress.start_step(
            "EDI 데이터 무결성 검증", "복사된 EDI 데이터의 무결성 검증"
        )
        
        # 증분 동기화 무결성 검증 (EDI는 증분 동기화)
        where_clause = table_config.get("where_clause", "changed >= '20250812'")
        
        validation_result = db_ops.validate_incremental_data_integrity(
            table_config["source"], 
            table_config["target"], 
            where_clause
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
        from common import ErrorHandler
        error_handler = ErrorHandler(context)
        
        # 모니터링 종료 (실패 상태)
        if 'monitoring' in locals():
            monitoring.add_error(f"EDI 테이블 복사 실패: {str(e)}")
            monitoring.stop_monitoring("failed")
        
        error_handler.handle_data_error("EDI 테이블 복사", table_name, e)


# 태스크 정의
validate_connections_task = PythonOperator(
    task_id="validate_connections",
    python_callable=validate_connections,
    dag=dag,
)

# EDI 테이블별 복사 태스크 생성 (TaskFactory 사용)
from common import TaskFactory

edi_copy_tasks = TaskFactory.create_edi_copy_tasks(
    table_configs=EDI_TABLES_CONFIG,
    copy_function=copy_table_data,
    dag=dag
)

# 태스크 의존성 설정 (dbt 스냅샷 없이)
validate_connections_task >> edi_copy_tasks

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
