"""
하이브리드 데이터 복사 DAG
여러 타겟 데이터베이스로 데이터를 복사하는 DAG

지원하는 동기화 모드:
- full_sync: 전체 테이블 동기화
- incremental_sync: 증분 동기화 (타임스탬프 기반)
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 공통 모듈 import
from common.data_copy_engine import DataCopyEngine
from common.database_operations import DatabaseOperations
from common.monitoring import MonitoringManager, ProgressTracker
from common.error_handler import ErrorHandler
from common.connection_manager import ConnectionManager
from common.dag_config_manager import DAGConfigManager
from common.settings import DAGSettings, BatchSettings
from common.dag_utils import UtilsDAGConfigManager

# 로거 설정
logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = DAGSettings.get_default_args()

# DAG 정의
dag = DAG(
    "hybrid_data_copy_dag",
    default_args=default_args,
    description="Hybrid data copy from DigitalOcean PostgreSQL to multiple target databases",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "data-copy", "etl", "hybrid", "multi-target", "digitalocean"],
    max_active_runs=1,
)

# 연결 ID 설정 (DAGConfigManager에서 가져오기)
dag_config = DAGConfigManager.get_dag_config("hybrid_data_copy_dag")
SOURCE_CONN_ID = dag_config.get("source_connection", "digitalocean_postgres")

# 하이브리드 테이블 설정 (DAGConfigManager에서 가져오기)
HYBRID_TABLES_CONFIG = DAGConfigManager.get_table_configs("hybrid_data_copy_dag")

# 데이터 복사 엔진 초기화
data_copy_engine = DataCopyEngine()
db_operations = DatabaseOperations()
monitoring_manager = MonitoringManager()
error_handler = ErrorHandler()

def validate_all_connections(**context) -> bool:
    """모든 연결 유효성 검사"""
    try:
        # 소스 연결 검사
        source_valid = ConnectionManager.test_connection(SOURCE_CONN_ID)
        if not source_valid:
            logger.error(f"❌ 소스 연결 실패: {SOURCE_CONN_ID}")
            return False
        
        logger.info(f"✅ 소스 연결 성공: {SOURCE_CONN_ID}")
        
        # 타겟 연결들 검사
        target_conn_ids = set()
        for table_config in HYBRID_TABLES_CONFIG:
            for target_config in table_config['targets']:
                target_conn_ids.add(target_config['conn_id'])
        
        all_targets_valid = True
        for conn_id in target_conn_ids:
            try:
                target_valid = ConnectionManager.test_connection(conn_id)
                if target_valid:
                    logger.info(f"✅ 타겟 연결 성공: {conn_id}")
                else:
                    logger.error(f"❌ 타겟 연결 실패: {conn_id}")
                    all_targets_valid = False
            except Exception as e:
                logger.error(f"❌ 타겟 연결 검사 중 오류: {conn_id} - {str(e)}")
                all_targets_valid = False
        
        return all_targets_valid
        
    except Exception as e:
        logger.error(f"❌ 연결 검사 중 오류 발생: {str(e)}")
        raise

def copy_table_to_targets(table_config: dict, **context) -> bool:
    """테이블을 여러 타겟으로 복사"""
    try:
        source_table = table_config['source']
        logger.info(f"🔄 테이블 복사 시작: {source_table}")
        
        success_count = 0
        total_targets = len(table_config['targets'])
        
        for target_config in table_config['targets']:
            try:
                logger.info(f"📤 {source_table} -> {target_config['table']} 복사 중...")
                
                # 데이터 복사 실행
                result = data_copy_engine.copy_data_with_custom_sql(
                    source_conn_id=SOURCE_CONN_ID,
                    target_conn_id=target_config['conn_id'],
                    source_table=source_table,
                    target_table=target_config['table'],
                    primary_key=table_config['primary_key'],
                    sync_mode=target_config['sync_mode'],
                    batch_size=target_config.get('batch_size', 10000),
                    chunk_mode=table_config.get('chunk_mode', True),
                    enable_checkpoint=table_config.get('enable_checkpoint', True),
                    max_retries=table_config.get('max_retries', 3),
                    incremental_field=table_config.get('incremental_field'),
                    incremental_field_type=table_config.get('incremental_field_type'),
                    custom_where=target_config.get('custom_where'),
                    description=f"{table_config['source_description']} - {target_config['description']}"
                )
                
                if result:
                    logger.info(f"✅ {source_table} -> {target_config['table']} 복사 완료")
                    success_count += 1
                else:
                    logger.error(f"❌ {source_table} -> {target_config['table']} 복사 실패")
                    
            except Exception as e:
                logger.error(f"❌ {source_table} -> {target_config['table']} 복사 중 오류: {str(e)}")
                error_handler.handle_error(e, context)
        
        if success_count == total_targets:
            logger.info(f"✅ {source_table}: 모든 타겟으로 복사 완료 ({success_count}/{total_targets})")
            return True
        else:
            logger.warning(f"⚠️ {source_table}: 일부 타겟 복사 실패 ({success_count}/{total_targets})")
            return False
            
    except Exception as e:
        logger.error(f"❌ 테이블 복사 중 오류 발생: {source_table} - {str(e)}")
        error_handler.handle_error(e, context)
        return False

def verify_hybrid_data_integrity(**context) -> bool:
    """하이브리드 데이터 무결성 검증"""
    try:
        logger.info("🔍 하이브리드 데이터 무결성 검증 시작")
        
        total_verifications = 0
        successful_verifications = 0
        
        for table_config in HYBRID_TABLES_CONFIG:
            source_table = table_config['source']
            
            # 소스 테이블 행 수 확인
            try:
                source_count = db_operations.get_table_row_count(
                    conn_id=SOURCE_CONN_ID,
                    table_name=source_table
                )
                logger.info(f"📊 {source_table}: 소스 {source_count}행")
            except Exception as e:
                logger.error(f"❌ {source_table} 소스 행 수 확인 실패: {str(e)}")
                continue
            
            # 각 타겟 테이블 행 수 확인 및 비교
            for target_config in table_config['targets']:
                try:
                    target_count = db_operations.get_table_row_count(
                        conn_id=target_config['conn_id'],
                        table_name=target_config['table']
                    )
                    
                    total_verifications += 1
                    
                    if source_count == target_count:
                        logger.info(f"✅ {source_table} -> {target_config['table']}: {source_count}행 일치")
                        successful_verifications += 1
                    else:
                        logger.warning(f"⚠️ {source_table} -> {target_config['table']}: 소스 {source_count}행, 타겟 {target_count}행 불일치")
                        
                except Exception as e:
                    logger.error(f"❌ {target_config['table']} 타겟 행 수 확인 실패: {str(e)}")
        
        logger.info(f"✅ 하이브리드 데이터 무결성 검증 완료: {successful_verifications}/{total_verifications}")
        return successful_verifications == total_verifications
        
    except Exception as e:
        logger.error(f"❌ 하이브리드 데이터 무결성 검증 중 오류 발생: {str(e)}")
        error_handler.handle_error(e, context)
        return False

def generate_copy_summary(**context) -> bool:
    """복사 작업 요약 생성"""
    try:
        logger.info("📋 복사 작업 요약 생성 시작")
        
        summary = {
            "source_database": SOURCE_CONN_ID,
            "total_tables": len(HYBRID_TABLES_CONFIG),
            "total_targets": sum(len(table_config['targets']) for table_config in HYBRID_TABLES_CONFIG),
            "copy_timestamp": datetime.now().isoformat(),
            "tables": []
        }
        
        for table_config in HYBRID_TABLES_CONFIG:
            table_summary = {
                "source_table": table_config['source'],
                "targets": [
                    {
                        "conn_id": target_config['conn_id'],
                        "table": target_config['table'],
                        "sync_mode": target_config['sync_mode'],
                        "description": target_config['description']
                    }
                    for target_config in table_config['targets']
                ]
            }
            summary["tables"].append(table_summary)
        
        # 요약을 로그로 출력
        logger.info(f"📋 복사 작업 요약:")
        logger.info(f"   소스 데이터베이스: {summary['source_database']}")
        logger.info(f"   총 테이블 수: {summary['total_tables']}")
        logger.info(f"   총 타겟 수: {summary['total_targets']}")
        logger.info(f"   복사 완료 시간: {summary['copy_timestamp']}")
        
        for table_summary in summary["tables"]:
            logger.info(f"   📊 {table_summary['source_table']}:")
            for target in table_summary['targets']:
                logger.info(f"     → {target['table']} ({target['sync_mode']})")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 복사 작업 요약 생성 중 오류 발생: {str(e)}")
        error_handler.handle_error(e, context)
        return False

# 태스크 정의
validate_connections_task = PythonOperator(
    task_id="validate_all_connections",
    python_callable=validate_all_connections,
    dag=dag,
)

# 테이블별 복사 태스크 생성
copy_tasks = []
for i, table_config in enumerate(HYBRID_TABLES_CONFIG):
    task = PythonOperator(
        task_id=f"copy_table_{i}_{table_config['source'].replace('.', '_').replace('-', '_')}",
        python_callable=copy_table_to_targets,
        op_kwargs={"table_config": table_config},
        dag=dag,
    )
    copy_tasks.append(task)

verify_integrity_task = PythonOperator(
    task_id="verify_hybrid_data_integrity",
    python_callable=verify_hybrid_data_integrity,
    dag=dag,
)

generate_summary_task = PythonOperator(
    task_id="generate_copy_summary",
    python_callable=generate_copy_summary,
    dag=dag,
)

# 태스크 의존성 설정
validate_connections_task >> copy_tasks
copy_tasks >> verify_integrity_task
verify_integrity_task >> generate_summary_task 