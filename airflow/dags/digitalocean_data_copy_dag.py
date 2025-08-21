"""
DigitalOcean 데이터 복사 DAG
DigitalOcean PostgreSQL에서 로컬 PostgreSQL로 데이터를 복사하는 DAG

지원하는 동기화 모드:
- full_sync: 전체 테이블 동기화
- incremental_sync: 증분 동기화 (타임스탬프 기반)
"""

import logging
import os
import subprocess
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 공통 모듈 import
from common.data_copy_engine import DataCopyEngine
from common.database_operations import DatabaseOperations
from common.monitoring import MonitoringManager, ProgressTracker

from common.connection_manager import ConnectionManager
from common.dag_config_manager import DAGConfigManager
from common.settings import DAGSettings, BatchSettings

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
    "digitalocean_postgres_data_copy",
    default_args=default_args,
    description="Copy data from DigitalOcean PostgreSQL to local PostgreSQL with dbt snapshots",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "data-copy", "etl", "digitalocean", "dbt-snapshot"],
    max_active_runs=1,
)

# 연결 ID 설정 (DAGConfigManager에서 가져오기)
dag_config = DAGConfigManager.get_dag_config("digitalocean_data_copy_dag")
SOURCE_CONN_ID = dag_config.get("source_connection", "digitalocean_postgres")
TARGET_CONN_ID = dag_config.get("target_connection", "postgres_default")

# dbt 프로젝트 경로 설정
DBT_PROJECT_PATH = "/opt/airflow/dbt"

# DigitalOcean PostgreSQL 테이블 설정 (DAGConfigManager에서 가져오기)
DIGITALOCEAN_TABLES_CONFIG = DAGConfigManager.get_table_configs("digitalocean_data_copy_dag")

# 설정이 비어있거나 잘못된 경우 기본값 사용
if not DIGITALOCEAN_TABLES_CONFIG or not isinstance(DIGITALOCEAN_TABLES_CONFIG, list):
    logger.warning("DAGConfigManager에서 테이블 설정을 가져올 수 없어 기본값을 사용합니다.")
    DIGITALOCEAN_TABLES_CONFIG = [
        {
            "source": "public.users",
            "target": "public.users",
            "primary_key": "id",
            "sync_mode": "full_sync",
            "batch_size": 10000,
            "chunk_mode": True,
            "enable_checkpoint": True,
            "max_retries": 3,
            "description": "사용자 테이블"
        }
    ]

# 데이터베이스 작업 객체 초기화
db_operations = DatabaseOperations(SOURCE_CONN_ID, TARGET_CONN_ID)

# 데이터 복사 엔진 초기화
data_copy_engine = DataCopyEngine(db_operations)
monitoring_manager = MonitoringManager()


def validate_connections(**context) -> bool:
    """연결 유효성 검사"""
    try:
        # ConnectionManager를 사용하여 연결 검사
        source_valid = ConnectionManager.test_connection(SOURCE_CONN_ID)
        target_valid = ConnectionManager.test_connection(TARGET_CONN_ID)
        
        if source_valid and target_valid:
            logger.info(f"✅ 모든 연결 검증 성공")
            return True
        else:
            logger.error(f"❌ 연결 검증 실패: 소스={source_valid}, 타겟={target_valid}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 연결 검사 중 오류 발생: {str(e)}")
        raise

def copy_table_data(table_config: dict, **context) -> bool:
    """개별 테이블 데이터 복사"""
    try:
        logger.info(f"🔄 테이블 복사 시작: {table_config['source']} -> {table_config['target']}")
        
        # 데이터 복사 실행
        result = data_copy_engine.copy_data_with_custom_sql(
            source_conn_id=SOURCE_CONN_ID,
            target_conn_id=TARGET_CONN_ID,
            source_table=table_config['source'],
            target_table=table_config['target'],
            primary_key=table_config['primary_key'],
            sync_mode=table_config['sync_mode'],
            batch_size=table_config.get('batch_size', 10000),
            chunk_mode=table_config.get('chunk_mode', True),
            enable_checkpoint=table_config.get('enable_checkpoint', True),
            max_retries=table_config.get('max_retries', 3),
            incremental_field=table_config.get('incremental_field'),
            incremental_field_type=table_config.get('incremental_field_type'),
            custom_where=table_config.get('custom_where'),
            description=table_config.get('description', '')
        )
        
        if result:
            logger.info(f"✅ 테이블 복사 완료: {table_config['source']}")
            return True
        else:
            logger.error(f"❌ 테이블 복사 실패: {table_config['source']}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 테이블 복사 중 오류 발생: {table_config['source']} - {str(e)}")
        logger.error(f"오류 발생: {str(e)}")
        return False

def run_dbt_snapshot(**context) -> bool:
    """dbt 스냅샷 실행"""
    try:
        logger.info("🔄 dbt 스냅샷 실행 시작")
        
        # dbt 스냅샷 명령어 실행
        cmd = f"cd {DBT_PROJECT_PATH} && dbt snapshot"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ dbt 스냅샷 실행 완료")
            return True
        else:
            logger.error(f"❌ dbt 스냅샷 실행 실패: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ dbt 스냅샷 실행 중 오류 발생: {str(e)}")
        error_handler.handle_error(e, context)
        return False

def verify_data_integrity(**context) -> bool:
    """데이터 무결성 검증"""
    try:
        logger.info("🔍 데이터 무결성 검증 시작")
        
        # 각 테이블별 데이터 무결성 검사
        for table_config in DIGITALOCEAN_TABLES_CONFIG:
            source_count = db_operations.get_table_row_count(
                conn_id=SOURCE_CONN_ID,
                table_name=table_config['source']
            )
            target_count = db_operations.get_table_row_count(
                conn_id=TARGET_CONN_ID,
                table_name=table_config['target']
            )
            
            if source_count == target_count:
                logger.info(f"✅ {table_config['source']}: 소스 {source_count}행, 타겟 {target_count}행 일치")
            else:
                logger.warning(f"⚠️ {table_config['source']}: 소스 {source_count}행, 타겟 {target_count}행 불일치")
        
        logger.info("✅ 데이터 무결성 검증 완료")
        return True
        
    except Exception as e:
        logger.error(f"❌ 데이터 무결성 검증 중 오류 발생: {str(e)}")
        error_handler.handle_error(e, context)
        return False

# 태스크 정의
validate_connections_task = PythonOperator(
    task_id="validate_connections",
    python_callable=validate_connections,
    dag=dag,
)

# 테이블별 복사 태스크 생성
copy_tasks = []
for i, table_config in enumerate(DIGITALOCEAN_TABLES_CONFIG):
    task = PythonOperator(
        task_id=f"copy_table_{i}_{table_config['source'].replace('.', '_').replace('-', '_')}",
        python_callable=copy_table_data,
        op_kwargs={"table_config": table_config},
        dag=dag,
    )
    copy_tasks.append(task)

run_dbt_snapshot_task = PythonOperator(
    task_id="run_dbt_snapshot",
    python_callable=run_dbt_snapshot,
    dag=dag,
)

verify_integrity_task = PythonOperator(
    task_id="verify_data_integrity",
    python_callable=verify_data_integrity,
    dag=dag,
)

# 태스크 의존성 설정
validate_connections_task >> copy_tasks
copy_tasks >> run_dbt_snapshot_task
run_dbt_snapshot_task >> verify_integrity_task 