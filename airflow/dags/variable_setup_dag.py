"""
DAG for setting up Airflow Variables for table configurations and copy methods
"""

import json
from datetime import datetime
import os # Added for os.getenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# DAG configuration
dag_config = {
    "dag_id": "variable_setup_dag",
    "schedule_interval": "@once",  # Run once manually
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "default_args": {"retries": 1},
    "tags": ["setup", "variables", "configuration"],
}

# DAG configurations (DBT별 설정)
DAG_CONFIGS = {
    "postgres_multi_table_copy_refactored": {
        "dag_id": "postgres_multi_table_copy_refactored",
        "description": "PostgreSQL Multi-Table Data Copy DAG (Refactored)",
        "schedule_interval": "@daily",
        "enabled": True,
        "tags": ["postgres", "data-copy", "etl", "refactored"],
        "source_connection": "fs2_postgres",
        "target_connection": "postgres_default",
        "tables": ["인포맥스종목마스터", "ff_v3_ff_sec_entity", "sym_v1_sym_ticker_exchange", "sym_v1_sym_coverage"],
        "execution_status": "active",
        "last_successful_run": "2025-08-21T01:08:45.430516+00:00",
        "avg_execution_time_minutes": 45,
        "success_rate_percent": 95.0
    },
    "digitalocean_data_copy_dag": {
        "dag_id": "digitalocean_data_copy_dag",
        "description": "DigitalOcean PostgreSQL Data Copy DAG",
        "schedule_interval": "@daily",
        "enabled": True,
        "tags": ["postgres", "data-copy", "etl", "digitalocean", "dbt-snapshot"],
        "source_connection": "digitalocean_postgres",
        "target_connection": "postgres_default",
        "tables": ["public.users", "public.orders"],
        "execution_status": "pending",
        "last_successful_run": None,
        "avg_execution_time_minutes": None,
        "success_rate_percent": None
    },
    "hybrid_data_copy_dag": {
        "dag_id": "hybrid_data_copy_dag",
        "description": "Hybrid Data Copy DAG (DigitalOcean to Multiple Targets)",
        "schedule_interval": "@daily",
        "enabled": True,
        "tags": ["postgres", "data-copy", "etl", "hybrid", "multi-target", "digitalocean"],
        "source_connection": "digitalocean_postgres",
        "target_connections": ["postgres_default", "fs2_postgres"],
        "tables": ["public.users", "public.orders"],
        "execution_status": "pending",
        "last_successful_run": None,
        "avg_execution_time_minutes": None,
        "success_rate_percent": None
    },
    "postgres_data_copy_dag": {
        "dag_id": "postgres_data_copy_dag",
        "description": "PostgreSQL Multi-Table Data Copy DAG (Original)",
        "schedule_interval": "@daily",
        "enabled": False,  # 리팩토링된 버전으로 대체
        "tags": ["postgres", "data-copy", "etl", "multi-table", "dbt-snapshot"],
        "source_connection": "fs2_postgres",
        "target_connection": "postgres_default",
        "tables": ["fds_팩셋.인포맥스종목마스터"],
        "execution_status": "deprecated",
        "last_successful_run": None,
        "avg_execution_time_minutes": None,
        "success_rate_percent": None
    },
    "dbt_processing_dag": {
        "dag_id": "dbt_processing_dag",
        "description": "DBT Processing DAG",
        "schedule_interval": "@daily",
        "enabled": True,
        "tags": ["dbt", "data-transformation", "etl"],
        "source_connection": "postgres_default",
        "target_connection": "postgres_default",
        "tables": ["staging", "marts"],
        "execution_status": "active",
        "last_successful_run": None,
        "avg_execution_time_minutes": None,
        "success_rate_percent": None
    }
}

# Table configurations (테이블별 설정 - copy_method와 sync_mode 통합)
TABLE_CONFIGS = {
    "인포맥스종목마스터": {
        "dag_id": "postgres_multi_table_copy_refactored",
        "source_connection": "fs2_postgres",
        "source_schema": "fds_팩셋",
        "source_table": "fds_팩셋.인포맥스종목마스터",
        "target_connection": "postgres_default",
        "target_schema": "raw_data",
        "target_table": "raw_data.인포맥스종목마스터",
        "primary_key": ["인포맥스코드", "팩셋거래소", "gts_exnm", "티커"],
        "sync_mode": "full_sync",
        "batch_size": 10000,
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 5,
        "description": "인포맥스 종목 마스터 - 청크 방식으로 안전하게 처리"
    },
    "ff_v3_ff_sec_entity": {
        "dag_id": "postgres_multi_table_copy_refactored",
        "source_connection": "fs2_postgres",
        "source_schema": "fds_copy",
        "source_table": "fds_copy.ff_v3_ff_sec_entity",
        "target_connection": "postgres_default",
        "target_schema": "raw_data",
        "target_table": "raw_data.ff_v3_ff_sec_entity",
        "primary_key": ["fsym_id"],
        "sync_mode": "full_sync",
        "batch_size": 20000,
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 5,
        "description": "FF v3 보안 엔티티 - 청크 방식으로 안전하게 처리"
    },
    "sym_v1_sym_ticker_exchange": {
        "dag_id": "postgres_multi_table_copy_refactored",
        "source_connection": "fs2_postgres",
        "source_schema": "fds_copy",
        "source_table": "fds_copy.sym_v1_sym_ticker_exchange",
        "target_connection": "postgres_default",
        "target_schema": "raw_data",
        "target_table": "raw_data.sym_v1_sym_ticker_exchange",
        "primary_key": ["fsym_id"],
        "sync_mode": "full_sync",
        "batch_size": 10000,
        "custom_where": "SPLIT_PART(ticker_exchange, '-', 2) IN ('AMS','BRU','FRA','HKG','HSTC','JAS','JKT','KRX','LIS','LON','NAS','NYS','PAR','ROCO','SES','SHE','SHG','STC','TAI','TKS','TSE')",
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 3,
        "description": "심볼 티커 거래소 - 청크 방식으로 안전하게 처리"
    },
    "sym_v1_sym_coverage": {
        "dag_id": "postgres_multi_table_copy_refactored",
        "source_connection": "fs2_postgres",
        "source_schema": "fds_copy",
        "source_table": "fds_copy.sym_v1_sym_coverage",
        "target_connection": "postgres_default",
        "target_schema": "raw_data",
        "target_table": "raw_data.sym_v1_sym_coverage",
        "primary_key": ["fsym_id"],
        "sync_mode": "full_sync",
        "batch_size": 5000,
        "custom_where": "fsym_id like '%-L' AND fref_security_type NOT IN ('ETF_UVI','ETF_NAV','NVDR','ALIEN','RIGHT','WARRANT') AND universe_type = 'EQ'",
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 3,
        "description": "심볼 커버리지 - 청크 방식으로 안전하게 처리"
    },
    "edi_690": {
        "dag_id": "postgres_multi_table_copy_refactored",
        "source_connection": "fs2_postgres",
        "source_schema": "m23",
        "source_table": "m23.edi_690",
        "target_connection": "postgres_default",
        "target_schema": "raw_data",
        "target_table": "raw_data.edi_690",
        "primary_key": ["eventcd", "eventid", "optionid", "serialid", "scexhid", "sedolid"],
        "sync_mode": "incremental_sync",
        "batch_size": 10000,
        "incremental_field": "changed",
        "incremental_field_type": "yyyymmdd",
        "custom_where": "changed >= '20250812'",
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 3,
        "description": "EDI 690 이벤트 데이터 - 청크 방식으로 안전하게 처리"
    },
    "digitalocean_users": {
        "dag_id": "digitalocean_data_copy_dag",
        "source_connection": "digitalocean_postgres",
        "source_schema": "public",
        "source_table": "public.users",
        "target_connection": "postgres_default",
        "target_schema": "raw_data",
        "target_table": "raw_data.digitalocean_users",
        "primary_key": ["id"],
        "sync_mode": "full_sync",
        "batch_size": 10000,
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 3,
        "description": "DigitalOcean 사용자 테이블 - 전체 동기화"
    },
    "digitalocean_orders": {
        "dag_id": "digitalocean_data_copy_dag",
        "source_connection": "digitalocean_postgres",
        "source_schema": "public",
        "source_table": "public.orders",
        "target_connection": "postgres_default",
        "target_schema": "raw_data",
        "target_table": "raw_data.digitalocean_orders",
        "primary_key": ["order_id"],
        "sync_mode": "incremental_sync",
        "batch_size": 5000,
        "incremental_field": "created_at",
        "incremental_field_type": "timestamp",
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 3,
        "description": "DigitalOcean 주문 테이블 - 증분 동기화"
    }
}

# Sync mode configurations (동기화 모드별 설정 - copy_method와 sync_mode 통합)
SYNC_MODE_CONFIGS = {
    "full_sync": {
        "description": "Full table sync - truncate and insert all data",
        "truncate_before_copy": True,
        "parallel_workers": 4,
        "timeout_minutes": 30,
        "sync_mode": "full_sync",
    },
    "incremental_sync": {
        "description": "Incremental sync - only new/modified records",
        "truncate_before_copy": False,
        "parallel_workers": 2,
        "timeout_minutes": 15,
        "sync_mode": "incremental_sync",
        "upsert_strategy": "insert_on_conflict",
    }
}

# Execution monitoring configurations (새로 추가)
EXECUTION_MONITORING_CONFIGS = {
    "monitoring_enabled": True,
    "alert_thresholds": {
        "execution_time_minutes": 60,
        "success_rate_percent": 90.0,
        "error_count": 3,
        "memory_usage_mb": 2048
    },
    "notification_channels": {
        "email": ["admin@example.com", "data-team@example.com"],
        "slack": "#data-pipeline-alerts",
        "webhook": "https://hooks.slack.com/services/..."
    },
    "retry_policies": {
        "max_retries": 3,
        "retry_delay_minutes": 5,
        "exponential_backoff": True
    },
    "checkpoint_settings": {
        "enable_checkpoint": True,
        "checkpoint_interval_rows": 10000,
        "checkpoint_timeout_minutes": 30
    }
}

# dbt configurations
DBT_CONFIGS = {
    "snapshot_tables": ["인포맥스종목마스터", "ff_v3_ff_sec_entity", "edi_690"],
    "snapshot_strategy": "timestamp",
    "snapshot_updated_at": "changed",
    "models_to_run": ["staging", "marts"],
    "test_after_run": True,
    "full_refresh": False,
    "project_path": "/opt/airflow/dbt",
    "profile_name": "postgres_data_copy",
    "target_name": "dev",
    "snapshot_select": "tag:infomax",
    "run_select": "tag:infomax",
    "test_select": "tag:infomax",
}

# 청크 방식 데이터 복사 설정
CHUNK_MODE_CONFIGS = {
    "default_settings": {
        "chunk_mode": True,
        "enable_checkpoint": True,
        "max_retries": 3,
        "description": "기본 청크 방식 설정 - 모든 테이블에 적용"
    },
    "size_based_recommendations": {
        "small": {  # 10만 행 이하
            "chunk_mode": False,
            "enable_checkpoint": False,
            "batch_size": 1000,
            "max_retries": 2,
            "reason": "소용량 테이블은 기존 방식이 더 빠름"
        },
        "medium": {  # 10만 ~ 100만 행
            "chunk_mode": True,
            "enable_checkpoint": True,
            "batch_size": 5000,
            "max_retries": 3,
            "reason": "중간 크기 테이블은 청크 방식으로 안전하게 처리"
        },
        "large": {  # 100만 ~ 1000만 행
            "chunk_mode": True,
            "enable_checkpoint": True,
            "batch_size": 10000,
            "max_retries": 5,
            "reason": "대용량 테이블은 청크 방식으로만 안전하게 처리 가능"
        },
        "xlarge": {  # 1000만 행 이상
            "chunk_mode": True,
            "enable_checkpoint": True,
            "batch_size": 15000,
            "max_retries": 7,
            "reason": "초대용량 테이블은 청크 방식과 체크포인트가 필수"
        }
    },
    "environment_settings": {
        "development": {
            "default_chunk_mode": True,
            "default_enable_checkpoint": True,
            "default_max_retries": 3,
            "default_batch_size": 5000,
            "description": "개발 환경 - 안전한 청크 방식 기본값"
        },
        "staging": {
            "default_chunk_mode": True,
            "default_enable_checkpoint": True,
            "default_max_retries": 4,
            "default_batch_size": 8000,
            "description": "스테이징 환경 - 안전한 청크 방식, 중간 배치 크기"
        },
        "production": {
            "default_chunk_mode": True,
            "default_enable_checkpoint": True,
            "default_max_retries": 5,
            "default_batch_size": 10000,
            "description": "프로덕션 환경 - 안전한 청크 방식, 최적화된 배치 크기"
        }
    }
}


def setup_table_variables(**context):
    """Set up table configuration variables"""
    Variable.set("table_configs", json.dumps(TABLE_CONFIGS, indent=2))
    print(f"Set table_configs variable with {len(TABLE_CONFIGS)} tables")
    return f"Successfully set {len(TABLE_CONFIGS)} table configurations"


def setup_sync_mode_variables(**context):
    """Set up sync mode configuration variables"""
    Variable.set("sync_mode_configs", json.dumps(SYNC_MODE_CONFIGS, indent=2))
    print(f"Set sync_mode_configs variable with {len(SYNC_MODE_CONFIGS)} sync modes")
    return f"Successfully set {len(SYNC_MODE_CONFIGS)} sync mode configurations"


def setup_dbt_variables(**context):
    """Set up dbt configuration variables"""
    Variable.set("dbt_configs", json.dumps(DBT_CONFIGS, indent=2))
    print(f"Set dbt_configs variable with {len(DBT_CONFIGS)} dbt configurations")
    return f"Successfully set {len(DBT_CONFIGS)} dbt configurations"


def setup_chunk_mode_variables(**context):
    """Set up chunk mode configuration variables"""
    # 설정 버전 관리 추가
    from datetime import datetime
    
    config_with_version = {
        "version": "1.0.0",
        "created_at": datetime.now().isoformat(),
        "environment": os.getenv("AIRFLOW_ENV", "development"),
        "configs": CHUNK_MODE_CONFIGS
    }
    
    Variable.set("chunk_mode_configs", json.dumps(config_with_version, indent=2))
    print(f"Set chunk_mode_configs variable with chunk mode configurations")
    print(f"  - Version: {config_with_version['version']}")
    print(f"  - Created at: {config_with_version['created_at']}")
    print(f"  - Environment: {config_with_version['environment']}")
    print("  - Default settings configured")
    print("  - Size-based recommendations configured")
    print("  - Environment-specific settings configured")
    return "Successfully set chunk mode configurations"


def setup_dag_variables(**context):
    """Set up DAG configuration variables"""
    Variable.set("dag_configs", json.dumps(DAG_CONFIGS, indent=2))
    print(f"Set dag_configs variable with {len(DAG_CONFIGS)} DAG configurations")
    return f"Successfully set {len(DAG_CONFIGS)} DAG configurations"


def setup_monitoring_variables(**context):
    """Set up execution monitoring configuration variables"""
    Variable.set("execution_monitoring_configs", json.dumps(EXECUTION_MONITORING_CONFIGS, indent=2))
    print(f"Set execution_monitoring_configs variable with {len(EXECUTION_MONITORING_CONFIGS)} monitoring configurations")
    return f"Successfully set {len(EXECUTION_MONITORING_CONFIGS)} monitoring configurations"


def verify_variables(**context):
    """Verify all variables are set correctly"""
    try:
        table_configs = json.loads(Variable.get("table_configs"))
        sync_mode_configs = json.loads(Variable.get("sync_mode_configs"))
        dbt_configs = json.loads(Variable.get("dbt_configs"))
        chunk_mode_configs = json.loads(Variable.get("chunk_mode_configs"))
        dag_configs = json.loads(Variable.get("dag_configs"))
        monitoring_configs = json.loads(Variable.get("execution_monitoring_configs"))

        print("✅ All variables verified successfully:")
        print(f"  - Table configs: {len(table_configs)} tables")
        print(f"  - Sync modes: {len(sync_mode_configs)} modes")
        print(f"  - dbt configs: {len(dbt_configs)} configurations")
        print(f"  - Chunk mode configs: {len(chunk_mode_configs)} configurations")
        print(f"  - DAG configs: {len(dag_configs)} DAGs")
        print(f"  - Monitoring configs: {len(monitoring_configs)} monitoring configurations")
        
        # 청크 방식 설정 검증
        print("\n🔍 Chunk mode configuration verification:")
        for table_name, config in table_configs.items():
            chunk_mode = config.get("chunk_mode", True)
            enable_checkpoint = config.get("enable_checkpoint", True)
            max_retries = config.get("max_retries", 3)
            sync_mode = config.get("sync_mode", "unknown")
            
            print(f"  - {table_name}:")
            print(f"    * Chunk mode: {'✅ 활성화' if chunk_mode else '❌ 비활성화'}")
            print(f"    * Checkpoint: {'✅ 활성화' if enable_checkpoint else '❌ 비활성화'}")
            print(f"    * Max retries: {max_retries}")
            print(f"    * Sync mode: {sync_mode}")
            
            # 설정 유효성 검증
            if enable_checkpoint and not chunk_mode:
                print(f"    ⚠️  경고: 체크포인트는 청크 모드가 활성화된 경우에만 사용 가능")

        return "All variables verified successfully"
    except Exception as e:
        print(f"❌ Error verifying variables: {e}")
        raise


# Create DAG
dag = DAG(**dag_config)

# Create tasks
setup_tables_task = PythonOperator(
    task_id="setup_table_variables",
    python_callable=setup_table_variables,
    dag=dag,
)

setup_sync_methods_task = PythonOperator(
    task_id="setup_sync_mode_variables",
    python_callable=setup_sync_mode_variables,
    dag=dag,
)

setup_dbt_task = PythonOperator(
    task_id="setup_dbt_variables",
    python_callable=setup_dbt_variables,
    dag=dag,
)

setup_chunk_mode_task = PythonOperator(
    task_id="setup_chunk_mode_variables",
    python_callable=setup_chunk_mode_variables,
    dag=dag,
)

setup_dag_task = PythonOperator(
    task_id="setup_dag_variables",
    python_callable=setup_dag_variables,
    dag=dag,
)

setup_monitoring_task = PythonOperator(
    task_id="setup_monitoring_variables",
    python_callable=setup_monitoring_variables,
    dag=dag,
)

verify_task = PythonOperator(
    task_id="verify_variables",
    python_callable=verify_variables,
    dag=dag,
)

# Set task dependencies
(
    setup_tables_task
    >> setup_sync_methods_task
    >> setup_dbt_task
    >> setup_chunk_mode_task
    >> setup_dag_task
    >> setup_monitoring_task
    >> verify_task
)
