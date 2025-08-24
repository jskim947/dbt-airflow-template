"""
Cosmos DAG specifically for dbt docs generation
Based on official Astronomer Cosmos examples
"""

from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# DAGConfigManager import 추가
from common.dag_config_manager import DAGConfigManager

# dbt project path
DBT_ROOT_PATH = Path("/opt/airflow/dbt")

# Profile configuration
profile_config = ProfileConfig(
    profile_name="example_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={
            "host": "{{ conn.host }}",
            "user": "{{ conn.login }}",
            "password": "{{ conn.password }}",  # 하드코딩된 기본값 제거
            "port": "{{ conn.port }}",
            "dbname": "{{ conn.schema }}",
            "schema": "raw_data"
        }
    ),
)

# DAG 설정 가져오기
dag_config = DAGConfigManager.get_dag_config("dbt_processing_dag")

# dbt docs generation DAG
dbt_docs_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": False,
    },
    schedule_interval=dag_config.get("schedule_interval", "@daily"),  # 설정에서 가져오기
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="cosmos_dbt_docs_dag",
    default_args={"retries": 2},
    tags=dag_config.get("tags", ["cosmos", "dbt", "docs"]),
)
