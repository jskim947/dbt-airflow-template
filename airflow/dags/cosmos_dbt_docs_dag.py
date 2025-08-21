"""
Cosmos DAG specifically for dbt docs generation
Based on official Astronomer Cosmos examples
"""

from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# dbt project path
DBT_ROOT_PATH = Path("/opt/airflow/dbt")

# Profile configuration
profile_config = ProfileConfig(
    profile_name="example_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={
            "schema": "example_dbt",
            "host": "{{ env_var('POSTGRES_HOST', 'postgres') }}",
            "user": "{{ env_var('POSTGRES_USER', 'airflow') }}",
            "password": "{{ env_var('POSTGRES_PASSWORD', 'airflow') }}",
            "port": 15432,  # 외부 포트로 변경
            "dbname": "{{ env_var('POSTGRES_DB', 'airflow') }}",
        },
    ),
)

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
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="cosmos_dbt_docs_dag",
    default_args={"retries": 2},
    tags=["cosmos", "dbt", "docs"],
)
