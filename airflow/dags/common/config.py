"""
Common configuration and utilities for DAGs
"""

from datetime import datetime
from pathlib import Path
from typing import Any

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# dbt project path
DBT_ROOT_PATH = Path("/opt/airflow/dbt")

# Default DAG configuration
DEFAULT_DAG_CONFIG = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "default_args": {"retries": 2},
    "tags": ["data-pipeline", "postgres", "dbt"],
}

# Default execution configuration
DEFAULT_EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",
)

# Default operator arguments
DEFAULT_OPERATOR_ARGS = {
    "install_deps": True,
    "full_refresh": False,
}


def get_profile_config(
    profile_name: str = "postgres_data_copy",
    target_name: str = "dev",
    schema: str = "raw_data",
    conn_id: str = "airflow_db",
) -> ProfileConfig:
    """
    Get profile configuration for dbt

    Args:
        profile_name: dbt profile name
        target_name: dbt target name
        schema: database schema
        conn_id: Airflow connection ID

    Returns:
        ProfileConfig object
    """
    return ProfileConfig(
        profile_name=profile_name,
        target_name=target_name,
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=conn_id,
            profile_args={
                "schema": schema,
                "host": "{{ env_var('POSTGRES_HOST', 'postgres') }}",
                "user": "{{ env_var('POSTGRES_USER', 'airflow') }}",
                "password": "{{ env_var('POSTGRES_PASSWORD', 'airflow') }}",
                "port": "{{ env_var('POSTGRES_PORT', '15432') }}",
                "dbname": "{{ env_var('POSTGRES_DB', 'airflow') }}",
            },
        ),
    )


def get_project_config(dbt_path: Path | None = None) -> ProjectConfig:
    """
    Get dbt project configuration

    Args:
        dbt_path: Path to dbt project (defaults to DBT_ROOT_PATH)

    Returns:
        ProjectConfig object
    """
    return ProjectConfig(dbt_path or DBT_ROOT_PATH)


def get_dag_config(
    dag_id: str, schedule_interval: str = "@daily", **kwargs
) -> dict[str, Any]:
    """
    Get DAG configuration with defaults

    Args:
        dag_id: DAG ID
        schedule_interval: Schedule interval
        **kwargs: Additional configuration

    Returns:
        DAG configuration dictionary
    """
    config = DEFAULT_DAG_CONFIG.copy()
    config.update(
        {
            "dag_id": dag_id,
            "schedule_interval": schedule_interval,
        }
    )
    config.update(kwargs)
    return config
