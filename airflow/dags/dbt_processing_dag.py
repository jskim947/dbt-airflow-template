"""
DAG for dbt processing including snapshots, models, and tests
Based on Cosmos framework for dbt orchestration
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# DAG configuration
dag_config = {
    "dag_id": "dbt_processing_dag",
    "schedule_interval": "@daily",
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "default_args": {"retries": 2},
    "tags": ["dbt", "data-transformation", "cosmos"],
}

logger = logging.getLogger(__name__)

# dbt project path
DBT_ROOT_PATH = Path("/opt/airflow/dbt")

# Profile configuration for dbt
profile_config = ProfileConfig(
    profile_name="postgres_data_copy",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={
            "schema": "raw_data",
            "host": "{{ env_var('POSTGRES_HOST', 'postgres') }}",
            "user": "{{ env_var('POSTGRES_USER', 'airflow') }}",
            "password": "{{ env_var('POSTGRES_PASSWORD', 'airflow') }}",
            "port": 5432,
            "dbname": "{{ env_var('POSTGRES_DB', 'airflow') }}",
        },
    ),
)


def get_dbt_configurations(**context):
    """Get dbt configurations from Airflow Variables"""
    try:
        dbt_configs = json.loads(Variable.get("dbt_configs"))
        db_configs = json.loads(Variable.get("db_configs"))

        logger.info(f"Retrieved dbt configurations: {dbt_configs}")
        return {"dbt_configs": dbt_configs, "db_configs": db_configs}
    except Exception as e:
        logger.error(f"Error retrieving dbt configurations: {e}")
        raise


def create_dbt_snapshot_dag(**context):
    """Create dbt snapshot DAG using Cosmos"""
    configs = context["task_instance"].xcom_pull(task_ids="get_dbt_configurations")
    dbt_configs = configs["dbt_configs"]

    logger.info("Creating dbt snapshot DAG configuration")
    return "dbt snapshot DAG configuration created successfully"


def create_dbt_run_dag(**context):
    """Create dbt run DAG using Cosmos"""
    configs = context["task_instance"].xcom_pull(task_ids="get_dbt_configurations")
    dbt_configs = configs["dbt_configs"]

    # Profile configuration for dbt
    profile_config = ProfileConfig(
        profile_name="postgres_data_copy",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="airflow_db",
            profile_args={
                "schema": "raw_data",
                "host": "{{ env_var('POSTGRES_HOST', 'postgres') }}",
                "user": "{{ env_var('POSTGRES_USER', 'airflow') }}",
                "password": "{{ env_var('POSTGRES_PASSWORD', 'airflow') }}",
                "port": 5432,
                "dbname": "{{ env_var('POSTGRES_DB', 'airflow') }}",
            },
        ),
    )

    # Create dbt run DAG configuration
    # Note: DbtDag는 전역 스코프에서 생성되어야 하므로 여기서는 설정만 반환
    run_config = {
        "project_config": "/opt/airflow/dbt",
        "profile_config": "postgres_data_copy",
        "select": " ".join(dbt_configs.get("models_to_run", ["staging", "marts"])),
        "full_refresh": dbt_configs.get("full_refresh", False),
        "dag_id": "dbt_run_dag",
    }

    logger.info("Created dbt run DAG configuration")
    return f"dbt run DAG configuration created: {run_config}"


def create_dbt_test_dag(**context):
    """Create dbt test DAG using Cosmos"""
    configs = context["task_instance"].xcom_pull(task_ids="get_dbt_configurations")
    dbt_configs = configs["dbt_configs"]

    # Profile configuration for dbt
    profile_config = ProfileConfig(
        profile_name="postgres_data_copy",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="airflow_db",
            profile_args={
                "schema": "raw_data",
                "host": "{{ env_var('POSTGRES_HOST', 'postgres') }}",
                "user": "{{ env_var('POSTGRES_USER', 'airflow') }}",
                "password": "{{ env_var('POSTGRES_PASSWORD', 'airflow') }}",
                "port": 5432,
                "dbname": "{{ env_var('POSTGRES_DB', 'airflow') }}",
            },
        ),
    )

    # Create dbt test DAG configuration
    # Note: DbtDag는 전역 스코프에서 생성되어야 하므로 여기서는 설정만 반환
    test_config = {
        "project_config": "/opt/airflow/dbt",
        "profile_config": "postgres_data_copy",
        "select": "test",
        "dag_id": "dbt_test_dag",
    }

    logger.info("Created dbt test DAG configuration")
    return f"dbt test DAG configuration created: {test_config}"


def create_dbt_docs_dag(**context):
    """Create dbt docs DAG using Cosmos"""
    configs = context["task_instance"].xcom_pull(task_ids="get_dbt_configurations")

    # Profile configuration for dbt
    profile_config = ProfileConfig(
        profile_name="postgres_data_copy",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="airflow_db",
            profile_args={
                "schema": "raw_data",
                "host": "{{ env_var('POSTGRES_HOST', 'postgres') }}",
                "user": "{{ env_var('POSTGRES_USER', 'airflow') }}",
                "password": "{{ env_var('POSTGRES_PASSWORD', 'airflow') }}",
                "port": 5432,
                "dbname": "{{ env_var('POSTGRES_DB', 'airflow') }}",
            },
        ),
    )

    # Create dbt docs DAG configuration
    # Note: DbtDag는 전역 스코프에서 생성되어야 하므로 여기서는 설정만 반환
    docs_config = {
        "project_config": "/opt/airflow/dbt",
        "profile_config": "postgres_data_copy",
        "select": "docs",
        "dag_id": "dbt_docs_dag",
    }

    logger.info("Created dbt docs DAG configuration")
    return f"dbt docs DAG configuration created: {docs_config}"


def orchestrate_dbt_workflow(**context):
    """Orchestrate the complete dbt workflow"""
    configs = context["task_instance"].xcom_pull(task_ids="get_dbt_configurations")
    dbt_configs = configs["dbt_configs"]

    logger.info("Starting dbt workflow orchestration")

    workflow_steps = []

    # Step 1: Run snapshots
    if dbt_configs.get("snapshot_tables"):
        workflow_steps.append("1. Snapshots created and ready to run")

    # Step 2: Run models
    if dbt_configs.get("models_to_run"):
        workflow_steps.append("2. Models configured and ready to run")

    # Step 3: Run tests
    if dbt_configs.get("test_after_run", True):
        workflow_steps.append("3. Tests configured and ready to run")

    # Step 4: Generate docs
    workflow_steps.append("4. Documentation generation configured")

    workflow_summary = "\n".join(workflow_steps)
    logger.info(f"dbt workflow orchestration completed:\n{workflow_summary}")

    return f"dbt workflow orchestration completed successfully:\n{workflow_summary}"


# Create actual dbt DAGs using Cosmos

# 1. dbt Snapshot DAG
dbt_snapshot_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": False,
        "select": "snapshot",  # Only run snapshots
    },
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_snapshot_dag",
    default_args={"retries": 2},
    tags=["cosmos", "dbt", "snapshot"],
)

# 2. dbt Run DAG
dbt_run_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": False,
        "select": "staging marts",  # Run staging and marts models
    },
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_run_dag",
    default_args={"retries": 2},
    tags=["cosmos", "dbt", "run"],
)

# 3. dbt Test DAG
dbt_test_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": False,
        "select": "test",  # Only run tests
    },
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_test_dag",
    default_args={"retries": 2},
    tags=["cosmos", "dbt", "test"],
)

# 4. dbt Docs DAG
dbt_docs_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": False,
        "select": "docs",  # Generate documentation
    },
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_docs_dag",
    default_args={"retries": 2},
    tags=["cosmos", "dbt", "docs"],
)

# Create main orchestration DAG
dag = DAG(**dag_config)

# Create tasks
get_configs_task = PythonOperator(
    task_id="get_dbt_configurations",
    python_callable=get_dbt_configurations,
    dag=dag,
)

create_snapshot_dag_task = PythonOperator(
    task_id="create_dbt_snapshot_dag",
    python_callable=create_dbt_snapshot_dag,
    dag=dag,
)

create_run_dag_task = PythonOperator(
    task_id="create_dbt_run_dag",
    python_callable=create_dbt_run_dag,
    dag=dag,
)

create_test_dag_task = PythonOperator(
    task_id="create_dbt_test_dag",
    python_callable=create_dbt_test_dag,
    dag=dag,
)

create_docs_dag_task = PythonOperator(
    task_id="create_dbt_docs_dag",
    python_callable=create_dbt_docs_dag,
    dag=dag,
)

orchestrate_task = PythonOperator(
    task_id="orchestrate_dbt_workflow",
    python_callable=orchestrate_dbt_workflow,
    dag=dag,
)

# Set task dependencies
(
    get_configs_task
    >> create_snapshot_dag_task
    >> create_run_dag_task
    >> create_test_dag_task
    >> create_docs_dag_task
    >> orchestrate_task
)
