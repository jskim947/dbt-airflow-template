"""
Example DAG demonstrating dbt command orchestration in Airflow
"""

from datetime import timedelta

from airflow import DAG  # type: ignore[attr-defined]
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# DAGConfigManager import 추가
from common.dag_config_manager import DAGConfigManager

# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 설정 가져오기
dag_config = DAGConfigManager.get_dag_config("dbt_processing_dag")

# Base command for dbt
DBT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"

with DAG(
    "example_dbt_dag",
    default_args=default_args,
    description="An example DAG for running dbt commands",
    schedule_interval=dag_config.get("schedule_interval", timedelta(days=1)),  # 설정에서 가져오기
    start_date=days_ago(1),
    catchup=False,
    tags=dag_config.get("tags", ["example", "dbt"]),
) as dag:
    # Debug information about environment
    debug_env = BashOperator(
        task_id="debug_env",
        bash_command=f"pwd && ls -la {DBT_DIR}",
    )

    # Run dbt debug to validate configuration
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_CMD} debug",
    )

    # Run dbt dependencies to install packages
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps",
    )

    # Run dbt tests on source data
    dbt_source_test = BashOperator(
        task_id="dbt_source_test",
        bash_command=f"{DBT_CMD} test --select source:*",
    )

    # Run dbt to build models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_CMD} run",
    )

    # Test the transformed data
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test",
    )

    # Generate and serve docs
    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=f"{DBT_CMD} docs generate",
    )

    # Define the order of operations
    (
        debug_env
        >> dbt_debug
        >> dbt_deps
        >> dbt_source_test
        >> dbt_run
        >> dbt_test
        >> dbt_docs
    )
