"""
Main orchestration DAG that coordinates the entire data pipeline
Coordinates variable setup, data copy, and dbt processing
"""

import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# DAGConfigManager import 추가
from common.dag_config_manager import DAGConfigManager

# DAG 설정 가져오기
dag_config_from_manager = DAGConfigManager.get_dag_config("main_orchestration_dag")

# DAG configuration
dag_config = {
    "dag_id": "main_orchestration_dag",
    "schedule_interval": dag_config_from_manager.get("schedule_interval", "@daily"),  # 설정에서 가져오기
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "default_args": {"retries": 2},
    "tags": dag_config_from_manager.get("tags", ["orchestration", "main", "data-pipeline"]),
}

logger = logging.getLogger(__name__)


def check_variables_exist(**context):
    """Check if required Airflow Variables exist"""
    try:
        required_vars = [
            "table_configs",
            "copy_method_configs",
            "db_configs",
            "dbt_configs",
        ]
        missing_vars = []

        for var_name in required_vars:
            try:
                Variable.get(var_name)
                logger.info(f"✅ Variable {var_name} exists")
            except:
                missing_vars.append(var_name)
                logger.warning(f"❌ Variable {var_name} missing")

        if missing_vars:
            raise ValueError(f"Missing required variables: {missing_vars}")

        logger.info("All required variables exist")
        return "All required variables exist"

    except Exception as e:
        logger.error(f"Error checking variables: {e}")
        raise


def validate_configurations(**context):
    """Validate configurations from Airflow Variables"""
    try:
        table_configs = json.loads(Variable.get("table_configs"))
        copy_method_configs = json.loads(Variable.get("copy_method_configs"))
        db_configs = json.loads(Variable.get("db_configs"))
        dbt_configs = json.loads(Variable.get("dbt_configs"))

        # Validate table configurations
        for table_name, config in table_configs.items():
            if not config.get("enabled", True):
                continue

            required_fields = [
                "source_schema",
                "target_schema",
                "copy_method",
                "primary_key",
            ]
            for field in required_fields:
                if field not in config:
                    raise ValueError(
                        f"Missing required field '{field}' in table config for {table_name}"
                    )

            # Validate copy method
            if config["copy_method"] not in copy_method_configs:
                raise ValueError(
                    f"Invalid copy method '{config['copy_method']}' for table {table_name}"
                )

            # Validate incremental config
            if (
                config["copy_method"] == "incremental"
                and "incremental_column" not in config
            ):
                raise ValueError(
                    f"Missing incremental_column for incremental table {table_name}"
                )

        # Validate database configurations
        required_db_configs = ["source_db", "target_db"]
        for db_name in required_db_configs:
            if db_name not in db_configs:
                raise ValueError(f"Missing database configuration: {db_name}")

        # Validate dbt configurations
        if not dbt_configs.get("snapshot_tables"):
            logger.warning("No snapshot tables configured")

        if not dbt_configs.get("models_to_run"):
            logger.warning("No models configured to run")

        logger.info("✅ All configurations validated successfully")
        return "All configurations validated successfully"

    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise


def prepare_pipeline_summary(**context):
    """Prepare summary of the data pipeline"""
    try:
        table_configs = json.loads(Variable.get("table_configs"))
        dbt_configs = json.loads(Variable.get("dbt_configs"))

        enabled_tables = [
            name
            for name, config in table_configs.items()
            if config.get("enabled", True)
        ]
        copy_methods = list(
            set(
                [
                    config["copy_method"]
                    for config in table_configs.values()
                    if config.get("enabled", True)
                ]
            )
        )

        summary = {
            "total_tables": len(table_configs),
            "enabled_tables": len(enabled_tables),
            "copy_methods": copy_methods,
            "snapshot_tables": dbt_configs.get("snapshot_tables", []),
            "models_to_run": dbt_configs.get("models_to_run", []),
            "pipeline_ready": True,
        }

        logger.info(f"Pipeline summary: {summary}")
        return summary

    except Exception as e:
        logger.error(f"Error preparing pipeline summary: {e}")
        raise


def pipeline_completion_check(**context):
    """Check if all pipeline components completed successfully"""
    try:
        # This function would typically check the status of downstream DAGs
        # For now, we'll just log completion
        logger.info("Pipeline orchestration completed successfully")
        return "Pipeline orchestration completed successfully"

    except Exception as e:
        logger.error(f"Pipeline completion check failed: {e}")
        raise


# Create DAG
dag = DAG(**dag_config)

# Create tasks
start_task = DummyOperator(
    task_id="start_pipeline",
    dag=dag,
)

check_vars_task = PythonOperator(
    task_id="check_variables_exist",
    python_callable=check_variables_exist,
    dag=dag,
)

validate_configs_task = PythonOperator(
    task_id="validate_configurations",
    python_callable=validate_configurations,
    dag=dag,
)

prepare_summary_task = PythonOperator(
    task_id="prepare_pipeline_summary",
    python_callable=prepare_pipeline_summary,
    dag=dag,
)

# Trigger variable setup DAG if needed
trigger_variable_setup_task = TriggerDagRunOperator(
    task_id="trigger_variable_setup_dag",
    trigger_dag_id="variable_setup_dag",
    dag=dag,
    wait_for_completion=True,
)

# Trigger data copy DAG
trigger_data_copy_task = TriggerDagRunOperator(
    task_id="trigger_data_copy_dag",
    trigger_dag_id="data_copy_dag",
    dag=dag,
    wait_for_completion=True,
)

# Trigger dbt processing DAG
trigger_dbt_processing_task = TriggerDagRunOperator(
    task_id="trigger_dbt_processing_dag",
    trigger_dag_id="dbt_processing_dag",
    dag=dag,
    wait_for_completion=True,
)

completion_check_task = PythonOperator(
    task_id="pipeline_completion_check",
    python_callable=pipeline_completion_check,
    dag=dag,
)

end_task = DummyOperator(
    task_id="end_pipeline",
    dag=dag,
)

# Set task dependencies
start_task >> check_vars_task >> validate_configs_task >> prepare_summary_task

# Conditional path - if variables don't exist, run variable setup first
check_vars_task >> trigger_variable_setup_task >> validate_configs_task

# Main pipeline flow
(
    prepare_summary_task
    >> trigger_data_copy_task
    >> trigger_dbt_processing_task
    >> completion_check_task
    >> end_task
)
