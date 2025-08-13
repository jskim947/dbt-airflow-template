"""
DAG for setting up Airflow Variables for table configurations and copy methods
"""

import json
from datetime import datetime

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

# Table configurations for data copy
TABLE_CONFIGS = {
    "users": {
        "source_schema": "public",
        "target_schema": "raw_data",
        "copy_method": "full_copy",  # full_copy, incremental, cdc
        "primary_key": "id",
        "batch_size": 1000,
        "enabled": True,
    },
    "orders": {
        "source_schema": "public",
        "target_schema": "raw_data",
        "copy_method": "incremental",
        "primary_key": "order_id",
        "incremental_column": "created_at",
        "batch_size": 500,
        "enabled": True,
    },
    "products": {
        "source_schema": "public",
        "target_schema": "raw_data",
        "copy_method": "full_copy",
        "primary_key": "product_id",
        "batch_size": 100,
        "enabled": True,
    },
    "order_items": {
        "source_schema": "public",
        "target_schema": "raw_data",
        "copy_method": "incremental",
        "primary_key": "item_id",
        "incremental_column": "created_at",
        "batch_size": 2000,
        "enabled": True,
    },
}

# Copy method configurations
COPY_METHOD_CONFIGS = {
    "full_copy": {
        "description": "Full table copy - truncate and insert all data",
        "truncate_before_copy": True,
        "parallel_workers": 4,
        "timeout_minutes": 30,
    },
    "incremental": {
        "description": "Incremental copy - only new/modified records",
        "truncate_before_copy": False,
        "parallel_workers": 2,
        "timeout_minutes": 15,
        "upsert_strategy": "insert_on_conflict",
    },
    "cdc": {
        "description": "Change Data Capture - track changes over time",
        "truncate_before_copy": False,
        "parallel_workers": 1,
        "timeout_minutes": 10,
        "track_deletes": True,
        "track_updates": True,
    },
}

# Database connection configurations
DB_CONFIGS = {
    "source_db": {
        "host": "{{ env_var('SOURCE_POSTGRES_HOST', 'source_postgres') }}",
        "port": "{{ env_var('SOURCE_POSTGRES_PORT', 5432) }}",
        "database": "{{ env_var('SOURCE_POSTGRES_DB', 'source_db') }}",
        "schema": "public",
        "connection_id": "source_postgres",
    },
    "target_db": {
        "host": "{{ env_var('POSTGRES_HOST', 'postgres') }}",
        "port": "{{ env_var('POSTGRES_PORT', 5432) }}",
        "database": "{{ env_var('POSTGRES_DB', 'airflow') }}",
        "schema": "raw_data",
        "connection_id": "airflow_db",
    },
}

# dbt configurations
DBT_CONFIGS = {
    "snapshot_tables": ["users", "orders", "products", "order_items"],
    "snapshot_strategy": "timestamp",
    "snapshot_updated_at": "updated_at",
    "models_to_run": ["staging", "marts"],
    "test_after_run": True,
    "full_refresh": False,
}


def setup_table_variables(**context):
    """Set up table configuration variables"""
    Variable.set("table_configs", json.dumps(TABLE_CONFIGS, indent=2))
    print(f"Set table_configs variable with {len(TABLE_CONFIGS)} tables")
    return f"Successfully set {len(TABLE_CONFIGS)} table configurations"


def setup_copy_method_variables(**context):
    """Set up copy method configuration variables"""
    Variable.set("copy_method_configs", json.dumps(COPY_METHOD_CONFIGS, indent=2))
    print(f"Set copy_method_configs variable with {len(COPY_METHOD_CONFIGS)} methods")
    return f"Successfully set {len(COPY_METHOD_CONFIGS)} copy method configurations"


def setup_database_variables(**context):
    """Set up database connection variables"""
    Variable.set("db_configs", json.dumps(DB_CONFIGS, indent=2))
    print(f"Set db_configs variable with {len(DB_CONFIGS)} database configurations")
    return f"Successfully set {len(DB_CONFIGS)} database configurations"


def setup_dbt_variables(**context):
    """Set up dbt configuration variables"""
    Variable.set("dbt_configs", json.dumps(DBT_CONFIGS, indent=2))
    print(f"Set dbt_configs variable with {len(DBT_CONFIGS)} dbt configurations")
    return f"Successfully set {len(DBT_CONFIGS)} dbt configurations"


def verify_variables(**context):
    """Verify all variables are set correctly"""
    try:
        table_configs = json.loads(Variable.get("table_configs"))
        copy_method_configs = json.loads(Variable.get("copy_method_configs"))
        db_configs = json.loads(Variable.get("db_configs"))
        dbt_configs = json.loads(Variable.get("dbt_configs"))

        print("✅ All variables verified successfully:")
        print(f"  - Table configs: {len(table_configs)} tables")
        print(f"  - Copy methods: {len(copy_method_configs)} methods")
        print(f"  - Database configs: {len(db_configs)} databases")
        print(f"  - dbt configs: {len(dbt_configs)} configurations")

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

setup_copy_methods_task = PythonOperator(
    task_id="setup_copy_method_variables",
    python_callable=setup_copy_method_variables,
    dag=dag,
)

setup_db_task = PythonOperator(
    task_id="setup_database_variables",
    python_callable=setup_database_variables,
    dag=dag,
)

setup_dbt_task = PythonOperator(
    task_id="setup_dbt_variables",
    python_callable=setup_dbt_variables,
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
    >> setup_copy_methods_task
    >> setup_db_task
    >> setup_dbt_task
    >> verify_task
)
