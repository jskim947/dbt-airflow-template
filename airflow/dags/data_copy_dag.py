"""
DAG for copying data from source to target database based on Airflow Variables
"""

import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

# PostgresOperator는 현재 사용하지 않으므로 제거
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# DAGConfigManager import 추가
from common.dag_config_manager import DAGConfigManager

# DAG 설정 가져오기
dag_config_from_manager = DAGConfigManager.get_dag_config("data_copy_dag")

# DAG configuration
dag_config = {
    "dag_id": "data_copy_dag",
    "schedule_interval": dag_config_from_manager.get("schedule_interval", "@daily"),  # 설정에서 가져오기
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "default_args": {"retries": 2},
    "tags": dag_config_from_manager.get("tags", ["data-copy", "postgres", "etl"]),
}

logger = logging.getLogger(__name__)


def get_configurations(**context):
    """Get configurations from Airflow Variables"""
    try:
        table_configs = json.loads(Variable.get("table_configs"))
        copy_method_configs = json.loads(Variable.get("copy_method_configs"))
        db_configs = json.loads(Variable.get("db_configs"))

        logger.info(f"Retrieved configurations for {len(table_configs)} tables")
        return {
            "table_configs": table_configs,
            "copy_method_configs": copy_method_configs,
            "db_configs": db_configs,
        }
    except Exception as e:
        logger.error(f"Error retrieving configurations: {e}")
        raise


def copy_table_full(
    table_name: str, source_config: dict, target_config: dict, **context
):
    """Full copy of table - truncate and insert all data"""
    try:
        source_hook = PostgresHook(postgres_conn_id=source_config["connection_id"])
        target_hook = PostgresHook(postgres_conn_id=target_config["connection_id"])

        # Get source data
        source_query = f"""
        SELECT * FROM {source_config['schema']}.{table_name}
        """
        source_data = source_hook.get_records(source_query)

        if not source_data:
            logger.info(f"No data found in source table {table_name}")
            return f"No data to copy for {table_name}"

        # Get column names from first row
        columns = [desc[0] for desc in source_hook.get_cursor().description]

        # Truncate target table
        target_hook.run(f"TRUNCATE TABLE {target_config['schema']}.{table_name}")
        logger.info(f"Truncated target table {target_config['schema']}.{table_name}")

        # Insert data in batches
        batch_size = 1000
        total_rows = len(source_data)

        for i in range(0, total_rows, batch_size):
            batch = source_data[i : i + batch_size]

            # Create INSERT statement
            placeholders = ",".join(["%s"] * len(columns))
            insert_query = f"""
            INSERT INTO {target_config['schema']}.{table_name}
            ({', '.join(columns)})
            VALUES ({placeholders})
            """

            target_hook.run(insert_query, parameters=batch)
            logger.info(
                f"Inserted batch {i//batch_size + 1} for {table_name} ({len(batch)} rows)"
            )

        logger.info(f"Successfully copied {total_rows} rows from {table_name}")
        return f"Successfully copied {total_rows} rows from {table_name}"

    except Exception as e:
        logger.error(f"Error copying table {table_name}: {e}")
        raise


def copy_table_incremental(
    table_name: str,
    table_config: dict,
    source_config: dict,
    target_config: dict,
    **context,
):
    """Incremental copy of table - only new/modified records"""
    try:
        source_hook = PostgresHook(postgres_conn_id=source_config["connection_id"])
        target_hook = PostgresHook(postgres_conn_id=target_config["connection_id"])

        # Get last processed timestamp from target
        last_processed_query = f"""
        SELECT MAX({table_config['incremental_column']})
        FROM {target_config['schema']}.{table_name}
        """
        last_processed = target_hook.get_first(last_processed_query)
        last_timestamp = (
            last_processed[0] if last_processed and last_processed[0] else None
        )

        # Build incremental query
        if last_timestamp:
            incremental_query = f"""
            SELECT * FROM {source_config['schema']}.{table_name}
            WHERE {table_config['incremental_column']} > %s
            ORDER BY {table_config['incremental_column']}
            """
            source_data = source_hook.get_records(
                incremental_query, parameters=[last_timestamp]
            )
        else:
            # First run - get all data
            incremental_query = f"""
            SELECT * FROM {source_config['schema']}.{table_name}
            ORDER BY {table_config['incremental_column']}
            """
            source_data = source_hook.get_records(incremental_query)

        if not source_data:
            logger.info(f"No new data found in source table {table_name}")
            return f"No new data to copy for {table_name}"

        # Get column names
        columns = [desc[0] for desc in source_hook.get_cursor().description]

        # Insert data in batches
        batch_size = table_config.get("batch_size", 1000)
        total_rows = len(source_data)

        for i in range(0, total_rows, batch_size):
            batch = source_data[i : i + batch_size]

            # Create INSERT statement with conflict resolution
            placeholders = ",".join(["%s"] * len(columns))
            insert_query = f"""
            INSERT INTO {target_config['schema']}.{table_name}
            ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT ({table_config['primary_key']})
            DO UPDATE SET
            """

            # Build UPDATE clause for all columns except primary key
            update_columns = [
                col for col in columns if col != table_config["primary_key"]
            ]
            update_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )
            insert_query += update_clause

            target_hook.run(insert_query, parameters=batch)
            logger.info(
                f"Inserted/updated batch {i//batch_size + 1} for {table_name} ({len(batch)} rows)"
            )

        logger.info(f"Successfully copied {total_rows} rows from {table_name}")
        return f"Successfully copied {total_rows} rows from {table_name}"

    except Exception as e:
        logger.error(f"Error copying table {table_name}: {e}")
        raise


def copy_table_cdc(
    table_name: str,
    table_config: dict,
    source_config: dict,
    target_config: dict,
    **context,
):
    """Change Data Capture copy - track changes over time"""
    try:
        source_hook = PostgresHook(postgres_conn_id=source_config["connection_id"])
        target_hook = PostgresHook(postgres_conn_id=target_config["connection_id"])

        # Create CDC table if it doesn't exist
        cdc_table = f"{table_name}_cdc"
        create_cdc_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_config['schema']}.{cdc_table} (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100),
            operation VARCHAR(10),
            record_id VARCHAR(100),
            old_data JSONB,
            new_data JSONB,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        target_hook.run(create_cdc_table_query)

        # Get source data
        source_query = f"""
        SELECT * FROM {source_config['schema']}.{table_name}
        ORDER BY {table_config.get('incremental_column', 'id')}
        """
        source_data = source_hook.get_records(source_query)

        if not source_data:
            logger.info(f"No data found in source table {table_name}")
            return f"No data to copy for {table_name}"

        # Get column names
        columns = [desc[0] for desc in source_hook.get_cursor().description]

        # Insert data and track changes
        batch_size = table_config.get("batch_size", 1000)
        total_rows = len(source_data)

        for i in range(0, total_rows, batch_size):
            batch = source_data[i : i + batch_size]

            # Create INSERT statement
            placeholders = ",".join(["%s"] * len(columns))
            insert_query = f"""
            INSERT INTO {target_config['schema']}.{table_name}
            ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT ({table_config['primary_key']})
            DO UPDATE SET
            """

            # Build UPDATE clause
            update_columns = [
                col for col in columns if col != table_config["primary_key"]
            ]
            update_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )
            insert_query += update_clause

            target_hook.run(insert_query, parameters=batch)

            # Track changes in CDC table
            for row in batch:
                record_id = row[columns.index(table_config["primary_key"])]
                cdc_insert_query = f"""
                INSERT INTO {target_config['schema']}.{cdc_table}
                (table_name, operation, record_id, new_data)
                VALUES (%s, %s, %s, %s)
                """
                target_hook.run(
                    cdc_insert_query,
                    parameters=[
                        table_name,
                        "INSERT",
                        str(record_id),
                        json.dumps(dict(zip(columns, row, strict=False))),
                    ],
                )

            logger.info(
                f"Processed batch {i//batch_size + 1} for {table_name} ({len(batch)} rows)"
            )

        logger.info(
            f"Successfully copied {total_rows} rows from {table_name} with CDC tracking"
        )
        return (
            f"Successfully copied {total_rows} rows from {table_name} with CDC tracking"
        )

    except Exception as e:
        logger.error(f"Error copying table {table_name}: {e}")
        raise


def copy_table(table_name: str, **context):
    """Main function to copy a table based on its configuration"""
    configs = context["task_instance"].xcom_pull(task_ids="get_configurations")

    table_config = configs["table_configs"][table_name]
    copy_method = table_config["copy_method"]
    source_config = configs["db_configs"]["source_db"]
    target_config = configs["db_configs"]["target_db"]

    logger.info(f"Starting {copy_method} copy for table {table_name}")

    if copy_method == "full_copy":
        return copy_table_full(table_name, source_config, target_config, **context)
    elif copy_method == "incremental":
        return copy_table_incremental(
            table_name, table_config, source_config, target_config, **context
        )
    elif copy_method == "cdc":
        return copy_table_cdc(
            table_name, table_config, source_config, target_config, **context
        )
    else:
        raise ValueError(f"Unknown copy method: {copy_method}")


def copy_all_tables(**context):
    """Copy all enabled tables"""
    configs = context["task_instance"].xcom_pull(task_ids="get_configurations")
    table_configs = configs["table_configs"]

    enabled_tables = [
        name for name, config in table_configs.items() if config.get("enabled", True)
    ]

    logger.info(
        f"Starting copy for {len(enabled_tables)} enabled tables: {enabled_tables}"
    )

    results = []
    for table_name in enabled_tables:
        try:
            result = copy_table(table_name, **context)
            results.append(f"✅ {table_name}: {result}")
        except Exception as e:
            error_msg = f"❌ {table_name}: Error - {e!s}"
            results.append(error_msg)
            logger.error(error_msg)

    return "\n".join(results)


# Create DAG
dag = DAG(**dag_config)

# Create tasks
get_configs_task = PythonOperator(
    task_id="get_configurations",
    python_callable=get_configurations,
    dag=dag,
)

copy_all_task = PythonOperator(
    task_id="copy_all_tables",
    python_callable=copy_all_tables,
    dag=dag,
)

# Set task dependencies
get_configs_task >> copy_all_task
