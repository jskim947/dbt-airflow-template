"""
Simple PostgreSQL Data Copy DAG
간단한 PostgreSQL 데이터 복사 DAG

이 DAG는 기본적인 데이터 복사 작업을 수행합니다:
1. 소스 테이블에서 데이터 추출
2. 타겟 테이블에 데이터 삽입
3. 간단한 검증
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# DAG 기본 설정
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    "simple_postgres_copy",
    default_args=default_args,
    description="Simple data copy between PostgreSQL databases",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "simple-copy"],
)

# 연결 ID 및 테이블 설정
SOURCE_CONN_ID = "source_postgres_conn"
TARGET_CONN_ID = "target_postgres_conn"
SOURCE_TABLE = "users"
TARGET_TABLE = "users_copy"


def copy_table_data(**context):
    """
    전체 테이블 데이터를 복사하는 함수
    """
    try:
        # 소스에서 데이터 추출
        source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        source_data = source_hook.get_records(f"SELECT * FROM {SOURCE_TABLE}")

        if not source_data:
            print(f"No data found in source table {SOURCE_TABLE}")
            return "No data to copy"

        # 타겟에 데이터 삽입
        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # 기존 데이터 삭제
        target_hook.run(f"DELETE FROM {TARGET_TABLE}")

        # 새 데이터 삽입
        if source_data:
            # 컬럼명 가져오기
            columns = source_hook.get_records(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{SOURCE_TABLE}' ORDER BY ordinal_position"
            )
            column_names = [col[0] for col in columns]

            # 배치 삽입
            batch_size = 1000
            for i in range(0, len(source_data), batch_size):
                batch = source_data[i : i + batch_size]
                placeholders = ",".join(["%s"] * len(column_names))

                insert_sql = f"""
                    INSERT INTO {TARGET_TABLE} ({', '.join(column_names)})
                    VALUES ({placeholders})
                """

                target_hook.run(insert_sql, parameters=batch)
                print(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")

        copied_count = len(source_data)
        context["task_instance"].xcom_push(key="copied_count", value=copied_count)

        return f"Successfully copied {copied_count} records from {SOURCE_TABLE} to {TARGET_TABLE}"

    except Exception as e:
        print(f"Error copying data: {e!s}")
        raise e


def verify_copy(**context):
    """
    복사된 데이터 검증
    """
    try:
        source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # 레코드 수 비교
        source_count = source_hook.get_first(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")[0]
        target_count = target_hook.get_first(f"SELECT COUNT(*) FROM {TARGET_TABLE}")[0]

        print(f"Source table count: {source_count}")
        print(f"Target table count: {target_count}")

        if source_count == target_count:
            print("✅ Record count verification passed")
            return f"Verification successful: {target_count} records copied"
        else:
            raise Exception(
                f"Record count mismatch: source={source_count}, target={target_count}"
            )

    except Exception as e:
        print(f"Verification failed: {e!s}")
        raise e


# Task 정의
copy_task = PythonOperator(
    task_id="copy_table_data",
    python_callable=copy_table_data,
    dag=dag,
)

verify_task = PythonOperator(
    task_id="verify_copy",
    python_callable=verify_copy,
    dag=dag,
)

# Task 의존성
copy_task >> verify_task
