"""
PostgreSQL Multi-Table Data Copy DAG
여러 PostgreSQL 테이블을 순차적으로 복사하는 DAG

이 DAG는 다음 작업을 수행합니다:
1. 소스 PostgreSQL에서 데이터 추출 (동기화 모드별)
2. 데이터 변환 및 검증
3. 타겟 PostgreSQL에 데이터 로드
4. 데이터 무결성 검사

동기화 모드:
- incremental_sync: 증분 동기화 (기존 데이터 유지)
- full_sync: 전체 동기화 (소스에 없는 데이터 삭제)

참고: https://github.com/apache/airflow/tree/providers-postgres/6.2.3/providers/postgres/tests/system/postgres
"""

import logging
import os
import subprocess
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 로거 설정
logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["admin@example.com"],
}

# DAG 정의
dag = DAG(
    "postgres_multi_table_copy",
    default_args=default_args,
    description="Copy data from multiple PostgreSQL tables sequentially and create dbt snapshots",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "data-copy", "etl", "multi-table", "dbt-snapshot"],
    max_active_runs=1,
)

# 연결 ID 설정
SOURCE_CONN_ID = "fs2_postgres"
TARGET_CONN_ID = "postgres_default"

# dbt 프로젝트 경로 설정
DBT_PROJECT_PATH = "/opt/airflow/dbt"

# 여러 테이블 설정 (순차 처리)
TABLES_CONFIG = [
    {
        "source": "fds_팩셋.인포맥스종목마스터",
        "target": "raw_data.인포맥스종목마스터",
        "primary_key": ["인포맥스코드", "팩셋거래소", "gts_exnm", "티커"],
        "sync_mode": "full_sync",  # 'incremental_sync' 또는 'full_sync'
        "batch_size": 10000,
    }
    # {
    #     'source': "fds_팩셋.인포맥스종목마스터",
    #     'target': "raw_data.인포맥스종목마스터",
    #     'primary_key': ['종목코드'],
    #     'incremental_field': '업데이트_시간',
    #     'incremental_field_type': 'timestamp',  # 'timestamp', 'yyyymmdd', 'date', 'datetime'
    #     'sync_mode': 'incremental_sync',  # 'incremental_sync' 또는 'full_sync'
    #     'batch_size': 10000,
    #     'custom_where': '시가총액 > 1000000000'  # 비즈니스 조건만
    # },
    # {
    #     'source': "fds_팩셋.거래내역",
    #     'target': "raw_data.거래내역",
    #     'primary_key': ['거래번호'],
    #     'incremental_field': '거래일시',
    #     'incremental_field_type': 'datetime',
    #     'sync_mode': 'incremental_sync',
    #     'batch_size': 5000,
    #     'custom_where': '거래상태 = \'완료\''
    # },
    # {
    #     'source': "fds_팩셋.기준정보",
    #     'target': "raw_data.기준정보",
    #     'primary_key': ['기준일자', '종목코드'],
    #     'incremental_field': '기준일자',
    #     'incremental_field_type': 'date',
    #     'sync_mode': 'full_sync',
    #     'batch_size': 1000,
    #     'custom_where': '유효여부 = \'Y\''
    # }
]


def get_common_filters() -> list[str]:
    """공통으로 적용할 기본 필터 반환"""
    return [
        "deleted_at IS NULL",  # 삭제되지 않은 데이터
        "status != 'deleted'",  # 삭제 상태가 아닌 데이터
        "is_active = true",  # 활성 데이터
    ]


def get_table_specific_filters(table_config: dict, hook: PostgresHook) -> list[str]:
    """
    테이블별로 실제 존재하는 컬럼에 대해서만 필터 적용

    Args:
        table_config: 테이블 설정
        hook: PostgreSQL 연결 훅

    Returns:
        실제 존재하는 컬럼에 대한 필터 목록
    """
    try:
        # 소스 테이블의 컬럼 정보 가져오기
        source_schema, source_table = table_config["source"].split(".")
        columns_sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """

        columns = hook.get_records(
            columns_sql, parameters=(source_schema, source_table)
        )
        existing_columns = {col[0].lower() for col in columns}

        # 공통 필터 중 실제 존재하는 컬럼에 대해서만 적용
        common_filters = get_common_filters()
        applicable_filters = []

        for filter_condition in common_filters:
            # 컬럼명 추출 (간단한 파싱)
            if (
                ("deleted_at" in filter_condition and "deleted_at" in existing_columns)
                or ("status" in filter_condition and "status" in existing_columns)
                or ("is_active" in filter_condition and "is_active" in existing_columns)
            ):
                applicable_filters.append(filter_condition)

        return applicable_filters

    except Exception as e:
        logger.warning(
            f"Could not get table-specific filters for {table_config['source']}: {e!s}"
        )
        # 에러 시 빈 리스트 반환 (필터 없이 진행)
        return []


def generate_temp_table_name(source_table: str, target_table: str) -> str:
    """
    소스 테이블과 타겟 테이블을 기반으로 temp 테이블 이름 자동 생성

    Args:
        source_table: "schema.table" 형식의 소스 테이블
        target_table: "schema.table" 형식의 타겟 테이블

    Returns:
        temp 테이블 이름 (예: "temp_인포맥스종목마스터")
    """
    # 타겟 테이블에서 테이블명만 추출
    target_schema, target_table_name = target_table.split(".")

    # temp_ 접두사 추가
    temp_table_name = f"temp_{target_table_name}"

    return temp_table_name


def get_table_config_with_temp(table_config: dict) -> dict:
    """
    테이블 설정에 temp 테이블 정보 추가

    Args:
        table_config: 기본 테이블 설정

    Returns:
        temp 테이블 정보가 추가된 설정
    """
    temp_table_name = generate_temp_table_name(
        table_config["source"], table_config["target"]
    )

    # 스키마와 테이블명 분리
    target_schema, target_table_name = table_config["target"].split(".")

    enhanced_config = table_config.copy()
    enhanced_config.update(
        {
            "temp_table": temp_table_name,
            "temp_table_full": f"{target_schema}.{temp_table_name}",
            "target_schema": target_schema,
            "target_table_name": target_table_name,
        }
    )

    # temp_table_full을 원본 table_config에도 추가 (다른 함수에서 사용)
    table_config["temp_table_full"] = enhanced_config["temp_table_full"]

    return enhanced_config


def build_incremental_filter(
    field_name: str, field_type: str, last_update_time: Any
) -> str:
    """
    필드 타입에 따라 증분 필터 자동 생성

    Args:
        field_name: 증분 필드명
        field_type: 필드 타입 ('timestamp', 'yyyymmdd', 'date', 'datetime')
        last_update_time: 마지막 업데이트 시간

    Returns:
        증분 필터 SQL 조건
    """
    if field_type == "timestamp":
        return f"{field_name} >= %(last_update_time)s"

    elif field_type == "yyyymmdd":
        # YYYYMMDD 형식 (예: 20240101)
        if isinstance(last_update_time, str):
            # 문자열을 YYYYMMDD 형식으로 변환
            formatted_time = (
                last_update_time.replace("-", "").replace(":", "").replace(" ", "")[:8]
            )
        else:
            formatted_time = last_update_time.strftime("%Y%m%d")
        return f"{field_name} >= {formatted_time}"

    elif field_type == "date":
        # DATE 형식 (예: 2024-01-01)
        if isinstance(last_update_time, str):
            formatted_time = last_update_time[:10]  # YYYY-MM-DD 부분만
        else:
            formatted_time = last_update_time.strftime("%Y-%m-%d")
        return f"{field_name} >= '{formatted_time}'"

    elif field_type == "datetime":
        # DATETIME 형식 (예: 2024-01-01 00:00:00)
        if isinstance(last_update_time, str):
            formatted_time = last_update_time[:19]  # YYYY-MM-DD HH:MM:SS 부분만
        else:
            formatted_time = last_update_time.strftime("%Y-%m-%d %H:%M:%S")
        return f"{field_name} >= '{formatted_time}'"

    else:
        raise ValueError(f"Unsupported field type: {field_type}")


def build_dynamic_sql(table_config: dict, sql_type: str, **kwargs) -> str:
    """
    테이블 설정에 따라 동적으로 SQL 생성
    """
    # 🚨 PRIORITY 컬럼 특별 처리 - 소수점 값을 정수로 변환
    if table_config['source'] == 'm23.edi_690':
        logger.info("🚨 m23.edi_690 테이블 감지! PRIORITY 컬럼 소수점 값 변환 SQL 생성")
        base_sql = f"""
            SELECT
                eventcd, eventid, optionid, serialid, scexhid, sedolid, actflag, changed, created,
                secid, issid, isin, uscode, issuername, cntryofincorp, sectycd, securitydesc,
                parvalue, pvcurrency, statusflag, primaryexchgcd, sedol, sedolcurrency, defunct,
                sedolregcntry, exchgcntry, exchgcd, mic, micseg, localcode, liststatus, issuedate,
                date1type, date1, date2type, date2, date3type, date3, date4type, date4, date5type,
                date5, date6type, date6, date7type, date7, date8type, date8, date9type, date9,
                date10type, date10, date11type, date11, date12type, date12, paytype,
                CASE
                    WHEN priority IS NULL THEN NULL
                    WHEN priority = '' THEN NULL
                    WHEN priority ~ '^[0-9]+\.[0-9]+$' THEN CAST(CAST(priority AS NUMERIC) AS BIGINT)::TEXT
                    ELSE priority
                END AS priority,
                defaultopt, outurnsecid, outurnisin, ratioold, rationew, fractions, currency,
                rate1type, rate1, rate2type, rate2, field1name, field1, field2name, field2,
                field3name, field3, field4name, field4, field5name, field5, field6name, field6,
                field7name, field7, field8name, field8, field9name, field9, field10name, field10,
                field11name, field11, field12name, field12, field13name, field13, field14name,
                field14, field15name, field15, field16name, field16, field17name, field17,
                field18name, field18, field19name, field19, field20name, field20, field21name,
                field21, field22name, field22, uptodate
            FROM {table_config['source']}
        """
    else:
        base_sql = f"SELECT * FROM {table_config['source']}"

    where_conditions = []

    if sql_type == "select_incremental":
        # 증분 동기화 시에만 증분 필터 추가
        if table_config["sync_mode"] == "incremental_sync":
            incremental_filter = build_incremental_filter(
                table_config["incremental_field"],
                table_config["incremental_field_type"],
                kwargs.get("last_update_time"),
            )
            where_conditions.append(incremental_filter)

    # 테이블별 공통 필터 적용 (실제 존재하는 컬럼에 대해서만)
    try:
        hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        table_filters = get_table_specific_filters(table_config, hook)
        where_conditions.extend(table_filters)
    except Exception as e:
        logger.warning(f"Could not apply table-specific filters: {e!s}")
        # 에러 시 기본 공통 필터 적용
        common_filters = get_common_filters()
        where_conditions.extend(common_filters)

    # 커스텀 WHERE 조건 추가
    if table_config.get("custom_where"):
        where_conditions.append(table_config["custom_where"])

    # WHERE 절 추가
    if where_conditions:
        base_sql += " WHERE " + " AND ".join(where_conditions)

    # ORDER BY 추가 (증분 필드 기준)
    if table_config.get("incremental_field"):
        base_sql += f" ORDER BY {table_config['incremental_field']} DESC"

    return base_sql


def get_default_time_by_type(field_type: str) -> Any:
    """필드 타입별 기본 시간 반환"""
    if field_type == "yyyymmdd":
        return "19000101"
    elif field_type == "date":
        return "1900-01-01"
    elif field_type == "datetime":
        return "1900-01-01 00:00:00"
    else:  # timestamp
        return datetime(1900, 1, 1)


def get_last_update_time(table_config: dict, **context) -> Any:
    """
    테이블별로 마지막 업데이트 시간 자동 감지
    full_sync 모드일 때는 None 반환
    """
    # full_sync 모드일 때는 마지막 업데이트 시간이 필요하지 않음
    if table_config.get("sync_mode") == "full_sync":
        logger.info(
            f"Full sync mode for {table_config['source']}, skipping last update time check"
        )
        return None

    # incremental_sync 모드일 때만 incremental_field와 incremental_field_type이 필요
    if not table_config.get("incremental_field") or not table_config.get(
        "incremental_field_type"
    ):
        logger.warning(
            f"Missing incremental_field or incremental_field_type for {table_config['source']} in incremental_sync mode"
        )
        return None

    try:
        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        incremental_field = table_config["incremental_field"]

        # 타겟 테이블에서 마지막 업데이트 시간 조회
        last_update_sql = f"""
            SELECT MAX({incremental_field})
            FROM {table_config['target']}
            WHERE {incremental_field} IS NOT NULL
        """

        result = target_hook.get_first(last_update_sql)
        last_update_time = result[0] if result and result[0] else None

        if last_update_time:
            logger.info(
                f"Last update time for {table_config['source']}: {last_update_time}"
            )
            return last_update_time
        else:
            # 데이터가 없으면 기본값 반환
            field_type = table_config["incremental_field_type"]
            return get_default_time_by_type(field_type)

    except Exception as e:
        logger.warning(
            f"Could not get last update time for {table_config['source']}: {e!s}"
        )
        # 에러 시 기본값 반환
        return get_default_time_by_type(table_config["incremental_field_type"])


def parse_table_name(full_table_name: str) -> tuple[str, str]:
    """
    전체 테이블명에서 스키마와 테이블명을 분리

    Args:
        full_table_name: "schema.table" 형식의 테이블명

    Returns:
        (schema_name, table_name) 튜플

    Raises:
        ValueError: 테이블명 형식이 올바르지 않은 경우
    """
    if "." not in full_table_name:
        raise ValueError(
            f"Invalid table name format: {full_table_name}. Expected 'schema.table'"
        )

    parts = full_table_name.split(".")
    if len(parts) != 2:
        raise ValueError(
            f"Invalid table name format: {full_table_name}. Expected 'schema.table'"
        )

    return parts[0], parts[1]


def check_source_connection(table_config: dict, **context) -> dict[str, Any]:
    """소스 PostgreSQL 연결 상태 확인 (테이블별)"""
    try:
        logger.info(f"Checking source connection for table: {table_config['source']}")

        hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # 연결 테스트
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        logger.info(
            f"Source database connection successful for {table_config['source']}"
        )

        return {"status": "success", "table": table_config["source"]}

    except Exception as e:
        error_msg = f"Source connection failed for {table_config['source']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def check_target_connection(table_config: dict, **context) -> dict[str, Any]:
    """타겟 PostgreSQL 연결 상태 확인 (테이블별)"""
    try:
        logger.info(f"Checking target connection for table: {table_config['target']}")

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # 연결 테스트
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        logger.info(
            f"Target database connection successful for {table_config['target']}"
        )

        return {"status": "success", "table": table_config["target"]}

    except Exception as e:
        error_msg = f"Target connection failed for {table_config['target']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def get_source_data_count(table_config: dict, **context) -> dict[str, Any]:
    """소스 테이블의 데이터 수 조회 (테이블별)"""
    try:
        logger.info(f"Getting data count for table: {table_config['source']}")

        hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)

        # 마지막 업데이트 시간 가져오기
        last_update_time = get_last_update_time(table_config, **context)

        # 동기화 모드에 따라 SQL 생성
        if table_config["sync_mode"] == "incremental_sync":
            count_sql = build_dynamic_sql(
                table_config, "select_incremental", last_update_time=last_update_time
            )
        else:
            count_sql = build_dynamic_sql(table_config, "select_full")

        # COUNT(*) 쿼리로 변환
        count_sql = count_sql.replace("SELECT *", "SELECT COUNT(*)")

        logger.info(f"Count SQL: {count_sql}")

        result = hook.get_first(count_sql)
        total_count = result[0] if result else 0

        logger.info(
            f"Total records to process for {table_config['source']}: {total_count}"
        )

        # target_table_name을 안전하게 생성
        target_schema, target_table_name = table_config["target"].split(".")

        # XCom에 저장
        context["task_instance"].xcom_push(
            key=f"source_data_count_{target_table_name}", value=total_count
        )

        return {"status": "success", "total_count": total_count, "sql_used": count_sql}

    except Exception as e:
        error_msg = f"Failed to get data count for {table_config['source']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def create_temp_table(table_config: dict, **context) -> str:
    """임시 테이블 생성 (테이블별)"""
    try:
        enhanced_config = get_table_config_with_temp(table_config)
        temp_table_full = enhanced_config["temp_table_full"]

        logger.info(f"Creating temporary table: {temp_table_full}")

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # 소스 테이블 스키마 가져오기 (소스 데이터베이스에서)
        source_schema, source_table = table_config["source"].split(".")
        source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)

        columns_sql = """
            SELECT column_name, data_type AS type, is_nullable AS nullable, column_default AS default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """

        columns = source_hook.get_records(
            columns_sql, parameters=(source_schema, source_table)
        )

        logger.info(
            f"Found {len(columns)} columns in source table: {[col[0] for col in columns]}"
        )

        if not columns:
            raise Exception(f"No columns found for table {table_config['source']}")

        # 기존 테이블이 있는지 확인
        table_exists_sql = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{enhanced_config['target_schema']}'
                AND table_name = '{enhanced_config['temp_table']}'
            )
        """
        table_exists = hook.get_first(table_exists_sql)[0]

        if table_exists:
            logger.info(
                f"Table {temp_table_full} already exists, checking column structure..."
            )

            # 기존 테이블의 컬럼 정보 가져오기
            existing_columns_sql = f"""
                SELECT column_name, data_type AS type, is_nullable AS nullable, column_default AS default
                FROM information_schema.columns
                WHERE table_schema = '{enhanced_config['target_schema']}'
                AND table_name = '{enhanced_config['temp_table']}'
                ORDER BY ordinal_position
            """
            existing_columns = hook.get_records(existing_columns_sql)
            existing_column_names = [col[0] for col in existing_columns]
            expected_column_names = [col[0] for col in columns]

            logger.info(f"Existing columns: {existing_column_names}")
            logger.info(f"Expected columns: {expected_column_names}")

            # 컬럼 구조가 일치하는지 확인
            if existing_column_names == expected_column_names:
                logger.info(
                    f"Column structure matches, using existing table {temp_table_full}"
                )
                return f"Using existing table {temp_table_full} with {len(existing_columns)} columns"
            else:
                logger.info(
                    f"Column structure mismatch, dropping and recreating table {temp_table_full}"
                )
                hook.run(f"DROP TABLE IF EXISTS {temp_table_full}")

        # CREATE TABLE 구문 생성
        create_sql = f"CREATE TABLE {temp_table_full} ("
        column_definitions = []

        for col in columns:
            col_name, col_type, nullable, col_default = col
            nullable_clause = "NULL" if nullable == "YES" else "NOT NULL"
            default = f" DEFAULT {col_default}" if col_default else ""
            column_definitions.append(
                f"{col_name} {col_type} {nullable_clause}{default}"
            )

        create_sql += ", ".join(column_definitions) + ")"

        logger.info(f"CREATE TABLE SQL: {create_sql}")

        hook.run(create_sql)
        logger.info(
            f"Temporary table {temp_table_full} created successfully with {len(columns)} columns"
        )

        # 생성된 테이블의 실제 컬럼 수 확인
        verify_sql = f"""
            SELECT COUNT(*) as column_count
            FROM information_schema.columns
            WHERE table_schema = '{enhanced_config['target_schema']}'
            AND table_name = '{enhanced_config['temp_table']}'
        """
        actual_columns = hook.get_first(verify_sql)[0]
        logger.info(f"Verified: Temporary table actually has {actual_columns} columns")

        if actual_columns != len(columns):
            raise Exception(
                f"Column count mismatch: expected {len(columns)}, but table has {actual_columns}"
            )

        return (
            f"Temporary table {temp_table_full} created with {actual_columns} columns"
        )

    except Exception as e:
        error_msg = (
            f"Failed to create temporary table for {table_config['source']}: {e!s}"
        )
        logger.error(error_msg)
        raise Exception(error_msg)


def ensure_target_table_exists(table_config: dict, **context) -> str:
    """
    타겟 테이블이 존재하지 않을 경우 소스 테이블 구조를 기반으로 생성
    """
    try:
        logger.info(f"Ensuring target table exists: {table_config['target']}")

        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)

        # 타겟 스키마와 테이블명 분리
        target_schema, target_table = table_config["target"].split(".")

        # 1단계: 스키마가 존재하는지 확인하고 없으면 생성
        schema_exists_sql = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.schemata
                WHERE schema_name = '{target_schema}'
            )
        """
        schema_exists = target_hook.get_first(schema_exists_sql)[0]

        if not schema_exists:
            logger.info(f"Schema {target_schema} does not exist, creating it...")
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {target_schema}"
            target_hook.run(create_schema_sql)
            logger.info(f"Schema {target_schema} created successfully")
        else:
            logger.info(f"Schema {target_schema} already exists")

        # 2단계: 타겟 테이블이 이미 존재하는지 확인
        check_sql = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{target_schema}'
                AND table_name = '{target_table}'
            )
        """

        logger.info(f"Checking table existence with SQL: {check_sql}")
        table_exists = target_hook.get_first(check_sql)[0]
        logger.info(f"Table existence check result: {table_exists}")

        if table_exists:
            logger.info(f"Target table {table_config['target']} already exists")
            # 실제로 테이블이 정말 존재하는지 한 번 더 확인
            verify_sql = f"SELECT COUNT(*) FROM {table_config['target']} LIMIT 1"
            try:
                row_count = target_hook.get_first(verify_sql)[0]
                logger.info(f"Table verification successful: {row_count} rows found")
                return f"Target table {table_config['target']} already exists and verified"
            except Exception as verify_error:
                logger.warning(f"Table verification failed: {verify_error}, will recreate table")
                # 테이블이 실제로는 존재하지 않으므로 생성 진행
                pass

        # 소스 테이블의 컬럼 정보 가져오기 (max_length 포함)
        source_schema, source_table = table_config["source"].split(".")
        columns_sql = """
            SELECT column_name, data_type AS type, is_nullable AS nullable, column_default AS default, character_maximum_length AS max_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """

        columns = source_hook.get_records(
            columns_sql, parameters=(source_schema, source_table)
        )

        if not columns:
            raise Exception(
                f"No columns found in source table {table_config['source']}"
            )

        # CREATE TABLE 구문 생성 - DataCopyEngine과 동일한 타입 매핑 사용
        create_sql = f"CREATE TABLE {table_config['target']} ("
        column_definitions = []

        for col in columns:
            col_name, col_type, nullable, col_default, max_length = col  # max_length 추가
            is_nullable = nullable == "YES"

            # DataCopyEngine과 동일한 타입 매핑 로직 사용
            if "char" in col_type.lower() or "text" in col_type.lower():
                if max_length and max_length > 0:
                    pg_type = f"VARCHAR({max_length})"
                else:
                    pg_type = "TEXT"
            elif "int" in col_type.lower():
                if "bigint" in col_type.lower():
                    pg_type = "BIGINT"
                elif "smallint" in col_type.lower():
                    pg_type = "SMALLINT"
                else:
                    pg_type = "INTEGER"
            elif "decimal" in col_type.lower() or "numeric" in col_type.lower():
                pg_type = "NUMERIC"
            elif "float" in col_type.lower() or "double" in col_type.lower():
                pg_type = "DOUBLE PRECISION"
            elif "real" in col_type.lower():
                pg_type = "REAL"
            elif "date" in col_type.lower():
                pg_type = "DATE"
            elif "time" in col_type.lower():
                if "timestamp" in col_type.lower():
                    pg_type = "TIMESTAMP"
                else:
                    pg_type = "TIME"
            elif "bool" in col_type.lower():
                pg_type = "BOOLEAN"
            elif "json" in col_type.lower():
                pg_type = "JSONB"
            elif "uuid" in col_type.lower():
                pg_type = "UUID"
            else:
                pg_type = "TEXT"  # 기본값

            nullable_clause = "NOT NULL" if not is_nullable else ""
            default = f" DEFAULT {col_default}" if col_default else ""
            column_definitions.append(
                f"{col_name} {pg_type} {nullable_clause}{default}"
            )

        create_sql += ", ".join(column_definitions) + ")"

        logger.info(f"CREATE TABLE SQL for target: {create_sql}")

        target_hook.run(create_sql)
        logger.info(
            f"Target table {table_config['target']} created successfully with {len(columns)} columns"
        )

        return (
            f"Target table {table_config['target']} created with {len(columns)} columns"
        )

    except Exception as e:
        error_msg = (
            f"Failed to ensure target table exists for {table_config['source']}: {e!s}"
        )
        logger.error(error_msg)
        raise Exception(error_msg)


def log_table_structure(table_config: dict, **context) -> str:
    """
    소스와 타겟 테이블의 컬럼 구조를 로그로 출력하여 디버깅 지원
    """
    try:
        logger.info(f"Logging table structure for debugging: {table_config['source']}")

        source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # 소스 테이블 컬럼 정보
        source_schema, source_table = table_config["source"].split(".")
        source_columns_sql = """
            SELECT column_name, data_type AS type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """

        source_columns = source_hook.get_records(
            source_columns_sql, parameters=(source_schema, source_table)
        )
        source_column_names = [col[0] for col in source_columns]

        logger.info(
            f"Source table {table_config['source']} columns: {source_column_names}"
        )

        # 타겟 테이블 컬럼 정보
        target_schema, target_table = table_config["target"].split(".")
        target_columns_sql = """
            SELECT column_name, data_type AS type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """

        target_columns = target_hook.get_records(
            target_columns_sql, parameters=(target_schema, target_table)
        )
        target_column_names = [col[0] for col in target_columns]

        logger.info(
            f"Target table {table_config['target']} columns: {target_column_names}"
        )

        # 컬럼 차이점 분석
        missing_in_target = set(source_column_names) - set(target_column_names)
        extra_in_target = set(target_column_names) - set(source_column_names)

        if missing_in_target:
            logger.warning(f"Columns missing in target: {list(missing_in_target)}")
        if extra_in_target:
            logger.warning(f"Extra columns in target: {list(extra_in_target)}")

        return f"Table structure logged. Source: {len(source_columns)} columns, Target: {len(target_columns)} columns"

    except Exception as e:
        logger.warning(
            f"Could not log table structure for {table_config['source']}: {e!s}"
        )
        return f"Failed to log table structure: {e!s}"


def copy_data_with_dynamic_sql(table_config: dict, **context) -> dict[str, Any]:
    """
    동적 SQL을 사용하여 데이터 복사
    """
    try:
        enhanced_config = get_table_config_with_temp(table_config)
        temp_table_full = enhanced_config["temp_table_full"]

        logger.info(
            f"Starting data copy with dynamic SQL from {table_config['source']} to {temp_table_full}"
        )

        source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # 마지막 업데이트 시간 가져오기
        last_update_time = get_last_update_time(table_config, **context)

        # 동기화 모드에 따라 SQL 생성
        if table_config["sync_mode"] == "incremental_sync":
            select_sql = build_dynamic_sql(
                table_config, "select_incremental", last_update_time=last_update_time
            )
        else:
            select_sql = build_dynamic_sql(table_config, "select_full")

        logger.info(f"Generated SQL: {select_sql}")

        # 1단계: 소스에서 데이터 추출하여 CSV로 내보내기 (psql \copy 사용)
        logger.info("Step 1: Exporting data from source to CSV using psql \\copy")
        csv_filename = f"/tmp/source_data_{enhanced_config['target_table_name']}.csv"

        # 소스 연결 정보 가져오기
        source_conn = source_hook.get_conn()
        source_info = source_conn.get_dsn_parameters()

        # 간단한 psql \copy 명령어로 데이터 내보내기
        export_cmd = f"psql -h {source_info.get('host', 'localhost')} -p {source_info.get('port', '15432')} -U {source_info.get('user')} -d {source_info.get('dbname')} -c \"\\copy ({select_sql}) TO '{csv_filename}' WITH CSV HEADER\""

        logger.info(f"Executing export command: {export_cmd}")
        result = subprocess.run(export_cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Export failed: {result.stderr}")

        logger.info("Data exported to CSV successfully")

        # 2단계: CSV에서 타겟으로 가져오기 (psql \copy 사용)
        logger.info("Step 2: Importing data from CSV to target using psql \\copy")

        # 타겟 연결 정보 가져오기 (비밀번호 포함)
        target_conn = target_hook.get_conn()
        target_info = target_conn.get_dsn_parameters()

        # PostgresHook에서 직접 비밀번호 가져오기
        target_password = target_hook.get_connection(
            target_hook.postgres_conn_id
        ).password

        logger.info(
            f"Target connection info - Host: {target_info.get('host')}, Port: {target_info.get('port')}, User: {target_info.get('user')}, DB: {target_info.get('dbname')}, Password: {'***' if target_password else 'None'}"
        )

        # 간단한 psql \copy 명령어로 데이터 가져오기 (비밀번호 포함)
        if target_password:
            # 비밀번호가 있으면 PGPASSWORD 환경변수 사용
            import_cmd = f"psql -h {target_info.get('host', 'localhost')} -p {target_info.get('port', '15432')} -U {target_info.get('user')} -d {target_info.get('dbname')} -c \"\\copy {temp_table_full} FROM '{csv_filename}' WITH CSV HEADER\""
        else:
            # 비밀번호가 없으면 기본 명령어
            import_cmd = f"psql -h {target_info.get('host', 'localhost')} -p {target_info.get('port', '15432')} -U {target_info.get('user')} -d {target_info.get('dbname')} -c \"\\copy {temp_table_full} FROM '{csv_filename}' WITH CSV HEADER\""

        logger.info(f"Executing import command: {import_cmd}")

        # 환경변수 설정
        env = os.environ.copy()
        if target_password:
            env["PGPASSWORD"] = target_password
            logger.info("PGPASSWORD environment variable set")
        else:
            logger.warning("No password found for target connection")

        result = subprocess.run(
            import_cmd, shell=True, capture_output=True, text=True, env=env
        )

        if result.returncode != 0:
            raise Exception(f"Import failed: {result.stderr}")

        logger.info("Data imported from CSV successfully")

        # 3단계: 중복 제거 (Primary Key 기준)
        logger.info("Step 3: Removing duplicates based on primary key")

        # Primary Key 컬럼들을 쉼표로 구분하여 문자열 생성
        if isinstance(table_config["primary_key"], list):
            pk_columns = ", ".join(table_config["primary_key"])
        else:
            pk_columns = table_config["primary_key"]

        # 더 효율적인 중복 제거 방법 (ROW_NUMBER 사용)
        dedup_sql = f"""
            DELETE FROM {temp_table_full}
            WHERE ctid IN (
                SELECT ctid
                FROM (
                    SELECT ctid,
                           ROW_NUMBER() OVER (
                               PARTITION BY {pk_columns}
                               ORDER BY ctid
                           ) as rn
                    FROM {temp_table_full}
                ) t
                WHERE t.rn > 1
            )
        """

        target_hook.run(dedup_sql)
        logger.info(f"Removed duplicates from {temp_table_full}")

        # 4단계: 복사된 레코드 수 확인 (중복 제거 후)
        copied_count = target_hook.get_first(f"SELECT COUNT(*) FROM {temp_table_full}")[
            0
        ]

        context["task_instance"].xcom_push(
            key=f'copied_records_count_{enhanced_config["target_table_name"]}',
            value=copied_count,
        )

        logger.info(f"Data copy completed. Total records copied: {copied_count}")

        return {
            "status": "success",
            "copied_count": copied_count,
            "sql_used": select_sql,
        }

    except Exception as e:
        error_msg = f"Data copy failed for {table_config['source']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def prepare_merge_parameters(table_config: dict, **context) -> dict[str, Any]:
    """
    MERGE 작업을 위한 동적 파라미터 준비 (테이블별)
    """
    try:
        logger.info(f"Preparing MERGE parameters for table: {table_config['source']}")

        source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # 1단계: 소스 테이블의 모든 컬럼 정보 가져오기
        schema_name, table_name = parse_table_name(table_config["source"])

        columns_sql = """
            SELECT column_name, data_type AS type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """

        columns = source_hook.get_records(
            columns_sql, parameters=(schema_name, table_name)
        )
        all_columns = [col[0] for col in columns]

        # 2단계: 업데이트할 컬럼 목록 생성 (제외 컬럼 제외)
        exclude_columns = ["created_at"]

        # incremental_sync 모드일 때만 incremental_field 제외
        if table_config.get("sync_mode") == "incremental_sync" and table_config.get(
            "incremental_field"
        ):
            exclude_columns.append(table_config["incremental_field"])

        # primary_key가 리스트인 경우 처리
        if isinstance(table_config["primary_key"], list):
            exclude_columns.extend(table_config["primary_key"])
        else:
            exclude_columns.append(table_config["primary_key"])

        update_columns = [col for col in all_columns if col not in exclude_columns]

        # 3단계: 마지막 업데이트 시간 가져오기
        last_update_time = get_last_update_time(table_config, **context)

        # enhanced_config에서 temp_table_full 가져오기
        enhanced_config = get_table_config_with_temp(table_config)
        temp_table_full = enhanced_config["temp_table_full"]

        # 4단계: MERGE 파라미터 구성
        merge_params = {
            "target_table": table_config["target"],
            "temp_table": temp_table_full,
            "primary_key": table_config["primary_key"],
            "incremental_field": table_config.get(
                "incremental_field"
            ),  # None일 수 있음
            "update_columns": update_columns,
            "all_columns": all_columns,
            "last_update_time": last_update_time,
        }

        logger.info(f"MERGE parameters prepared: {merge_params}")

        # target_table_name을 안전하게 생성
        target_schema, target_table_name = table_config["target"].split(".")

        # 5단계: XCom에 파라미터 저장
        context["task_instance"].xcom_push(
            key=f"merge_parameters_{target_table_name}", value=merge_params
        )

        return merge_params

    except Exception as e:
        error_msg = (
            f"Failed to prepare MERGE parameters for {table_config['source']}: {e!s}"
        )
        logger.error(error_msg)
        raise Exception(error_msg)


def execute_full_sync_merge(table_config: dict, merge_params: dict, **context) -> str:
    """전체 동기화 MERGE 실행 (SQL 파일 사용)"""
    try:
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # enhanced_config에서 temp_table_full 가져오기
        enhanced_config = get_table_config_with_temp(table_config)
        temp_table_full = enhanced_config["temp_table_full"]

        # SQL 파일에서 MERGE SQL 읽기
        import os

        sql_file_path = os.path.join(
            os.path.dirname(__file__), "sql", "full_sync_merge.sql"
        )

        with open(sql_file_path) as f:
            merge_sql = f.read()

        # SQL 파라미터 치환
        merge_sql = merge_sql.replace("TARGET_TABLE", table_config["target"])
        merge_sql = merge_sql.replace("TEMP_TABLE", temp_table_full)

        # primary_key가 리스트인 경우 처리
        if isinstance(table_config["primary_key"], list):
            primary_key_condition = " AND ".join(
                [f"target.{pk} = source.{pk}" for pk in table_config["primary_key"]]
            )
        else:
            primary_key_condition = f"target.{table_config['primary_key']} = source.{table_config['primary_key']}"
        merge_sql = merge_sql.replace("PRIMARY_KEY_CONDITION", primary_key_condition)

        # full_sync 모드에서는 incremental_field 관련 치환 건너뛰기
        # 컬럼 정보 치환
        update_columns = merge_params.get("update_columns", [])
        all_columns = merge_params.get("all_columns", [])

        # 업데이트 컬럼 치환
        update_set_clause = ", ".join(
            [f"{col} = source.{col}" for col in update_columns]
        )

        # updated_at 컬럼이 존재하는지 확인하고 조건부로 추가
        target_schema, target_table = table_config["target"].split(".")
        updated_at_exists_sql = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_schema = '{target_schema}'
                AND table_name = '{target_table}'
                AND column_name = 'updated_at'
            )
        """
        updated_at_exists = hook.get_first(updated_at_exists_sql)[0]

        if updated_at_exists:
            update_set_clause += ", updated_at = CURRENT_TIMESTAMP"
            logger.info(
                f"updated_at column exists in {table_config['target']}, adding to UPDATE SET"
            )
        else:
            logger.info(
                f"updated_at column does not exist in {table_config['target']}, skipping"
            )

        merge_sql = merge_sql.replace("UPDATE_COLUMNS", update_set_clause)

        # INSERT 컬럼 치환
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join([f"source.{col}" for col in all_columns])
        merge_sql = merge_sql.replace("ALL_COLUMNS", insert_columns)
        merge_sql = merge_sql.replace("INSERT_VALUES", insert_values)

        # PRIMARY_KEY 치환 (결과 확인용)
        if isinstance(table_config["primary_key"], list):
            primary_key_str = table_config["primary_key"][0]  # 첫 번째 PK만 사용
        else:
            primary_key_str = table_config["primary_key"]
        merge_sql = merge_sql.replace("PRIMARY_KEY", primary_key_str)

        logger.info(f"Executing full sync MERGE SQL for {table_config['source']}")

        # MERGE 실행
        hook.run(merge_sql)

        logger.info(f"Full sync MERGE completed for {table_config['source']}")
        return "Full sync MERGE completed"

    except Exception as e:
        error_msg = f"Full sync MERGE failed for {table_config['source']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def execute_incremental_merge(table_config: dict, merge_params: dict, **context) -> str:
    """증분 동기화 MERGE 실행 (SQL 파일 사용)"""
    try:
        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # enhanced_config에서 temp_table_full 가져오기
        enhanced_config = get_table_config_with_temp(table_config)
        temp_table_full = enhanced_config["temp_table_full"]

        # SQL 파일에서 MERGE SQL 읽기
        import os

        sql_file_path = os.path.join(
            os.path.dirname(__file__), "sql", "incremental_merge.sql"
        )

        with open(sql_file_path) as file:
            merge_sql = file.read()

        # SQL 파라미터 치환
        merge_sql = merge_sql.replace("TARGET_TABLE", table_config["target"])
        merge_sql = merge_sql.replace("TEMP_TABLE", temp_table_full)

        # primary_key가 리스트인 경우 처리
        if isinstance(table_config["primary_key"], list):
            primary_key_condition = " AND ".join(
                [f"target.{pk} = source.{pk}" for pk in table_config["primary_key"]]
            )
        else:
            primary_key_condition = f"target.{table_config['primary_key']} = source.{table_config['primary_key']}"
        merge_sql = merge_sql.replace("PRIMARY_KEY_CONDITION", primary_key_condition)

        # incremental_field가 있을 때만 치환, 없으면 빈 문자열로 치환
        incremental_field = table_config.get("incremental_field", "")
        merge_sql = merge_sql.replace("INCREMENTAL_FIELD", incremental_field)

        # 컬럼 정보 치환
        update_columns = merge_params.get("update_columns", [])
        all_columns = merge_params.get("all_columns", [])

        # 업데이트 컬럼 치환
        update_set_clause = ", ".join(
            [f"{col} = source.{col}" for col in update_columns]
        )
        merge_sql = merge_sql.replace("UPDATE_COLUMNS", update_set_clause)

        # INSERT 컬럼 치환
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join([f"source.{col}" for col in all_columns])
        merge_sql = merge_sql.replace("ALL_COLUMNS", insert_columns)
        merge_sql = merge_sql.replace("INSERT_VALUES", insert_values)

        # 마지막 업데이트 시간 치환
        last_update_time = merge_params.get("last_update_time", "1900-01-01")
        if isinstance(last_update_time, datetime):
            last_update_time = last_update_time.strftime("%Y-%m-%d %H:%M:%S")
        merge_sql = merge_sql.replace("LAST_UPDATE_TIME", str(last_update_time))

        # PRIMARY_KEY 치환 (결과 확인용)
        if isinstance(table_config["primary_key"], list):
            primary_key_str = table_config["primary_key"][0]  # 첫 번째 PK만 사용
        else:
            primary_key_str = table_config["primary_key"]
        merge_sql = merge_sql.replace("PRIMARY_KEY", primary_key_str)

        logger.info(f"Executing incremental MERGE SQL for {table_config['source']}")

        # MERGE 실행
        hook.run(merge_sql)

        logger.info(f"Incremental MERGE completed for {table_config['source']}")
        return "Incremental MERGE completed"

    except Exception as e:
        error_msg = f"Incremental MERGE failed for {table_config['source']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def validate_data_integrity(table_config: dict, **context) -> dict[str, Any]:
    """
    MERGE 후 데이터 무결성 검증 (테이블별)
    """
    try:
        logger.info(
            f"Starting MERGE data integrity validation for {table_config['source']}"
        )

        source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # target_table_name을 안전하게 생성
        target_schema, target_table_name = table_config["target"].split(".")

        # MERGE 파라미터 가져오기
        merge_params = context["task_instance"].xcom_pull(
            key=f"merge_parameters_{target_table_name}", task_ids="process_all_tables"
        )

        if not merge_params:
            raise Exception("MERGE parameters not found")

        # 1단계: 소스와 타겟의 레코드 수 비교
        source_count = source_hook.get_first(
            f"SELECT COUNT(*) FROM {table_config['source']}"
        )[0]
        target_count = target_hook.get_first(
            f"SELECT COUNT(*) FROM {table_config['target']}"
        )[0]

        logger.info(f"Source count: {source_count}, Target count: {target_count}")

        # 2단계: MERGE 결과 검증 (PK 기준)
        primary_key = merge_params["primary_key"]
        incremental_field = merge_params["incremental_field"]

        # incremental_field가 없으면 전체 데이터 검증
        if not incremental_field:
            logger.info(
                f"No incremental field for {table_config['source']}, performing full data validation"
            )
            latest_source_count = source_count
            latest_target_count = target_count
        else:
            # 소스에서 최신 데이터만 추출하여 검증
            latest_source_sql = f"""
                SELECT COUNT(*) FROM {table_config['source']}
                WHERE {incremental_field} >= %s
            """
            latest_source_count = source_hook.get_first(
                latest_source_sql, parameters=(merge_params["last_update_time"],)
            )[0]

            # 타겟에서 최신 데이터 수 확인
            latest_target_sql = f"""
                SELECT COUNT(*) FROM {table_config['target']}
                WHERE {incremental_field} >= %s
            """
            latest_target_count = target_hook.get_first(
                latest_target_sql, parameters=(merge_params["last_update_time"],)
            )[0]

        logger.info(
            f"Latest source count: {latest_source_count}, Latest target count: {latest_target_count}"
        )

        # 3단계: 샘플 데이터 검증 (PK 기준)
        logger.info("Performing sample data validation by primary key")

        # primary_key가 리스트인 경우 처리
        if isinstance(primary_key, list):
            pk_columns = ", ".join(primary_key)
            pk_where_conditions = " AND ".join([f"{pk} = %s" for pk in primary_key])
        else:
            pk_columns = primary_key
            pk_where_conditions = f"{primary_key} = %s"

        # incremental_field가 없으면 PK만으로 검증
        if not incremental_field:
            sample_sql = f"""
                SELECT {pk_columns}
                FROM {table_config['source']}
                LIMIT 10
            """
            source_sample = source_hook.get_records(sample_sql)

            # 타겟에서 동일한 PK로 검증 (데이터 존재 여부만 확인)
            for source_row in source_sample:
                pk_values = (
                    source_row[: len(primary_key)]
                    if isinstance(primary_key, list)
                    else [source_row[0]]
                )

                target_sql = f"""
                    SELECT {pk_columns}
                    FROM {table_config['target']}
                    WHERE {pk_where_conditions}
                """
                target_row = target_hook.get_first(target_sql, parameters=pk_values)

                if not target_row:
                    pk_display = (
                        ", ".join(map(str, pk_values))
                        if isinstance(primary_key, list)
                        else str(pk_values[0])
                    )
                    error_msg = f"Data not found in target for PK {pk_display}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
        else:
            # incremental_field가 있으면 시간 기준으로 검증
            sample_sql = f"""
                SELECT {pk_columns}, {incremental_field}
                FROM {table_config['source']}
                ORDER BY {incremental_field} DESC
                LIMIT 10
            """
            source_sample = source_hook.get_records(sample_sql)

            # 타겟에서 동일한 PK로 검증
            for source_row in source_sample:
                if isinstance(primary_key, list):
                    pk_values = source_row[: len(primary_key)]
                    source_time = source_row[len(primary_key)]
                else:
                    pk_values = [source_row[0]]
                    source_time = source_row[1]

                target_sql = f"""
                    SELECT {pk_columns}, {incremental_field}
                    FROM {table_config['target']}
                    WHERE {pk_where_conditions}
                """
                target_row = target_hook.get_first(target_sql, parameters=pk_values)

                if not target_row or target_row[len(primary_key)] != source_time:
                    pk_display = (
                        ", ".join(map(str, pk_values))
                        if isinstance(primary_key, list)
                        else str(pk_values[0])
                    )
                    error_msg = f"Data mismatch for PK {pk_display}: source={source_time}, target={target_row[len(primary_key)] if target_row else 'None'}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

        logger.info("Sample data validation passed")

        context["task_instance"].xcom_push(
            key=f"validation_status_{target_table_name}", value="passed"
        )

        return {
            "status": "success",
            "source_count": source_count,
            "target_count": target_count,
            "latest_source_count": latest_source_count,
            "latest_target_count": latest_target_count,
            "validation": "passed",
            "sync_mode": table_config["sync_mode"],
        }

    except Exception as e:
        error_msg = f"MERGE data validation failed for {table_config['source']}: {e!s}"
        logger.error(error_msg)

        context["task_instance"].xcom_push(
            key=f"validation_status_{target_table_name}", value="failed"
        )
        raise Exception(error_msg)


def cleanup_temp_table(table_config: dict, **context) -> str:
    """
    임시 테이블 정리 및 MERGE 결과 요약 (테이블별)
    """
    try:
        enhanced_config = get_table_config_with_temp(table_config)
        temp_table_full = enhanced_config["temp_table_full"]

        logger.info(f"Cleaning up temporary table: {temp_table_full}")

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # target_table_name을 안전하게 생성
        target_schema, target_table_name = table_config["target"].split(".")

        # MERGE 결과 가져오기
        merge_params = context["task_instance"].xcom_pull(
            key=f"merge_parameters_{target_table_name}", task_ids="process_all_tables"
        )

        # MERGE 결과 요약
        if merge_params:
            logger.info(f"MERGE completed with parameters: {merge_params}")

            # 최종 결과 확인
            final_count_sql = f"SELECT COUNT(*) FROM {table_config['target']}"
            final_count = hook.get_first(final_count_sql)[0]

            context["task_instance"].xcom_push(
                key=f"final_record_count_{target_table_name}", value=final_count
            )

            logger.info(f"Final record count in target table: {final_count}")

        # 임시 테이블이 여전히 존재하는지 확인 (스키마 포함)
        temp_exists_sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """

        temp_exists = hook.get_first(
            temp_exists_sql,
            parameters=(
                enhanced_config["target_schema"],
                enhanced_config["temp_table"],
            ),
        )[0]

        if temp_exists:
            hook.run(f"DROP TABLE IF EXISTS {temp_table_full}")
            logger.info(f"Temporary table {temp_table_full} dropped")
            return f"Temporary table {temp_table_full} cleaned up. MERGE completed successfully."
        else:
            logger.info("No temporary table to clean up")
            return "No temporary table to clean up. MERGE completed successfully."

    except Exception as e:
        error_msg = f"Failed to cleanup temp table for {table_config['source']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def run_dbt_snapshot(table_config: dict, **context) -> dict[str, Any]:
    """
    dbt 스냅샷 실행

    Args:
        table_config: 테이블 설정
        **context: Airflow 컨텍스트

    Returns:
        스냅샷 실행 결과
    """
    try:
        logger.info(f"Running dbt snapshot for table: {table_config['source']}")

        # 현재 작업 디렉토리 저장
        original_cwd = os.getcwd()

        try:
            # dbt 프로젝트 디렉토리로 이동
            if not os.path.exists(DBT_PROJECT_PATH):
                raise Exception(
                    f"dbt project directory does not exist: {DBT_PROJECT_PATH}"
                )

            os.chdir(DBT_PROJECT_PATH)
            logger.info(f"Changed to dbt project directory: {DBT_PROJECT_PATH}")

            # dbt가 설치되어 있는지 확인
            dbt_version_cmd = ["/home/airflow/.local/bin/dbt", "--version"]
            version_result = subprocess.run(
                dbt_version_cmd, capture_output=True, text=True, env=os.environ.copy()
            )

            if version_result.returncode != 0:
                logger.warning(
                    "dbt not found in default location, trying alternative locations"
                )
                # dbt가 기본 경로에 없는 경우 대안 경로 시도
                possible_dbt_paths = [
                    "/usr/local/bin/dbt",
                    "/opt/airflow/.local/bin/dbt",
                    "/home/airflow/.local/bin/dbt",
                    "/home/jskim947/.local/bin/dbt",
                ]

                dbt_found = False
                for dbt_path in possible_dbt_paths:
                    if os.path.exists(dbt_path) and os.access(dbt_path, os.X_OK):
                        logger.info(f"Found executable dbt at: {dbt_path}")
                        dbt_cmd = dbt_path
                        dbt_found = True
                        break

                if not dbt_found:
                    # 마지막 시도: which 명령어로 dbt 찾기
                    try:
                        which_result = subprocess.run(
                            ["which", "dbt"], capture_output=True, text=True
                        )
                        if which_result.returncode == 0:
                            dbt_cmd = which_result.stdout.strip()
                            logger.info(f"Found dbt using 'which' command: {dbt_cmd}")
                            dbt_found = True
                    except Exception as e:
                        logger.warning(f"Failed to use 'which' command: {e}")

                if not dbt_found:
                    raise Exception("dbt executable not found in any expected location")
            else:
                dbt_cmd = "/home/airflow/.local/bin/dbt"
                logger.info(f"dbt version: {version_result.stdout.strip()}")

            # dbt 실행 파일이 실제로 존재하고 실행 가능한지 최종 확인
            if not os.path.exists(dbt_cmd):
                raise Exception(
                    f"dbt executable not found at specified path: {dbt_cmd}"
                )

            if not os.access(dbt_cmd, os.X_OK):
                raise Exception(f"dbt executable is not executable at path: {dbt_cmd}")

            logger.info(f"Using dbt executable: {dbt_cmd}")

            # 환경 변수 설정
            env = os.environ.copy()
            env.update(
                {
                    "DBT_PROFILES_DIR": DBT_PROJECT_PATH,
                    "DBT_PROJECT_DIR": DBT_PROJECT_PATH,
                    "PYTHONPATH": f"{DBT_PROJECT_PATH}:{env.get('PYTHONPATH', '')}",
                }
            )

            logger.info(f"DBT_PROFILES_DIR: {env.get('DBT_PROFILES_DIR')}")
            logger.info(f"DBT_PROJECT_DIR: {env.get('DBT_PROJECT_DIR')}")
            logger.info(f"Current working directory: {os.getcwd()}")
            logger.info(f"dbt project path: {DBT_PROJECT_PATH}")
            logger.info(f"dbt executable: {dbt_cmd}")

            # dbt deps 실행 (의존성 설치)
            logger.info("Running dbt deps...")
            deps_cmd = [dbt_cmd, "deps"]
            logger.info(f"Executing dbt deps command: {' '.join(deps_cmd)}")
            deps_result = subprocess.run(
                deps_cmd, capture_output=True, text=True, env=env, cwd=DBT_PROJECT_PATH
            )

            if deps_result.returncode != 0:
                logger.warning(f"dbt deps failed: {deps_result.stderr}")
                logger.warning(f"dbt deps stdout: {deps_result.stdout}")
            else:
                logger.info("dbt deps completed successfully")

            # dbt debug 실행 (프로젝트 설정 확인)
            logger.info("Running dbt debug...")
            debug_cmd = [dbt_cmd, "debug"]
            logger.info(f"Executing dbt debug command: {' '.join(debug_cmd)}")
            debug_result = subprocess.run(
                debug_cmd, capture_output=True, text=True, env=env, cwd=DBT_PROJECT_PATH
            )

            if debug_result.returncode != 0:
                logger.warning(f"dbt debug failed: {debug_result.stderr}")
                logger.warning(f"dbt debug output: {debug_result.stdout}")
            else:
                logger.info("dbt debug completed successfully")

            # dbt parse 실행 (프로젝트 파싱 테스트)
            logger.info("Running dbt parse...")
            parse_cmd = [dbt_cmd, "parse"]
            logger.info(f"Executing dbt parse command: {' '.join(parse_cmd)}")
            parse_result = subprocess.run(
                parse_cmd, capture_output=True, text=True, env=env, cwd=DBT_PROJECT_PATH
            )

            if parse_result.returncode != 0:
                logger.error(f"dbt parse failed: {parse_result.stderr}")
                logger.error(f"dbt parse output: {parse_result.stdout}")
                raise Exception(f"dbt project parsing failed: {parse_result.stderr}")
            else:
                logger.info("dbt parse completed successfully")

            # dbt run 실행 (모델 실행)
            logger.info("Running dbt run...")
            run_cmd = [dbt_cmd, "run"]
            logger.info(f"Executing dbt run command: {' '.join(run_cmd)}")
            run_result = subprocess.run(
                run_cmd, capture_output=True, text=True, env=env, cwd=DBT_PROJECT_PATH
            )

            if run_result.returncode != 0:
                logger.warning(f"dbt run failed: {run_result.stderr}")
                logger.warning(f"dbt run output: {run_result.stdout}")
            else:
                logger.info("dbt run completed successfully")

            # dbt 스냅샷 실행
            logger.info("Running dbt snapshot...")
            snapshot_cmd = [dbt_cmd, "snapshot"]
            logger.info(f"Executing dbt snapshot command: {' '.join(snapshot_cmd)}")
            snapshot_result = subprocess.run(
                snapshot_cmd,
                capture_output=True,
                text=True,
                env=env,
                cwd=DBT_PROJECT_PATH,
            )

            if snapshot_result.returncode != 0:
                error_msg = f"dbt snapshot failed: {snapshot_result.stderr}"
                logger.error(error_msg)
                logger.error(f"Command output: {snapshot_result.stdout}")
                logger.error(f"Command executed: {' '.join(snapshot_cmd)}")
                logger.error(f"Working directory: {os.getcwd()}")
                logger.error(f"dbt executable: {dbt_cmd}")
                logger.error(f"dbt project path: {DBT_PROJECT_PATH}")
                logger.error(
                    f"Environment variables: DBT_PROFILES_DIR={env.get('DBT_PROFILES_DIR')}, DBT_PROJECT_DIR={env.get('DBT_PROJECT_DIR')}"
                )

                # 추가 디버깅 정보
                try:
                    # dbt 실행 파일 상태 확인
                    if os.path.exists(dbt_cmd):
                        logger.error(f"dbt executable exists at: {dbt_cmd}")
                        logger.error(
                            f"dbt executable permissions: {oct(os.stat(dbt_cmd).st_mode)[-3:]}"
                        )
                    else:
                        logger.error(f"dbt executable does not exist at: {dbt_cmd}")

                    # dbt 프로젝트 디렉토리 상태 확인
                    if os.path.exists(DBT_PROJECT_PATH):
                        logger.error(
                            f"dbt project directory exists at: {DBT_PROJECT_PATH}"
                        )
                        logger.error(
                            f"dbt project directory contents: {os.listdir(DBT_PROJECT_PATH)}"
                        )
                    else:
                        logger.error(
                            f"dbt project directory does not exist at: {DBT_PROJECT_PATH}"
                        )

                except Exception as debug_e:
                    logger.error(f"Error during debug info collection: {debug_e}")

                raise Exception(error_msg)

            logger.info(
                f"dbt snapshot completed successfully: {snapshot_result.stdout}"
            )

            # 스냅샷 결과를 XCom에 저장
            target_schema, target_table_name = table_config["target"].split(".")
            context["task_instance"].xcom_push(
                key=f"dbt_snapshot_result_{target_table_name}",
                value={
                    "status": "success",
                    "stdout": snapshot_result.stdout,
                    "stderr": snapshot_result.stderr,
                    "return_code": snapshot_result.returncode,
                },
            )

            return {
                "status": "success",
                "stdout": snapshot_result.stdout,
                "stderr": snapshot_result.stderr,
                "return_code": snapshot_result.returncode,
            }

        finally:
            # 원래 작업 디렉토리로 복원
            os.chdir(original_cwd)
            logger.info(f"Restored original working directory: {original_cwd}")

    except Exception as e:
        error_msg = f"Failed to run dbt snapshot for {table_config['source']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def validate_snapshot_data(table_config: dict, **context) -> dict[str, Any]:
    """
    dbt 스냅샷 데이터 검증

    Args:
        table_config: 테이블 설정
        **context: Airflow 컨텍스트

    Returns:
        스냅샷 검증 결과
    """
    try:
        logger.info(f"Validating dbt snapshot data for table: {table_config['source']}")

        target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

        # 스냅샷 테이블명 생성 - 테이블 설정에서 동적으로 생성
        target_schema, target_table_name = table_config["target"].split(".")
        snapshot_table_name = f"{target_table_name}_snapshot"
        snapshot_table = f"snapshots.{snapshot_table_name}"

        # 스냅샷 테이블 존재 확인
        snapshot_exists_sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'snapshots'
                AND table_name = %s
            )
        """

        snapshot_exists = target_hook.get_first(
            snapshot_exists_sql, parameters=(snapshot_table_name,)
        )[0]

        if not snapshot_exists:
            raise Exception(f"Snapshot table {snapshot_table} does not exist")

        # 스냅샷 데이터 수 확인
        snapshot_count = target_hook.get_first(
            f"SELECT COUNT(*) FROM {snapshot_table}"
        )[0]

        # 원본 테이블과 비교
        original_count = target_hook.get_first(
            f"SELECT COUNT(*) FROM {table_config['target']}"
        )[0]

        logger.info(
            f"Original table count: {original_count}, Snapshot count: {snapshot_count}"
        )

        # 스냅샷 컬럼 확인
        snapshot_columns_sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'snapshots'
            AND table_name = '인포맥스종목마스터_snapshot'
            ORDER BY ordinal_position
        """

        snapshot_columns = target_hook.get_records(snapshot_columns_sql)
        snapshot_column_names = [col[0] for col in snapshot_columns]

        # dbt 스냅샷 필수 컬럼 확인
        required_columns = ["dbt_valid_from", "dbt_valid_to", "dbt_scd_id"]
        missing_columns = [
            col for col in required_columns if col not in snapshot_column_names
        ]

        if missing_columns:
            raise Exception(f"Missing required snapshot columns: {missing_columns}")

        # 스냅샷 데이터 품질 검증
        # 1. 유효한 레코드 수 확인 (dbt_valid_to IS NULL)
        valid_records = target_hook.get_first(
            f"SELECT COUNT(*) FROM {snapshot_table} WHERE dbt_valid_to IS NULL"
        )[0]

        # 2. 히스토리 레코드 수 확인 (dbt_valid_to IS NOT NULL)
        history_records = target_hook.get_first(
            f"SELECT COUNT(*) FROM {snapshot_table} WHERE dbt_valid_to IS NOT NULL"
        )[0]

        logger.info(
            f"Valid records: {valid_records}, History records: {history_records}"
        )

        # 검증 결과를 XCom에 저장
        target_schema, target_table_name = table_config["target"].split(".")
        validation_result = {
            "status": "success",
            "snapshot_table": snapshot_table,
            "snapshot_count": snapshot_count,
            "original_count": original_count,
            "valid_records": valid_records,
            "history_records": history_records,
            "snapshot_columns": snapshot_column_names,
        }

        context["task_instance"].xcom_push(
            key=f"snapshot_validation_result_{target_table_name}",
            value=validation_result,
        )

        return validation_result

    except Exception as e:
        error_msg = f"Snapshot validation failed for {table_config['source']}: {e!s}"
        logger.error(error_msg)
        raise Exception(error_msg)


def log_snapshot_results(table_config: dict, **context) -> str:
    """
    dbt 스냅샷 실행 결과 로깅

    Args:
        table_config: 테이블 설정
        **context: Airflow 컨텍스트

    Returns:
        로깅 결과 메시지
    """
    try:
        target_schema, target_table_name = table_config["target"].split(".")

        # dbt 스냅샷 실행 결과 가져오기
        snapshot_result = context["task_instance"].xcom_pull(
            key=f"dbt_snapshot_result_{target_table_name}", task_ids="run_dbt_snapshot"
        )

        # 스냅샷 검증 결과 가져오기
        validation_result = context["task_instance"].xcom_pull(
            key=f"snapshot_validation_result_{target_table_name}",
            task_ids="validate_snapshot_data",
        )

        if snapshot_result and validation_result:
            logger.info(
                f"dbt Snapshot completed successfully for {table_config['source']}"
            )
            logger.info(f"Snapshot result: {snapshot_result}")
            logger.info(f"Validation result: {validation_result}")

            return (
                f"dbt Snapshot completed successfully for {table_config['source']}. "
                f"Snapshot table: {validation_result['snapshot_table']}, "
                f"Total records: {validation_result['snapshot_count']}, "
                f"Valid records: {validation_result['valid_records']}, "
                f"History records: {validation_result['history_records']}"
            )
        else:
            error_msg = "Snapshot or validation results not found"
            logger.error(error_msg)
            return error_msg

    except Exception as e:
        error_msg = (
            f"Failed to log snapshot results for {table_config['source']}: {e!s}"
        )
        logger.error(error_msg)
        return error_msg


def process_all_tables(**context) -> str:
    """모든 테이블을 순차적으로 처리"""
    results = []

    for i, table_config in enumerate(TABLES_CONFIG):
        logger.info(
            f"Processing table {i+1}/{len(TABLES_CONFIG)}: {table_config['source']}"
        )

        try:
            # 1. 소스 연결 확인
            check_source_connection(table_config, **context)

            # 2. 타겟 연결 확인
            check_target_connection(table_config, **context)

            # 3. 타겟 테이블 존재 확인 및 생성
            ensure_target_table_exists(table_config, **context)

            # 4. 테이블 구조 로깅 (디버깅용)
            log_table_structure(table_config, **context)

            # 5. 데이터 카운트
            count_result = get_source_data_count(table_config, **context)

            # 6. 임시 테이블 생성
            create_temp_table(table_config, **context)

            # 7. 데이터 복사
            copy_result = copy_data_with_dynamic_sql(table_config, **context)

            # 8. MERGE 파라미터 준비
            merge_params = prepare_merge_parameters(table_config, **context)

            # 9. MERGE 실행 (동기화 모드에 따라)
            if table_config["sync_mode"] == "full_sync":
                merge_result = execute_full_sync_merge(
                    table_config, merge_params, **context
                )
            else:
                merge_result = execute_incremental_merge(
                    table_config, merge_params, **context
                )

            # 10. 데이터 검증
            validation_result = validate_data_integrity(table_config, **context)

            # 11. 임시 테이블 정리
            cleanup_result = cleanup_temp_table(table_config, **context)

            # 12. dbt 스냅샷 실행
            dbt_snapshot_result = run_dbt_snapshot(table_config, **context)

            # 13. dbt 스냅샷 데이터 검증
            snapshot_validation_result = validate_snapshot_data(table_config, **context)

            # 14. dbt 스냅샷 결과 로깅
            dbt_log_result = log_snapshot_results(table_config, **context)

            table_result = {
                "table": table_config["source"],
                "status": "success",
                "records_processed": count_result.get("total_count", 0),
                "sync_mode": table_config["sync_mode"],
                "sync_result": merge_result,
                "dbt_snapshot_result": dbt_snapshot_result,
                "snapshot_validation_result": snapshot_validation_result,
                "dbt_log_result": dbt_log_result,
            }

        except Exception as e:
            logger.error(f"Failed to process table {table_config['source']}: {e!s}")
            table_result = {
                "table": table_config["source"],
                "status": "failed",
                "error": str(e),
            }

        results.append(table_result)

        # 다음 테이블 처리 전 잠시 대기
        time.sleep(2)

    # 전체 결과 요약
    success_count = len([r for r in results if r["status"] == "success"])
    failed_count = len([r for r in results if r["status"] == "failed"])

    summary = f"Processing completed. Success: {success_count}, Failed: {failed_count}"
    logger.info(summary)

    # XCom에 결과 저장
    context["task_instance"].xcom_push(key="processing_results", value=results)

    # 실패한 테이블이 있으면 예외 발생하여 Airflow에서 실패로 표시
    if failed_count > 0:
        failed_tables = [r["table"] for r in results if r["status"] == "failed"]
        failed_errors = [
            f"{r['table']}: {r['error']}" for r in results if r["status"] == "failed"
        ]
        error_msg = f"Failed to process {failed_count} table(s): {', '.join(failed_tables)}. Errors: {'; '.join(failed_errors)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    return summary


def send_completion_notification(**context) -> str:
    """작업 완료 알림 전송 (성공/실패 모두 포함)"""
    try:
        # 처리 결과 가져오기
        results = context["task_instance"].xcom_pull(
            key="processing_results", task_ids="process_all_tables"
        )

        if not results:
            return "No processing results found"

        # 성공한 테이블들
        success_tables = [r for r in results if r["status"] == "success"]
        failed_tables = [r for r in results if r["status"] == "failed"]

        # dbt 스냅샷 결과 가져오기
        target_schema, target_table_name = TABLES_CONFIG[0]["target"].split(".")
        snapshot_result = context["task_instance"].xcom_pull(
            key=f"dbt_snapshot_result_{target_table_name}", task_ids="run_dbt_snapshot"
        )
        snapshot_validation = context["task_instance"].xcom_pull(
            key=f"snapshot_validation_result_{target_table_name}",
            task_ids="validate_snapshot_data",
        )

        # 알림 메시지 구성
        message = f"""
        PostgreSQL Multi-Table Copy DAG 실행 완료

        총 처리 테이블: {len(results)}개
        성공: {len(success_tables)}개
        실패: {len(failed_tables)}개

        성공한 테이블:
        {chr(10).join([f"- {r['table']} ({r['sync_mode']}): {r['records_processed']}건" for r in success_tables])}
        """

        if failed_tables:
            message += f"""

        실패한 테이블:
        {chr(10).join([f"- {r['table']}: {r['error']}" for r in failed_tables])}
        """

        # dbt 스냅샷 결과 추가
        if snapshot_result and snapshot_validation:
            message += f"""

        dbt 스냅샷 실행 결과:
        - 스냅샷 테이블: {snapshot_validation['snapshot_table']}
        - 총 레코드 수: {snapshot_validation['snapshot_count']}건
        - 유효 레코드: {snapshot_validation['valid_records']}건
        - 히스토리 레코드: {snapshot_validation['history_records']}건
        - 상태: 성공
        """
        else:
            message += """

        dbt 스냅샷 실행 결과:
        - 상태: 실패 또는 결과 없음
        """

        logger.info(message)

        # 실패한 테이블이 있으면 경고 로그로 표시
        if failed_tables:
            logger.warning(
                f"Some tables failed to process: {', '.join([r['table'] for r in failed_tables])}"
            )

        return message

    except Exception as e:
        error_msg = f"Failed to send success notification: {e!s}"
        logger.error(error_msg)
        return error_msg


# DAG 태스크 정의

# 단일 태스크로 모든 테이블 처리
process_all_tables_task = PythonOperator(
    task_id="process_all_tables",
    python_callable=process_all_tables,
    dag=dag,
    doc_md="""
    모든 테이블을 순차적으로 처리합니다.

    - 소스/타겟 연결 확인
    - 데이터 복사
    - 동기화 모드에 따른 MERGE 실행
    - 데이터 검증
    - 임시 테이블 정리
    - dbt 스냅샷 실행
    - 스냅샷 데이터 검증
    """,
)

# dbt 스냅샷 실행 태스크 (전체 테이블 처리 후)
run_dbt_snapshot_task = PythonOperator(
    task_id="run_dbt_snapshot",
    python_callable=run_dbt_snapshot,
    op_kwargs={"table_config": TABLES_CONFIG[0]},  # 첫 번째 테이블에 대해서만
    dag=dag,
    doc_md="""
    dbt 스냅샷을 실행하여 데이터 변화를 추적합니다.

    - 인포맥스종목마스터 테이블의 변경사항 캡처
    - SCD2 패턴으로 데이터 히스토리 보존
    - 변경된 레코드의 유효기간 관리
    """,
)

# dbt 스냅샷 데이터 검증 태스크
validate_snapshot_task = PythonOperator(
    task_id="validate_snapshot_data",
    python_callable=validate_snapshot_data,
    op_kwargs={"table_config": TABLES_CONFIG[0]},  # 첫 번째 테이블에 대해서만
    dag=dag,
    doc_md="""
    dbt 스냅샷 데이터의 품질을 검증합니다.

    - 스냅샷 테이블 존재 확인
    - 필수 컬럼 검증
    - 데이터 수량 검증
    - 유효/히스토리 레코드 분류
    """,
)

# dbt 스냅샷 결과 로깅 태스크
log_snapshot_task = PythonOperator(
    task_id="log_snapshot_results",
    python_callable=log_snapshot_results,
    op_kwargs={"table_config": TABLES_CONFIG[0]},  # 첫 번째 테이블에 대해서만
    dag=dag,
    doc_md="""
    dbt 스냅샷 실행 결과를 로깅합니다.

    - 스냅샷 실행 결과 요약
    - 검증 결과 요약
    - 데이터 품질 지표
    """,
)

# 완료 알림
completion_notification_task = PythonOperator(
    task_id="send_completion_notification",
    python_callable=send_completion_notification,
    dag=dag,
    doc_md="""
    작업 완료 알림을 전송합니다.

    - 처리된 테이블 수 요약
    - 성공/실패 현황
    - 각 테이블별 처리 결과
    - dbt 스냅샷 실행 결과
    """,
)

# Task 의존성 설정 (dbt 스냅샷 포함)
(
    process_all_tables_task
    >> run_dbt_snapshot_task
    >> validate_snapshot_task
    >> log_snapshot_task
    >> completion_notification_task
)
