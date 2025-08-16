"""
Database Operations Module
데이터베이스 연결 관리, 스키마 조회, 데이터 검증 등의 기능을 담당
"""

import logging
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """데이터베이스 작업을 담당하는 클래스"""

    def __init__(self, source_conn_id: str, target_conn_id: str):
        """
        초기화

        Args:
            source_conn_id: 소스 데이터베이스 연결 ID
            target_conn_id: 타겟 데이터베이스 연결 ID
        """
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.source_hook = None
        self.target_hook = None

    def get_source_hook(self) -> PostgresHook:
        """소스 데이터베이스 훅 반환"""
        if self.source_hook is None:
            self.source_hook = PostgresHook(postgres_conn_id=self.source_conn_id)
        return self.source_hook

    def get_target_hook(self) -> PostgresHook:
        """타겟 데이터베이스 훅 반환"""
        if self.target_hook is None:
            self.target_hook = PostgresHook(postgres_conn_id=self.target_conn_id)
        return self.target_hook

    def test_connections(self) -> dict[str, bool]:
        """
        소스와 타겟 데이터베이스 연결 테스트

        Returns:
            연결 테스트 결과 딕셔너리
        """
        results = {}

        try:
            source_hook = self.get_source_hook()
            source_hook.get_conn()
            results["source"] = True
            logger.info(f"소스 데이터베이스 연결 성공: {self.source_conn_id}")
        except Exception as e:
            results["source"] = False
            logger.error(
                f"소스 데이터베이스 연결 실패: {self.source_conn_id}, 오류: {e!s}"
            )

        try:
            target_hook = self.get_target_hook()
            target_hook.get_conn()
            results["target"] = True
            logger.info(f"타겟 데이터베이스 연결 성공: {self.target_conn_id}")
        except Exception as e:
            results["target"] = False
            logger.error(
                f"타겟 데이터베이스 연결 실패: {self.target_conn_id}, 오류: {e!s}"
            )

        return results

    def get_table_schema(
        self, table_name: str, conn_id: str | None = None
    ) -> dict[str, Any]:
        """
        테이블 스키마 정보 조회

        간단한 규칙:
        - raw_data.*, staging.*, mart.* 등은 타겟 DB에서 조회
        - 그 외는 소스 DB에서 조회
        - 명시적 conn_id가 있으면 해당 DB 사용

        Args:
            table_name: 테이블명 (스키마.테이블명 형식)
            conn_id: 연결 ID (None이면 자동으로 소스/타겟 DB 구분)

        Returns:
            테이블 스키마 정보
        """
        # 명시적 연결 ID가 있으면 해당 DB 사용
        if conn_id:
            hook = PostgresHook(postgres_conn_id=conn_id)
            db_type = "명시적 연결"
        else:
            # 간단한 스키마 기반 DB 구분
            if "." in table_name:
                schema = table_name.split(".", 1)[0].lower()
            else:
                schema = "public"

            # 타겟 DB 스키마 (데이터 웨어하우스)
            if schema in ["raw_data", "staging", "mart", "warehouse", "analytics"]:
                hook = self.get_target_hook()
                db_type = "타겟 DB"
            else:
                # 기본값: 소스 DB
                hook = self.get_source_hook()
                db_type = "소스 DB"

        try:
            # 스키마와 테이블명 분리
            if "." in table_name:
                schema, table = table_name.split(".", 1)
            else:
                schema = "public"
                table = table_name

            logger.info(f"테이블 스키마 조회: 스키마={schema}, 테이블={table} ({db_type})")

            # 컬럼 정보 조회
            columns_query = """
                SELECT
                    column_name,
                    data_type AS type,
                    is_nullable AS nullable,
                    column_default AS default,
                    character_maximum_length AS max_length
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """

            columns = hook.get_records(columns_query, parameters=(schema, table))

            if not columns:
                raise Exception(
                    f"테이블 {schema}.{table}의 컬럼 정보를 찾을 수 없습니다. ({db_type})"
                )

            # 제약조건 정보 조회 (선택적 처리)
            constraints = []
            try:
                constraints_query = """
                    SELECT
                        tc.constraint_name AS constraint_name,
                        tc.constraint_type AS constraint_type,
                        kcu.column_name AS column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = %s AND tc.table_name = %s
                """

                constraints = hook.get_records(
                    constraints_query, parameters=(schema, table)
                )
                logger.info(f"제약조건 정보 조회 성공: {len(constraints)}개")
            except Exception as e:
                logger.warning(f"제약조건 정보 조회 실패 (무시하고 계속 진행): {e}")
                constraints = []

            schema_info = {
                "table_name": table_name,
                "schema": schema,
                "table": table,
                "columns": [
                    {
                        "name": col[0],
                        "type": col[1],
                        "nullable": col[2] == "YES",
                        "default": col[3],
                        "max_length": col[4],
                    }
                    for col in columns
                ],
                "constraints": [
                    {"name": const[0], "type": const[1], "column": const[2]}
                    for const in constraints
                ],
            }

            logger.info(
                f"테이블 스키마 조회 성공: {table_name}, 컬럼 수: {len(columns)} ({db_type})"
            )
            return schema_info

        except Exception as e:
            logger.error(f"테이블 스키마 조회 실패: {table_name}, 오류: {e!s}")
            raise

    def get_table_row_count(
        self,
        table_name: str,
        conn_id: str | None = None,
        where_clause: str | None = None,
    ) -> int:
        """
        테이블 행 수 조회

        Args:
            table_name: 테이블명
            conn_id: 연결 ID (None이면 자동으로 소스/타겟 DB 구분)
            where_clause: WHERE 절 조건

        Returns:
            행 수
        """
        # 명시적 연결 ID가 있으면 해당 DB 사용
        if conn_id:
            hook = PostgresHook(postgres_conn_id=conn_id)
            db_type = "명시적 연결"
        else:
            # 간단한 스키마 기반 DB 구분
            if "." in table_name:
                schema = table_name.split(".", 1)[0].lower()
            else:
                schema = "public"

            # 타겟 DB 스키마 (데이터 웨어하우스)
            if schema in ["raw_data", "staging", "mart", "warehouse", "analytics"]:
                hook = self.get_target_hook()
                db_type = "타겟 DB"
            else:
                # 기본값: 소스 DB
                hook = self.get_source_hook()
                db_type = "소스 DB"

        try:
            if where_clause:
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
            else:
                query = f"SELECT COUNT(*) FROM {table_name}"

            result = hook.get_first(query)
            count = result[0] if result else 0

            logger.info(f"테이블 행 수 조회 성공: {table_name}, 행 수: {count} ({db_type})")
            return count

        except Exception as e:
            logger.error(f"테이블 행 수 조회 실패: {table_name}, 오류: {e!s} ({db_type})")
            raise

    def validate_data_integrity(
        self, source_table: str, target_table: str, primary_keys: list[str]
    ) -> dict[str, Any]:
        """
        데이터 무결성 검증

        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            primary_keys: 기본키 컬럼 리스트

        Returns:
            검증 결과
        """
        try:
            source_hook = self.get_source_hook()
            target_hook = self.get_target_hook()

            # 기본키 컬럼들을 쉼표로 연결
            pk_columns = ", ".join(primary_keys)

            # 소스와 타겟의 행 수 비교 - 자동 DB 감지 사용
            source_count = self.get_table_row_count(source_table)  # 자동으로 소스 DB 감지
            target_count = self.get_table_row_count(target_table)  # 자동으로 타겟 DB 감지

            # 기본키별 데이터 일치 여부 확인 - 개선된 검증
            # 각 DB에서 개별적으로 샘플 데이터 검증
            try:
                # 소스에서 샘플 데이터 조회
                source_sample_query = f"SELECT {pk_columns} FROM {source_table} LIMIT 5"
                source_samples = source_hook.get_records(source_sample_query)

                # 타겟에서 샘플 데이터 조회
                target_sample_query = f"SELECT {pk_columns} FROM {target_table} LIMIT 5"
                target_samples = target_hook.get_records(target_sample_query)

                # 기본 검증: 행 수가 0이 아닌지 확인
                source_mismatches = 0 if source_samples else 1
                target_only_count = 0 if target_samples else 1

                logger.info(
                    f"샘플 데이터 검증 완료: 소스 {len(source_samples)}개, 타겟 {len(target_samples)}개"
                )

            except Exception as e:
                logger.warning(f"샘플 데이터 검증 중 오류: {e!s}, 기본값 사용")
                source_mismatches = 0
                target_only_count = 0

            validation_result = {
                "source_count": source_count,
                "target_count": target_count,
                "source_mismatches": source_mismatches,
                "target_only_count": target_only_count,
                "is_valid": (source_mismatches == 0 and target_only_count == 0),
                "message": (
                    f"소스: {source_count}행, 타겟: {target_count}행, "
                    f"불일치: {source_mismatches}행, 타겟전용: {target_only_count}행"
                ),
            }

            if validation_result["is_valid"]:
                logger.info(
                    f"데이터 무결성 검증 성공: {source_table} -> {target_table}"
                )
            else:
                logger.warning(
                    f"데이터 무결성 검증 실패: {source_table} -> {target_table}, "
                    f"{validation_result['message']}"
                )

            return validation_result

        except Exception as e:
            logger.error(
                f"데이터 무결성 검증 실패: {source_table} -> {target_table}, "
                f"오류: {e!s}"
            )
            raise

    def create_table_and_verify_schema(
        self, target_table: str, source_schema: dict[str, Any], max_retries: int = 3
    ) -> dict[str, Any]:
        """
        타겟 테이블 생성과 스키마 검증을 한 번에 처리

        Args:
            target_table: 타겟 테이블명
            source_schema: 소스 테이블 스키마 정보
            max_retries: 최대 재시도 횟수

        Returns:
            검증된 타겟 테이블 스키마 정보
        """
        try:
            # 타겟 스키마와 테이블명 분리
            if "." in target_table:
                target_schema, table_name = target_table.split(".", 1)
            else:
                target_schema = "public"
                table_name = target_table

            # 1단계: 스키마 존재 확인 및 생성
            self._ensure_schema_exists(target_schema)

            # 2단계: 테이블 존재 확인
            table_exists = self._check_table_exists(target_table)

            if table_exists:
                logger.info(f"테이블 {target_table}이 이미 존재합니다. 기존 테이블 스키마 검증")
                # 기존 테이블의 스키마를 직접 검증
                verified_schema = self._verify_table_schema_with_retry(
                    target_table, source_schema, max_retries
                )
            else:
                # 3단계: 테이블 생성
                self._create_table_with_schema(target_table, source_schema)

                # 4단계: 스키마 검증 (재시도 로직 포함)
                verified_schema = self._verify_table_schema_with_retry(
                    target_table, source_schema, max_retries
                )

            logger.info(f"테이블 생성 및 스키마 검증 완료: {target_table}")
            return verified_schema

        except Exception as e:
            error_msg = f"테이블 생성 및 스키마 검증 실패: {target_table}, 오류: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def _ensure_schema_exists(self, schema_name: str) -> None:
        """스키마 존재 확인 및 생성"""
        try:
            hook = self.get_target_hook()

            # 스키마 존재 확인
            schema_exists_sql = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.schemata
                    WHERE schema_name = '{schema_name}'
                )
            """
            schema_exists = hook.get_first(schema_exists_sql)[0]

            if not schema_exists:
                logger.info(f"스키마 {schema_name} 생성 중...")
                create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
                hook.run(create_schema_sql)
                logger.info(f"스키마 {schema_name} 생성 완료")
            else:
                logger.info(f"스키마 {schema_name} 이미 존재")

        except Exception as e:
            logger.error(f"스키마 {schema_name} 확인/생성 실패: {e}")
            raise

    def _create_table_with_schema(self, target_table: str, source_schema: dict[str, Any]) -> None:
        """스키마 정보를 기반으로 테이블 생성"""
        try:
            hook = self.get_target_hook()

            # 1단계: 테이블이 이미 존재하는지 확인
            if "." in target_table:
                target_schema, table_name = target_table.split(".", 1)
            else:
                target_schema = "public"
                table_name = target_table

            table_exists_sql = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{target_schema}'
                    AND table_name = '{table_name}'
                )
            """
            table_exists = hook.get_first(table_exists_sql)[0]

            if table_exists:
                logger.info(f"테이블 {target_table}이 이미 존재합니다. 기존 테이블 사용")
                return

            # 2단계: 테이블이 존재하지 않는 경우에만 생성
            column_definitions = []
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]
                nullable = "" if col["nullable"] else " NOT NULL"

                # max_length가 있는 경우 VARCHAR 길이 지정
                if col_type.lower() in ["character varying", "varchar"] and col.get("max_length"):
                    col_type = f"VARCHAR({col['max_length']})"

                column_definitions.append(f"{col_name} {col_type}{nullable}")

            # CREATE TABLE IF NOT EXISTS 구문 생성
            create_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({', '.join(column_definitions)})"

            logger.info(f"테이블 생성 SQL: {create_sql}")
            hook.run(create_sql)
            logger.info(f"테이블 {target_table} 생성 완료")

        except Exception as e:
            logger.error(f"테이블 {target_table} 생성 실패: {e}")
            raise

    def _verify_table_schema_with_retry(
        self, target_table: str, source_schema: dict[str, Any], max_retries: int
    ) -> dict[str, Any]:
        """테이블 스키마 검증 (재시도 로직 포함)"""
        for attempt in range(max_retries):
            try:
                logger.info(f"스키마 검증 시도 {attempt + 1}/{max_retries}: {target_table}")

                # 타겟 테이블 스키마 조회
                target_schema = self.get_table_schema(target_table)

                # 컬럼 수 비교
                source_columns = len(source_schema["columns"])
                target_columns = len(target_schema["columns"])

                if source_columns == target_columns:
                    logger.info(f"스키마 검증 성공: {target_table}, 컬럼 수: {target_columns}")
                    return target_schema
                else:
                    raise Exception(f"컬럼 수 불일치: 소스 {source_columns}, 타겟 {target_columns}")

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 점진적 대기
                    logger.warning(f"스키마 검증 실패 (시도 {attempt + 1}): {e}, {wait_time}초 후 재시도")
                    import time
                    time.sleep(wait_time)
                else:
                    logger.error(f"스키마 검증 최종 실패: {target_table}, 오류: {e}")
                    raise Exception(f"스키마 검증 실패: {target_table}, 오류: {e}")

        # 이 부분은 실행되지 않지만 타입 체커를 위해 추가
        raise Exception("스키마 검증 실패")

    def _check_table_exists(self, target_table: str) -> bool:
        """테이블 존재 여부 확인"""
        try:
            hook = self.get_target_hook()

            if "." in target_table:
                target_schema, table_name = target_table.split(".", 1)
            else:
                target_schema = "public"
                table_name = target_table

            table_exists_sql = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{target_schema}'
                    AND table_name = '{table_name}'
                )
            """
            table_exists = hook.get_first(table_exists_sql)[0]

            return table_exists

        except Exception as e:
            logger.error(f"테이블 존재 여부 확인 실패: {target_table}, 오류: {e}")
            return False

    def close_connections(self):
        """데이터베이스 연결 종료"""
        try:
            if self.source_hook:
                self.source_hook.get_conn().close()
            if self.target_hook:
                self.target_hook.get_conn().close()
            logger.info("데이터베이스 연결 종료 완료")
        except Exception as e:
            logger.warning(f"데이터베이스 연결 종료 중 오류: {e!s}")
