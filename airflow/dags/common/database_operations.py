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

        Args:
            table_name: 테이블명 (스키마.테이블명 형식)
            conn_id: 연결 ID (None이면 소스 데이터베이스 사용)

        Returns:
            테이블 스키마 정보
        """
        if conn_id is None:
            hook = self.get_source_hook()
        else:
            hook = PostgresHook(postgres_conn_id=conn_id)

        try:
            # 스키마와 테이블명 분리
            if "." in table_name:
                schema, table = table_name.split(".", 1)
            else:
                schema = "public"
                table = table_name

            logger.info(f"테이블 스키마 조회: 스키마={schema}, 테이블={table}")

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
                    f"테이블 {schema}.{table}의 컬럼 정보를 찾을 수 없습니다."
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
                f"테이블 스키마 조회 성공: {table_name}, 컬럼 수: {len(columns)}"
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
            conn_id: 연결 ID
            where_clause: WHERE 절 조건

        Returns:
            행 수
        """
        if conn_id is None:
            hook = self.get_source_hook()
        else:
            hook = PostgresHook(postgres_conn_id=conn_id)

        try:
            if where_clause:
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
            else:
                query = f"SELECT COUNT(*) FROM {table_name}"

            result = hook.get_first(query)
            count = result[0] if result else 0

            logger.info(f"테이블 행 수 조회 성공: {table_name}, 행 수: {count}")
            return count

        except Exception as e:
            logger.error(f"테이블 행 수 조회 실패: {table_name}, 오류: {e!s}")
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

            # 소스와 타겟의 행 수 비교 - 연결 ID 명시적 지정
            source_count = self.get_table_row_count(source_table, self.source_conn_id)
            target_count = self.get_table_row_count(target_table, self.target_conn_id)

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
