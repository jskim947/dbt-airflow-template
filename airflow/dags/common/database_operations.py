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
        소스와 타겟 데이터베이스 연결을 테스트

        Returns:
            연결 테스트 결과 딕셔너리
        """
        results = {}
        
        try:
            # 소스 연결 테스트
            source_hook = self.get_source_hook()
            source_conn = source_hook.get_conn()
            source_conn.close()
            results["source"] = True
            logger.info(f"Source connection {self.source_conn_id} test successful")
        except Exception as e:
            results["source"] = False
            logger.error(f"Source connection {self.source_conn_id} test failed: {e}")

        try:
            # 타겟 연결 테스트
            target_hook = self.get_target_hook()
            target_conn = target_hook.get_conn()
            target_conn.close()
            results["target"] = True
            logger.info(f"Target connection {self.target_conn_id} test successful")
        except Exception as e:
            results["target"] = False
            logger.error(f"Target connection {self.target_conn_id} test failed: {e}")

        return results

    def get_table_schema(self, table_name: str) -> dict[str, Any]:
        """
        테이블의 스키마 정보를 조회

        Args:
            table_name: 테이블명 (schema.table 형식)

        Returns:
            테이블 스키마 정보 딕셔너리
        """
        try:
            source_hook = self.get_source_hook()
            schema, table = table_name.split(".")
            
            # 컬럼 정보 조회
            columns_sql = """
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable, 
                    column_default,
                    character_maximum_length,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            
            columns = source_hook.get_records(columns_sql, parameters=(schema, table))
            
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
                        "position": col[5]
                    }
                    for col in columns
                ]
            }
            
            logger.info(f"Retrieved schema for table {table_name}: {len(schema_info['columns'])} columns")
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get table schema for {table_name}: {e}")
            raise

    def create_table_if_not_exists(self, target_table: str, source_schema: dict[str, Any]) -> str:
        """
        타겟 테이블이 존재하지 않을 경우 소스 테이블 구조를 기반으로 생성

        Args:
            target_table: 생성할 타겟 테이블명
            source_schema: 소스 테이블 스키마 정보

        Returns:
            생성 결과 메시지
        """
        try:
            logger.info(f"Ensuring target table exists: {target_table}")

            target_hook = self.get_target_hook()

            # 타겟 스키마와 테이블명 분리
            target_schema, target_table_name = target_table.split(".")

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
                    AND table_name = '{target_table_name}'
                )
            """

            logger.info(f"Checking table existence with SQL: {check_sql}")
            table_exists = target_hook.get_first(check_sql)[0]
            logger.info(f"Table existence check result: {table_exists}")

            if table_exists:
                logger.info(f"Target table {target_table} already exists")
                # 실제로 테이블이 정말 존재하는지 한 번 더 확인
                verify_sql = f"SELECT COUNT(*) FROM {target_table} LIMIT 1"
                try:
                    row_count = target_hook.get_first(verify_sql)[0]
                    logger.info(f"Table verification successful: {row_count} rows found")
                    return f"Target table {target_table} already exists and verified"
                except Exception as verify_error:
                    logger.warning(f"Table verification failed: {verify_error}, will recreate table")
                    # 테이블이 실제로는 존재하지 않으므로 생성 진행
                    pass

            # CREATE TABLE 구문 생성
            create_sql = f"CREATE TABLE {target_table} ("
            column_definitions = []

            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]
                is_nullable = col["nullable"]
                col_default = col["default"]
                max_length = col["max_length"]

                # PostgreSQL 타입 매핑
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
                f"Target table {target_table} created successfully with {len(source_schema['columns'])} columns"
            )

            return (
                f"Target table {target_table} created with {len(source_schema['columns'])} columns"
            )

        except Exception as e:
            error_msg = (
                f"Failed to ensure target table exists for {target_table}: {e!s}"
            )
            logger.error(error_msg)
            raise Exception(error_msg)

    def ensure_target_table_exists(self, table_config: dict[str, Any], **context) -> str:
        """
        타겟 테이블이 존재하지 않을 경우 소스 테이블 구조를 기반으로 생성
        (기존 DAG와의 호환성을 위해 유지)

        Args:
            table_config: 테이블 설정 딕셔너리
            **context: Airflow 컨텍스트

        Returns:
            생성 결과 메시지
        """
        try:
            # 소스 테이블 스키마 조회
            source_schema = self.get_table_schema(table_config["source"])
            
            # 타겟 테이블 생성
            return self.create_table_if_not_exists(table_config["target"], source_schema)
            
        except Exception as e:
            error_msg = f"Failed to ensure target table exists: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def get_table_row_count(self, table_name: str, where_clause: str = None, use_target_db: bool = False) -> int:
        """
        테이블의 행 수를 조회

        Args:
            table_name: 테이블명
            where_clause: WHERE 조건절 (선택사항)
            use_target_db: 타겟 DB 사용 여부 (False: 소스 DB, True: 타겟 DB)

        Returns:
            행 수
        """
        try:
            if use_target_db:
                hook = self.get_target_hook()
            else:
                hook = self.get_source_hook()
                
            if where_clause:
                count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
            else:
                count_sql = f"SELECT COUNT(*) FROM {table_name}"
            
            result = hook.get_first(count_sql)
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Failed to get row count for table {table_name} from {'target' if use_target_db else 'source'} DB: {e}")
            raise

    def validate_data_integrity(
        self, source_table: str, target_table: str, primary_keys: list[str], where_clause: str = None
    ) -> dict[str, Any]:
        """
        데이터 무결성 검증

        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            primary_keys: 기본키 컬럼 목록
            where_clause: WHERE 조건 (선택사항)

        Returns:
            검증 결과 딕셔너리
        """
        try:
            source_hook = self.get_source_hook()
            target_hook = self.get_target_hook()

            # WHERE 조건이 있는 경우와 없는 경우를 구분하여 처리
            if where_clause:
                # WHERE 조건이 적용된 경우: 조건에 맞는 데이터만 비교
                logger.info(f"WHERE 조건이 적용된 무결성 검증: {where_clause}")
                
                # 소스 테이블 행 수 (WHERE 조건 적용)
                source_count = self.get_table_row_count(
                    source_table, 
                    where_clause=where_clause, 
                    use_target_db=False
                )
                
                # 타겟 테이블 행 수 (전체)
                target_count = self.get_table_row_count(target_table, use_target_db=True)
                
                # 기본키 기반 데이터 일치 여부 확인 (WHERE 조건 적용)
                if primary_keys:
                    pk_columns = ", ".join(primary_keys)
                    source_pk_sql = f"SELECT {pk_columns} FROM {source_table} WHERE {where_clause} ORDER BY {pk_columns}"
                    target_pk_sql = f"SELECT {pk_columns} FROM {target_table} ORDER BY {pk_columns}"
                    
                    source_pks = source_hook.get_records(source_pk_sql)
                    target_pks = target_hook.get_records(target_pk_sql)
                    
                    # 기본키 값들을 튜플로 변환하여 비교
                    source_pk_set = {tuple(pk) for pk in source_pks}
                    target_pk_set = {tuple(pk) for pk in target_pks}
                    
                    missing_in_target = source_pk_set - target_pk_set
                    extra_in_target = target_pk_set - source_pk_set
                    
                    is_valid = len(missing_in_target) == 0 and len(extra_in_target) == 0
                    
                    validation_result = {
                        "is_valid": is_valid,
                        "source_count": source_count,
                        "target_count": target_count,
                        "missing_in_target": len(missing_in_target),
                        "extra_in_target": len(extra_in_target),
                        "where_clause": where_clause,
                        "message": f"WHERE 조건 적용 - Source: {source_count} rows, Target: {target_count} rows"
                    }
                    
                    if not is_valid:
                        validation_result["message"] += f", Missing: {len(missing_in_target)}, Extra: {len(extra_in_target)}"
                
                else:
                    # 기본키가 없는 경우 행 수만 비교
                    is_valid = source_count == target_count
                    validation_result = {
                        "is_valid": is_valid,
                        "source_count": source_count,
                        "target_count": target_count,
                        "where_clause": where_clause,
                        "message": f"WHERE 조건 적용 - Row count comparison - Source: {source_count}, Target: {target_count}"
                    }
            else:
                # WHERE 조건이 없는 경우: 전체 테이블 비교 (기존 로직)
                logger.info("전체 테이블 무결성 검증")
                
                # 소스 테이블 행 수 (소스 DB에서)
                source_count = self.get_table_row_count(source_table, use_target_db=False)
                
                # 타겟 테이블 행 수 (타겟 DB에서)
                target_count = self.get_table_row_count(target_table, use_target_db=True)

                # 기본키 기반 데이터 일치 여부 확인
                if primary_keys:
                    pk_columns = ", ".join(primary_keys)
                    source_pk_sql = f"SELECT {pk_columns} FROM {source_table} ORDER BY {pk_columns}"
                    target_pk_sql = f"SELECT {pk_columns} FROM {target_table} ORDER BY {pk_columns}"
                    
                    source_pks = source_hook.get_records(source_pk_sql)
                    target_pks = target_hook.get_records(target_pk_sql)
                    
                    # 기본키 값들을 튜플로 변환하여 비교
                    source_pk_set = {tuple(pk) for pk in source_pks}
                    target_pk_set = {tuple(pk) for pk in target_pks}
                    
                    missing_in_target = source_pk_set - target_pk_set
                    extra_in_target = target_pk_set - source_pk_set
                    
                    is_valid = len(missing_in_target) == 0 and len(extra_in_target) == 0
                    
                    validation_result = {
                        "is_valid": is_valid,
                        "source_count": source_count,
                        "target_count": target_count,
                        "missing_in_target": len(missing_in_target),
                        "extra_in_target": len(extra_in_target),
                        "message": f"전체 테이블 - Source: {source_count} rows, Target: {target_count} rows"
                    }
                    
                    if not is_valid:
                        validation_result["message"] += f", Missing: {len(missing_in_target)}, Extra: {len(extra_in_target)}"
                    
                else:
                    # 기본키가 없는 경우 행 수만 비교
                    is_valid = source_count == target_count
                    validation_result = {
                        "is_valid": is_valid,
                        "source_count": source_count,
                        "target_count": target_count,
                        "message": f"전체 테이블 - Row count comparison - Source: {source_count}, Target: {target_count}"
                    }

            logger.info(f"Data integrity validation result: {validation_result}")
            return validation_result

        except Exception as e:
            logger.error(f"Data integrity validation failed: {e}")
            return {
                "is_valid": False,
                "message": f"Validation error: {e}"
            }

    def validate_incremental_data_integrity(self, source_table: str, target_table: str, where_clause: str) -> dict:
        """
        증분 동기화 데이터 무결성 검증

        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            where_clause: 증분 조건 (예: "changed >= '20250812'")

        Returns:
            검증 결과 딕셔너리
        """
        try:
            # 증분 조건에 해당하는 소스 데이터 개수
            source_incremental_count = self.get_table_row_count(
                source_table, 
                where_clause=where_clause,
                use_target_db=False  # 소스 DB에서 조회
            )
            
            # 타겟 테이블의 전체 데이터 개수 (증분 데이터만 있어야 함)
            target_count = self.get_table_row_count(
                target_table,
                use_target_db=True  # 타겟 DB에서 조회
            )
            
            # 증분 데이터 무결성 검증
            is_valid = source_incremental_count == target_count
            missing_count = max(0, source_incremental_count - target_count)
            extra_count = max(0, target_count - source_incremental_count)
            
            result = {
                "is_valid": is_valid,
                "source_incremental_count": source_incremental_count,
                "target_count": target_count,
                "missing_count": missing_count,
                "extra_count": extra_count,
                "sync_type": "incremental",
                "where_clause": where_clause,
                "message": (
                    f"증분 동기화 검증: 소스 증분 {source_incremental_count}행, "
                    f"타겟 {target_count}행, "
                    f"누락 {missing_count}행, "
                    f"초과 {extra_count}행"
                )
            }
            
            logger.info(f"Incremental data integrity validation result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"증분 데이터 무결성 검증 실패: {e}")
            return {
                "is_valid": False,
                "message": f"증분 검증 오류: {e}"
            }

    def create_table_and_verify_schema(
        self, 
        target_table: str, 
        source_schema: dict[str, Any],
        table_type: str = "standard"
    ) -> dict[str, Any]:
        """
        EDI 테이블을 위한 타겟 테이블 생성 및 스키마 검증

        Args:
            target_table: 타겟 테이블명
            source_schema: 소스 테이블 스키마 정보
            table_type: 테이블 타입 ("standard", "edi")

        Returns:
            검증된 스키마 정보
        """
        try:
            target_hook = self.get_target_hook()
            
            # 스키마와 테이블명 분리
            if "." in target_table:
                schema_name, table_name = target_table.split(".", 1)
            else:
                schema_name = "raw_data"
                table_name = target_table

            # 스키마 존재 확인 및 생성
            schema_exists_sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.schemata
                    WHERE schema_name = %s
                )
            """
            schema_exists = target_hook.get_first(schema_exists_sql, parameters=(schema_name,))[0]
            
            if not schema_exists:
                target_hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                logger.info(f"Schema {schema_name} created")

            # 테이블 존재 확인
            table_exists_sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            """
            table_exists = target_hook.get_first(table_exists_sql, parameters=(schema_name, table_name))[0]

            if not table_exists:
                # EDI 테이블 생성
                if table_type == "edi":
                    create_table_sql = self._generate_edi_create_table_sql(
                        target_table, source_schema
                    )
                else:
                    create_table_sql = self._generate_standard_create_table_sql(
                        target_table, source_schema
                    )
                
                target_hook.run(create_table_sql)
                logger.info(f"Table {target_table} created")

            # 생성된 테이블의 스키마 검증
            verified_schema = self._verify_target_table_schema(target_table, source_schema)
            
            return verified_schema

        except Exception as e:
            error_msg = f"Failed to create table and verify schema for {target_table}: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def _generate_edi_create_table_sql(
        self, 
        target_table: str, 
        source_schema: dict[str, Any]
    ) -> str:
        """EDI 테이블 생성을 위한 SQL 생성"""
        columns = []
        
        for column_info in source_schema["columns"]:
            column_name = column_info["name"]
            data_type = column_info["type"]
            is_nullable = column_info["nullable"]
            
            # EDI 테이블은 모든 컬럼을 TEXT로 생성 (유연성 확보)
            pg_type = "TEXT"
            
            if not is_nullable:
                columns.append(f'"{column_name}" {pg_type} NOT NULL')
            else:
                columns.append(f'"{column_name}" {pg_type}')
        
        columns_sql = ", ".join(columns)
        create_sql = f"CREATE TABLE {target_table} ({columns_sql})"
        
        return create_sql

    def _generate_standard_create_table_sql(
        self, 
        target_table: str, 
        source_schema: dict[str, Any]
    ) -> str:
        """표준 테이블 생성을 위한 SQL 생성"""
        columns = []
        
        for column_info in source_schema["columns"]:
            column_name = column_info["name"]
            data_type = column_info["type"]
            is_nullable = column_info["nullable"]
            max_length = column_info.get("max_length")
            
            # PostgreSQL 타입으로 변환
            pg_type = self._convert_to_postgres_type(data_type, max_length)
            
            if not is_nullable:
                columns.append(f'"{column_name}" {pg_type} NOT NULL')
            else:
                columns.append(f'"{column_name}" {pg_type}')
        
        columns_sql = ", ".join(columns)
        create_sql = f"CREATE TABLE {target_table} ({columns_sql})"
        
        return create_sql

    def _verify_target_table_schema(
        self, 
        target_table: str, 
        source_schema: dict[str, Any]
    ) -> dict[str, Any]:
        """타겟 테이블 스키마 검증"""
        try:
            target_hook = self.get_target_hook()
            
            # 타겟 테이블의 실제 스키마 조회
            target_schema_sql = """
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable, 
                    column_default,
                    character_maximum_length,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            
            if "." in target_table:
                schema_name, table_name = target_table.split(".", 1)
            else:
                schema_name = "raw_data"
                table_name = target_table
            
            target_columns = target_hook.get_records(
                target_schema_sql, parameters=(schema_name, table_name)
            )
            
            # 스키마 정보를 딕셔너리 형태로 변환
            verified_columns = []
            for col in target_columns:
                verified_columns.append({
                    "name": col[0],
                    "type": col[1],
                    "nullable": col[2] == "YES",
                    "default": col[3],
                    "max_length": col[4],
                    "position": col[5]
                })
            
            # get_table_schema와 동일한 구조로 반환
            verified_schema = {
                "table_name": table_name,
                "schema": schema_name,
                "table": table_name,
                "columns": verified_columns
            }
            
            logger.info(f"Schema verification completed for {target_table}: {len(verified_columns)} columns")
            return verified_schema
            
        except Exception as e:
            logger.error(f"Schema verification failed for {target_table}: {target_table}: {e}")
            raise

    def _convert_to_postgres_type(self, col_type: str, max_length: int | None = None) -> str:
        """데이터베이스 타입을 PostgreSQL 타입으로 변환"""
        col_type_lower = col_type.lower()

        # 문자열 타입
        if "char" in col_type_lower or "text" in col_type_lower:
            if max_length and max_length > 0:
                return f"VARCHAR({max_length})"
            else:
                return "TEXT"

        # 숫자 타입
        elif "int" in col_type_lower:
            if "bigint" in col_type_lower:
                return "BIGINT"
            elif "smallint" in col_type_lower:
                return "INTEGER"
            else:
                return "INTEGER"

        elif "decimal" in col_type_lower or "numeric" in col_type_lower:
            return "NUMERIC"

        elif "float" in col_type_lower or "double" in col_type_lower:
            return "DOUBLE PRECISION"

        elif "real" in col_type_lower:
            return "REAL"

        # 날짜/시간 타입
        elif "date" in col_type_lower:
            return "DATE"

        elif "time" in col_type_lower:
            if "timestamp" in col_type_lower:
                return "TIMESTAMP"
            else:
                return "TIME"

        # 불린 타입
        elif "bool" in col_type_lower:
            return "BOOLEAN"

        # 기타 타입
        elif "json" in col_type_lower:
            return "JSONB"

        elif "uuid" in col_type_lower:
            return "UUID"

        # 기본값 - 안전하게 TEXT로 처리
        else:
            return "TEXT"

    def close_connections(self):
        """데이터베이스 연결을 닫음"""
        try:
            if self.source_hook:
                self.source_hook.get_conn().close()
            if self.target_hook:
                self.target_hook.get_conn().close()
            logger.info("Database connections closed")
        except Exception as e:
            logger.warning(f"Error closing connections: {e}")


