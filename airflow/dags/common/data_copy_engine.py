"""
Data Copy Engine Module
데이터 복사 핵심 로직, CSV 내보내기/가져오기, 임시 테이블 관리, MERGE 작업 등을 담당
"""

import logging
import os
from typing import Any

import pandas as pd

from .database_operations import DatabaseOperations

logger = logging.getLogger(__name__)


class DataCopyEngine:
    """데이터 복사 엔진 클래스"""

    def __init__(self, db_ops: DatabaseOperations, temp_dir: str = "/tmp"):
        """
        초기화

        Args:
            db_ops: DatabaseOperations 인스턴스
            temp_dir: 임시 파일 저장 디렉토리
        """
        self.db_ops = db_ops
        self.temp_dir = temp_dir
        self.source_hook = db_ops.get_source_hook()
        self.target_hook = db_ops.get_target_hook()

    def export_to_csv(
        self,
        table_name: str,
        csv_path: str,
        where_clause: str | None = None,
        batch_size: int = 10000,
    ) -> int:
        """
        테이블을 CSV로 내보내기

        Args:
            table_name: 테이블명
            csv_path: CSV 파일 경로
            where_clause: WHERE 절 조건
            batch_size: 배치 크기

        Returns:
            내보낸 행 수
        """
        try:
            # 전체 행 수 조회
            total_count = self.db_ops.get_table_row_count(
                table_name, where_clause=where_clause
            )

            if total_count == 0:
                logger.warning(f"테이블 {table_name}에 데이터가 없습니다.")
                return 0

            # 스키마와 테이블명 분리
            if "." in table_name:
                schema, table = table_name.split(".", 1)
            else:
                schema = "public"
                table = table_name

            # 먼저 테이블 스키마에서 컬럼명 가져오기
            try:
                schema_query = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """
                schema_columns = self.source_hook.get_records(
                    schema_query, parameters=(schema, table)
                )
                if schema_columns:
                    columns = [col[0] for col in schema_columns]
                    logger.info(f"스키마에서 컬럼명을 가져왔습니다: {columns}")
                else:
                    raise Exception("스키마에서 컬럼명을 찾을 수 없습니다.")
            except Exception as e:
                logger.error(f"스키마에서 컬럼명 가져오기 실패: {e}")
                raise

            # 배치 단위로 데이터 추출
            offset = 0
            all_data = []

            while offset < total_count:
                if where_clause:
                    query = f"""
                        SELECT * FROM {table_name}
                        WHERE {where_clause}
                        ORDER BY 1
                        LIMIT {batch_size} OFFSET {offset}
                    """
                else:
                    query = f"""
                        SELECT * FROM {table_name}
                        ORDER BY 1
                        LIMIT {batch_size} OFFSET {offset}
                    """

                batch_data = self.source_hook.get_records(query)
                all_data.extend(batch_data)
                offset += batch_size

                logger.info(
                    f"배치 처리 진행률: {min(offset, total_count)}/{total_count}"
                )

            # DataFrame으로 변환
            if all_data:
                # 이미 가져온 컬럼명 사용
                df = pd.DataFrame(all_data, columns=columns)

                # CSV로 저장
                df.to_csv(csv_path, index=False, encoding="utf-8")

                logger.info(
                    f"CSV 내보내기 완료: {table_name} -> {csv_path}, 행 수: {len(df)}"
                )
                return len(df)
            else:
                logger.warning(f"테이블 {table_name}에서 데이터를 추출할 수 없습니다.")
                return 0

        except Exception as e:
            logger.error(f"CSV 내보내기 실패: {table_name} -> {csv_path}, 오류: {e!s}")
            raise

    def create_temp_table_and_import_csv(
        self, target_table: str, source_schema: dict[str, Any], csv_path: str
    ) -> tuple[str, int]:
        """
        임시 테이블 생성과 CSV 가져오기를 하나의 연결에서 실행

        Args:
            target_table: 타겟 테이블명
            source_schema: 소스 테이블 스키마 정보
            csv_path: CSV 파일 경로

        Returns:
            (임시 테이블명, 가져온 행 수) 튜플
        """
        try:
            # 임시 테이블명 생성
            temp_table = (
                f"temp_{target_table.replace('.', '_')}_"
                f"{int(pd.Timestamp.now().timestamp())}"
            )

            # 스키마와 테이블명 분리
            if "." in target_table:
                schema, table = target_table.split(".", 1)
            else:
                schema = "public"
                table = target_table

            # 컬럼 정의 생성
            column_definitions = []
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]  # data_type -> type으로 변경
                is_nullable = col["nullable"]  # nullable 정보 복원

                # PostgreSQL 데이터 타입 매핑
                if col_type.upper() in ["VARCHAR", "CHAR", "TEXT"]:
                    pg_type = "TEXT"
                elif (
                    col_type.upper() in ["INTEGER", "INT", "BIGINT", "SMALLINT"]
                    or col_type.upper()
                    in ["DECIMAL", "NUMERIC", "REAL", "DOUBLE PRECISION"]
                    or col_type.upper() in ["DATE", "TIMESTAMP", "TIME"]
                ):
                    pg_type = "TEXT"  # 안전성을 위해 TEXT로 변환
                else:
                    pg_type = "TEXT"  # 기본값

                # 원본 스키마의 nullable 정보를 유지
                nullable_clause = "NOT NULL" if not is_nullable else ""
                column_definitions.append(
                    f"{col_name} {pg_type} {nullable_clause}".strip()
                )

            # CREATE TABLE 문 생성 (임시 테이블 대신 영구 테이블 사용)
            create_sql = f"""
                CREATE TABLE {temp_table} (
                    {', '.join(column_definitions)}
                )
            """

            logger.info(f"임시 테이블 생성 SQL: {create_sql}")

            # 타겟 데이터베이스 연결
            target_hook = self.target_hook

            # 하나의 연결에서 임시 테이블 생성과 CSV 가져오기 실행
            with target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # 1. 임시 테이블 생성
                    cursor.execute(create_sql)
                    logger.info(f"임시 테이블 생성 완료: {temp_table}")

                    # 2. CSV 파일 읽기 (NA 문자열을 NaN으로 잘못 인식하지 않도록 설정)
                    df = pd.read_csv(
                        csv_path, encoding="utf-8", na_values=[], keep_default_na=False
                    )

                    if df.empty:
                        logger.warning(f"CSV 파일 {csv_path}에 데이터가 없습니다.")
                        return temp_table, 0

                    # 3. 소스 스키마에서 컬럼명 가져오기
                    if source_schema and source_schema.get("columns"):
                        temp_columns = [col["name"] for col in source_schema["columns"]]
                        logger.info(
                            f"소스 스키마에서 컬럼명을 가져왔습니다: {temp_columns}"
                        )

                        # CSV 컬럼을 소스 스키마 순서에 맞춰 재정렬
                        df_reordered = df[temp_columns]
                        logger.info(
                            f"CSV 컬럼을 소스 스키마 순서에 맞춰 재정렬했습니다: {temp_columns}"
                        )
                    else:
                        temp_columns = list(df.columns)
                        logger.info(
                            f"소스 스키마 정보가 없어 CSV 컬럼명을 그대로 사용합니다: {temp_columns}"
                        )
                        df_reordered = df

                    # 4. 데이터 타입 변환 및 null 값 검증
                    # NOT NULL 제약조건이 있는 컬럼들 확인
                    not_null_columns = []
                    for col in source_schema["columns"]:
                        if not col["nullable"]:
                            not_null_columns.append(col["name"])

                    logger.info(f"NOT NULL 제약조건이 있는 컬럼: {not_null_columns}")

                    # null 값이 있는 행 검증 (실제 null 값만, "NA" 문자열은 제외)
                    null_violations = []
                    for col_name in not_null_columns:
                        if col_name in df_reordered.columns:
                            # 실제 null 값만 검사 (None, numpy.nan 등)
                            null_rows = df_reordered[df_reordered[col_name].isna()]
                            if not null_rows.empty:
                                null_violations.append(
                                    {
                                        "column": col_name,
                                        "count": len(null_rows),
                                        "sample_rows": null_rows.head(3).to_dict(
                                            "records"
                                        ),
                                    }
                                )

                    if null_violations:
                        logger.warning(
                            f"NOT NULL 제약조건 위반 발견: {null_violations}"
                        )
                        # null 값을 빈 문자열로 변환하여 임시로 처리
                        for col_name in not_null_columns:
                            if col_name in df_reordered.columns:
                                df_reordered[col_name] = df_reordered[col_name].fillna(
                                    ""
                                )
                                logger.info(
                                    f"컬럼 {col_name}의 null 값을 빈 문자열로 변환했습니다."
                                )

                    # 일반적인 데이터 타입 변환
                    for col in df_reordered.columns:
                        # 실제 null 값만 빈 문자열로 변환 ("NA" 문자열은 보존)
                        df_reordered[col] = df_reordered[col].fillna("")

                        # 숫자 컬럼의 경우 문자열로 변환하여 안전하게 처리
                        if df_reordered[col].dtype in ["int64", "float64"]:
                            df_reordered[col] = df_reordered[col].astype(str)

                    logger.info(f"데이터 타입 변환 완료: {len(df_reordered)}행")

                    # 데이터 샘플 로깅 (디버깅용)
                    logger.info("처리된 데이터 샘플 (처음 3행):")
                    for i, row in df_reordered.head(3).iterrows():
                        logger.info(f"행 {i}: {dict(row)}")

                    # 5. INSERT 실행
                    batch_size = 1000
                    total_inserted = 0

                    for i in range(0, len(df_reordered), batch_size):
                        batch_df = df_reordered.iloc[i : i + batch_size]
                        batch_data = [tuple(row) for row in batch_df.values]

                        insert_query = f"""
                            INSERT INTO {temp_table} ({', '.join(temp_columns)})
                            VALUES ({', '.join(['%s'] * len(temp_columns))})
                        """

                        cursor.executemany(insert_query, batch_data)
                        total_inserted += len(batch_data)

                        if i % 10000 == 0:
                            logger.info(
                                f"INSERT 진행률: {total_inserted}/{len(df_reordered)}"
                            )

                    # 커밋
                    conn.commit()

                    logger.info(
                        f"CSV 가져오기 완료: {csv_path} -> {temp_table}, 행 수: {total_inserted}"
                    )
                    return temp_table, total_inserted

        except Exception as e:
            logger.error(f"임시 테이블 생성 및 CSV 가져오기 실패: {e}")
            raise Exception(f"임시 테이블 생성 및 CSV 가져오기 실패: {e}")

    def create_temp_table(
        self, target_table: str, source_schema: dict[str, Any]
    ) -> str:
        """
        임시 테이블 생성

        Args:
            target_table: 타겟 테이블명
            source_schema: 소스 테이블 스키마 정보

        Returns:
            생성된 임시 테이블명
        """
        try:
            # 임시 테이블명 생성
            temp_table = (
                f"temp_{target_table.replace('.', '_')}_"
                f"{int(pd.Timestamp.now().timestamp())}"
            )

            # 스키마와 테이블명 분리
            if "." in target_table:
                schema, table = target_table.split(".", 1)
            else:
                schema = "public"
                table = target_table

            # 컬럼 정의 생성
            column_definitions = []
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]

                # PostgreSQL 타입 매핑
                if col_type == "character varying":
                    col_type = f"VARCHAR({col['max_length'] or 255})"
                elif col_type == "character":
                    col_type = f"CHAR({col['max_length'] or 1})"
                elif col_type == "numeric":
                    col_type = "NUMERIC"
                elif col_type == "timestamp without time zone":
                    col_type = "TIMESTAMP"
                elif col_type == "timestamp with time zone":
                    col_type = "TIMESTAMPTZ"
                elif col_type == "date":
                    col_type = "DATE"
                elif col_type == "time without time zone":
                    col_type = "TIME"
                elif col_type == "boolean":
                    col_type = "BOOLEAN"
                elif col_type == "integer":
                    col_type = "INTEGER"
                elif col_type == "bigint":
                    col_type = "BIGINT"
                elif col_type == "text":
                    col_type = "TEXT"
                else:
                    col_type = "TEXT"  # 기본값

                nullable = "" if col["nullable"] else " NOT NULL"
                column_definitions.append(f"{col_name} {col_type}{nullable}")

            # 임시 테이블 생성 (스키마를 명시적으로 지정)
            create_temp_table_sql = f"""
                CREATE TEMP TABLE {temp_table} (
                    {', '.join(column_definitions)}
                ) ON COMMIT DROP
            """

            logger.info(f"임시 테이블 생성 SQL: {create_temp_table_sql}")
            self.target_hook.run(create_temp_table_sql)
            logger.info(f"임시 테이블 생성 완료: {temp_table}")

            return temp_table

        except Exception as e:
            logger.error(f"임시 테이블 생성 실패: {target_table}, 오류: {e!s}")
            raise

    def import_from_csv(
        self,
        csv_path: str,
        temp_table: str,
        source_schema: dict[str, Any] | None = None,
    ) -> int:
        """
        CSV를 임시 테이블로 가져오기

        Args:
            csv_path: CSV 파일 경로
            temp_table: 임시 테이블명
            source_schema: 소스 테이블 스키마 정보 (선택사항)

        Returns:
            가져온 행 수
        """
        try:
            # CSV 파일 읽기
            df = pd.read_csv(csv_path, encoding="utf-8")

            if df.empty:
                logger.warning(f"CSV 파일 {csv_path}에 데이터가 없습니다.")
                return 0

            # 임시 테이블의 실제 컬럼명 가져오기
            try:
                if source_schema and source_schema.get("columns"):
                    # 소스 스키마에서 컬럼명 가져오기
                    temp_columns = [col["name"] for col in source_schema["columns"]]
                    logger.info(
                        f"소스 스키마에서 컬럼명을 가져왔습니다: {temp_columns}"
                    )

                    # CSV 컬럼명과 소스 스키마 컬럼명이 일치하는지 확인
                    csv_columns = list(df.columns)
                    if len(csv_columns) != len(temp_columns):
                        logger.warning(
                            f"CSV 컬럼 수({len(csv_columns)})와 소스 스키마 컬럼 수({len(temp_columns)})가 일치하지 않습니다."
                        )
                        # CSV 컬럼명을 그대로 사용
                        temp_columns = csv_columns
                        logger.info(f"CSV 컬럼명을 그대로 사용합니다: {temp_columns}")

                    # 컬럼 순서를 소스 스키마 순서에 맞춰 재정렬
                    df_reordered = df[temp_columns]
                    logger.info(
                        f"CSV 컬럼을 소스 스키마 순서에 맞춰 재정렬했습니다: {temp_columns}"
                    )
                else:
                    # 소스 스키마 정보가 없는 경우 CSV 컬럼명을 그대로 사용
                    temp_columns = list(df.columns)
                    logger.info(
                        f"소스 스키마 정보가 없어 CSV 컬럼명을 그대로 사용합니다: {temp_columns}"
                    )
                    df_reordered = df

            except Exception as e:
                logger.error(f"컬럼 정보 처리 실패: {e}")
                raise

            # 데이터 타입 변환 및 검증
            try:
                # 데이터프레임의 데이터 타입을 안전하게 변환
                for col in df_reordered.columns:
                    # NaN 값을 None으로 변환
                    df_reordered[col] = df_reordered[col].where(
                        pd.notna(df_reordered[col]), None
                    )

                    # 숫자 컬럼의 경우 문자열로 변환하여 안전하게 처리
                    if df_reordered[col].dtype in ["int64", "float64"]:
                        df_reordered[col] = df_reordered[col].astype(str)

                logger.info(f"데이터 타입 변환 완료: {len(df_reordered)}행")

            except Exception as e:
                logger.error(f"데이터 타입 변환 실패: {e}")
                raise Exception(f"데이터 타입 변환 실패: {e}")

            # INSERT 실행
            try:
                # 배치 크기 설정 (메모리 효율성을 위해)
                batch_size = 1000
                total_inserted = 0

                # 타겟 데이터베이스 연결
                target_hook = self.target_hook

                with target_hook.get_conn() as conn:
                    with conn.cursor() as cursor:
                        for i in range(0, len(df_reordered), batch_size):
                            batch_df = df_reordered.iloc[i : i + batch_size]
                            batch_data = [tuple(row) for row in batch_df.values]

                            # INSERT 문 실행
                            insert_query = f"""
                                INSERT INTO {temp_table} ({', '.join(temp_columns)})
                                VALUES ({', '.join(['%s'] * len(temp_columns))})
                            """

                            cursor.executemany(insert_query, batch_data)
                            total_inserted += len(batch_data)

                            if i % 10000 == 0:
                                logger.info(
                                    f"INSERT 진행률: {total_inserted}/{len(df_reordered)}"
                                )

                        # 커밋
                        conn.commit()

                logger.info(
                    f"CSV 가져오기 완료: {csv_path} -> {temp_table}, 행 수: {total_inserted}"
                )
                return total_inserted

            except Exception as e:
                logger.error(f"INSERT 실행 실패: {e}")
                raise Exception(f"INSERT 실행 실패: {e}")

        except Exception as e:
            logger.error(f"CSV 가져오기 실패: {csv_path} -> {temp_table}, 오류: {e!s}")
            raise

    def execute_merge_operation(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str = "full_sync",
    ) -> dict[str, Any]:
        """
        MERGE 작업 실행

        Args:
            source_table: 소스 테이블명 (임시 테이블)
            target_table: 타겟 테이블명
            primary_keys: 기본키 컬럼 리스트
            sync_mode: 동기화 모드 ('full_sync' 또는 'incremental_sync')

        Returns:
            MERGE 작업 결과
        """
        try:
            # 스키마와 테이블명 분리
            if "." in target_table:
                schema, table = target_table.split(".", 1)
            else:
                schema = "public"
                table = target_table

            # 기본키 컬럼들을 쉼표로 연결
            pk_columns = ", ".join(primary_keys)

            # 모든 컬럼 조회 (기본키 제외)
            all_columns_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                AND column_name NOT IN ({', '.join([f"'{pk}'" for pk in primary_keys])})
                ORDER BY ordinal_position
            """

            non_pk_columns = self.target_hook.get_records(all_columns_query)
            non_pk_column_names = [col[0] for col in non_pk_columns]

            # non_pk_column_names가 비어있는 경우 처리
            if not non_pk_column_names:
                logger.warning(
                    "비기본키 컬럼이 없습니다. 기본키만 사용하여 MERGE를 수행합니다."
                )
                # 기본키만 사용하는 경우
                if sync_mode == "full_sync":
                    merge_sql = f"""
                        BEGIN;

                        -- 기존 데이터 삭제
                        DELETE FROM {target_table}
                        WHERE ({pk_columns}) IN (
                            SELECT {pk_columns} FROM {source_table}
                        );

                        -- 새 데이터 삽입 (기본키만)
                        INSERT INTO {target_table} ({pk_columns})
                        SELECT {pk_columns}
                        FROM {source_table};

                        COMMIT;
                    """
                else:
                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns})
                        SELECT {pk_columns}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO NOTHING;
                    """

                # MERGE 실행
                start_time = pd.Timestamp.now()
                self.target_hook.run(merge_sql)
                end_time = pd.Timestamp.now()

                # 결과 확인
                source_count = self.db_ops.get_table_row_count(source_table)
                target_count = self.db_ops.get_table_row_count(target_table)

                merge_result = {
                    "source_count": source_count,
                    "target_count": target_count,
                    "sync_mode": sync_mode,
                    "execution_time": (end_time - start_time).total_seconds(),
                    "status": "success",
                    "message": (
                        f"MERGE 완료 (기본키만): {source_table} -> {target_table}, "
                        f"소스: {source_count}행, 타겟: {target_count}행"
                    ),
                }

                logger.info(merge_result["message"])
                return merge_result

            # MERGE 쿼리 생성
            if sync_mode == "full_sync":
                # 전체 동기화: 기존 데이터 삭제 후 새로 삽입
                if non_pk_column_names:
                    merge_sql = f"""
                        BEGIN;

                        -- 기존 데이터 삭제
                        DELETE FROM {target_table}
                        WHERE ({pk_columns}) IN (
                            SELECT {pk_columns} FROM {source_table}
                        );

                        -- 새 데이터 삽입
                        INSERT INTO {target_table} ({pk_columns}, {', '.join(non_pk_column_names)})
                        SELECT {pk_columns}, {', '.join(non_pk_column_names)}
                        FROM {source_table};

                        COMMIT;
                    """
                else:
                    # 비기본키 컬럼이 없는 경우 기본키만 사용
                    merge_sql = f"""
                        BEGIN;

                        -- 기존 데이터 삭제
                        DELETE FROM {target_table}
                        WHERE ({pk_columns}) IN (
                            SELECT {pk_columns} FROM {source_table}
                        );

                        -- 새 데이터 삽입 (기본키만)
                        INSERT INTO {target_table} ({pk_columns})
                        SELECT {pk_columns}
                        FROM {source_table};

                        COMMIT;
                    """
            else:
                # 증분 동기화: UPSERT (INSERT ... ON CONFLICT)
                if non_pk_column_names:
                    update_set_clause = ", ".join(
                        [f"{col} = EXCLUDED.{col}" for col in non_pk_column_names]
                    )

                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns}, {', '.join(non_pk_column_names)})
                        SELECT {pk_columns}, {', '.join(non_pk_column_names)}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO UPDATE SET {update_set_clause};
                    """
                else:
                    # 비기본키 컬럼이 없는 경우 기본키만 사용
                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns})
                        SELECT {pk_columns}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO NOTHING;
                    """

            # MERGE 실행
            start_time = pd.Timestamp.now()
            self.target_hook.run(merge_sql)
            end_time = pd.Timestamp.now()

            # 결과 확인
            source_count = self.db_ops.get_table_row_count(source_table)
            target_count = self.db_ops.get_table_row_count(target_table)

            merge_result = {
                "source_count": source_count,
                "target_count": target_count,
                "sync_mode": sync_mode,
                "execution_time": (end_time - start_time).total_seconds(),
                "status": "success",
                "message": (
                    f"MERGE 완료: {source_table} -> {target_table}, "
                    f"소스: {source_count}행, 타겟: {target_count}행"
                ),
            }

            logger.info(merge_result["message"])
            return merge_result

        except Exception as e:
            logger.error(
                f"MERGE 작업 실패: {source_table} -> {target_table}, 오류: {e!s}"
            )
            raise

    def cleanup_temp_files(self, csv_path: str):
        """임시 파일 정리"""
        try:
            if os.path.exists(csv_path):
                os.remove(csv_path)
                logger.info(f"임시 파일 삭제 완료: {csv_path}")
        except Exception as e:
            logger.warning(f"임시 파일 삭제 실패: {csv_path}, 오류: {e!s}")

    def _convert_to_postgres_type(
        self, col_type: str, max_length: int | None = None
    ) -> str:
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
                return "SMALLINT"
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

    def _create_temp_table_in_session(
        self, cursor, target_table: str, source_schema: dict
    ) -> str:
        """하나의 세션에서 임시 테이블 생성"""
        try:
            # 스키마와 테이블명 분리
            if "." in target_table:
                schema, table = target_table.split(".", 1)
            else:
                schema = "public"
                table = target_table

            # 임시 테이블명 생성 (타임스탬프 포함)
            timestamp = int(pd.Timestamp.now().timestamp())
            temp_table = f"temp_{schema}_{table}_{timestamp}"

            # 컬럼 정의 생성 - 소스 스키마의 실제 데이터 타입 사용
            column_definitions = []
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]
                is_nullable = col["nullable"]

                # 소스 테이블의 실제 데이터 타입을 그대로 사용
                # PostgreSQL과 호환되는 타입으로 변환
                pg_type = self._convert_to_postgres_type(
                    col_type, col.get("max_length")
                )

                # 원본 스키마의 nullable 정보를 유지
                nullable_clause = "NOT NULL" if not is_nullable else ""
                column_definitions.append(
                    f"{col_name} {pg_type} {nullable_clause}".strip()
                )

            # CREATE TABLE 문 생성 (임시 테이블 대신 영구 테이블 사용)
            create_sql = f"""
                CREATE TABLE {temp_table} (
                    {', '.join(column_definitions)}
                )
            """

            logger.info(f"임시 테이블 생성 SQL: {create_sql}")

            # 임시 테이블 생성
            cursor.execute(create_sql)
            logger.info(f"임시 테이블 생성 완료: {temp_table}")

            return temp_table

        except Exception as e:
            logger.error(f"임시 테이블 생성 실패: {e}")
            raise Exception(f"임시 테이블 생성 실패: {e}")

    def _import_csv_in_session(
        self, cursor, temp_table: str, csv_path: str, source_schema: dict
    ) -> int:
        """하나의 세션에서 CSV 데이터를 임시 테이블에 삽입"""
        try:
            # CSV 파일 읽기 (NA 문자열을 NaN으로 잘못 인식하지 않도록 설정)
            df = pd.read_csv(
                csv_path, encoding="utf-8", na_values=[], keep_default_na=False
            )

            if df.empty:
                logger.warning(f"CSV 파일 {csv_path}에 데이터가 없습니다.")
                return 0

            # 소스 스키마에서 컬럼명 가져오기
            if source_schema and source_schema.get("columns"):
                temp_columns = [col["name"] for col in source_schema["columns"]]
                logger.info(f"소스 스키마에서 컬럼명을 가져왔습니다: {temp_columns}")

                # CSV 컬럼을 소스 스키마 순서에 맞춰 재정렬
                df_reordered = df[temp_columns]
                logger.info(
                    f"CSV 컬럼을 소스 스키마 순서에 맞춰 재정렬했습니다: {temp_columns}"
                )
            else:
                temp_columns = list(df.columns)
                logger.info(
                    f"소스 스키마 정보가 없어 CSV 컬럼명을 그대로 사용합니다: {temp_columns}"
                )
                df_reordered = df

            # 데이터 타입 변환 및 null 값 검증
            # NOT NULL 제약조건이 있는 컬럼들 확인
            not_null_columns = []
            for col in source_schema["columns"]:
                if not col["nullable"]:
                    not_null_columns.append(col["name"])

            logger.info(f"NOT NULL 제약조건이 있는 컬럼: {not_null_columns}")

            # null 값이 있는 행 검증 (실제 null 값만, "NA" 문자열은 제외)
            null_violations = []
            for col_name in not_null_columns:
                if col_name in df_reordered.columns:
                    # 실제 null 값만 검사 (None, numpy.nan 등)
                    null_rows = df_reordered[df_reordered[col_name].isna()]
                    if not null_rows.empty:
                        null_violations.append(
                            {
                                "column": col_name,
                                "count": len(null_rows),
                                "sample_rows": null_rows.head(3).to_dict("records"),
                            }
                        )

            if null_violations:
                logger.warning(f"NOT NULL 제약조건 위반 발견: {null_violations}")
                # null 값을 빈 문자열로 변환하여 임시로 처리
                for col_name in not_null_columns:
                    if col_name in df_reordered.columns:
                        df_reordered[col_name] = df_reordered[col_name].fillna("")
                        logger.info(
                            f"컬럼 {col_name}의 null 값을 빈 문자열로 변환했습니다."
                        )

            # 일반적인 데이터 타입 변환
            for col in df_reordered.columns:
                # 실제 null 값만 빈 문자열로 변환 ("NA" 문자열은 보존)
                df_reordered[col] = df_reordered[col].fillna("")

                # 숫자 컬럼의 경우 문자열로 변환하여 안전하게 처리
                if df_reordered[col].dtype in ["int64", "float64"]:
                    df_reordered[col] = df_reordered[col].astype(str)

            logger.info(f"데이터 타입 변환 완료: {len(df_reordered)}행")

            # 데이터 샘플 로깅 (디버깅용)
            logger.info("처리된 데이터 샘플 (처음 3행):")
            for i, row in df_reordered.head(3).iterrows():
                logger.info(f"행 {i}: {dict(row)}")

            # INSERT 실행
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(df_reordered), batch_size):
                batch_df = df_reordered.iloc[i : i + batch_size]
                batch_data = [tuple(row) for row in batch_df.values]

                insert_query = f"""
                    INSERT INTO {temp_table} ({', '.join(temp_columns)})
                    VALUES ({', '.join(['%s'] * len(temp_columns))})
                """

                cursor.executemany(insert_query, batch_data)
                total_inserted += len(batch_data)

                if i % 10000 == 0:
                    logger.info(f"INSERT 진행률: {total_inserted}/{len(df_reordered)}")

            logger.info(
                f"CSV 가져오기 완료: {csv_path} -> {temp_table}, 행 수: {total_inserted}"
            )
            return total_inserted

        except Exception as e:
            logger.error(f"CSV 가져오기 실패: {e}")
            raise Exception(f"CSV 가져오기 실패: {e}")

    def _execute_merge_in_session(
        self,
        cursor,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str,
    ) -> dict:
        """하나의 세션에서 MERGE 작업 실행"""
        try:
            # 스키마와 테이블명 분리
            if "." in target_table:
                schema, table = target_table.split(".", 1)
            else:
                schema = "public"
                table = target_table

            # 기본키 컬럼들을 쉼표로 연결
            pk_columns = ", ".join(primary_keys)

            # 모든 컬럼 조회 (기본키 제외)
            all_columns_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                AND column_name NOT IN ({', '.join([f"'{pk}'" for pk in primary_keys])})
                ORDER BY ordinal_position
            """

            cursor.execute(all_columns_query)
            non_pk_columns = cursor.fetchall()
            non_pk_column_names = [col[0] for col in non_pk_columns]

            # non_pk_column_names가 비어있는 경우 처리
            if not non_pk_column_names:
                logger.warning(
                    "비기본키 컬럼이 없습니다. 기본키만 사용하여 MERGE를 수행합니다."
                )
                # 기본키만 사용하는 경우
                if sync_mode == "full_sync":
                    merge_sql = f"""
                        -- 기존 데이터 삭제
                        DELETE FROM {target_table}
                        WHERE ({pk_columns}) IN (
                            SELECT {pk_columns} FROM {source_table}
                        );

                        -- 새 데이터 삽입 (기본키만)
                        INSERT INTO {target_table} ({pk_columns})
                        SELECT {pk_columns}
                        FROM {source_table};
                    """
                else:
                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns})
                        SELECT {pk_columns}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO NOTHING;
                    """

                # MERGE 실행
                start_time = pd.Timestamp.now()
                cursor.execute(merge_sql)
                end_time = pd.Timestamp.now()

                # 결과 확인
                cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
                source_count = cursor.fetchone()[0]

                cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
                target_count = cursor.fetchone()[0]

                merge_result = {
                    "source_count": source_count,
                    "target_count": target_count,
                    "sync_mode": sync_mode,
                    "execution_time": (end_time - start_time).total_seconds(),
                    "status": "success",
                    "message": (
                        f"MERGE 완료 (기본키만): {source_table} -> {target_table}, "
                        f"소스: {source_count}행, 타겟: {target_count}행"
                    ),
                }

                logger.info(merge_result["message"])
                return merge_result

            # MERGE 쿼리 생성
            if sync_mode == "full_sync":
                # 전체 동기화: 기존 데이터 삭제 후 새로 삽입
                if non_pk_column_names:
                    merge_sql = f"""
                        -- 기존 데이터 삭제
                        DELETE FROM {target_table}
                        WHERE ({pk_columns}) IN (
                            SELECT {pk_columns} FROM {source_table}
                        );

                        -- 새 데이터 삽입
                        INSERT INTO {target_table} ({pk_columns}, {', '.join(non_pk_column_names)})
                        SELECT {pk_columns}, {', '.join(non_pk_column_names)}
                        FROM {source_table};
                    """
                else:
                    # 비기본키 컬럼이 없는 경우 기본키만 사용
                    merge_sql = f"""
                        -- 기존 데이터 삭제
                        DELETE FROM {target_table}
                        WHERE ({pk_columns}) IN (
                            SELECT {pk_columns} FROM {source_table}
                        );

                        -- 새 데이터 삽입 (기본키만)
                        INSERT INTO {target_table} ({pk_columns})
                        SELECT {pk_columns}
                        FROM {source_table};
                    """
            else:
                # 증분 동기화: UPSERT (INSERT ... ON CONFLICT)
                if non_pk_column_names:
                    update_set_clause = ", ".join(
                        [f"{col} = EXCLUDED.{col}" for col in non_pk_column_names]
                    )

                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns}, {', '.join(non_pk_column_names)})
                        SELECT {pk_columns}, {', '.join(non_pk_column_names)}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO UPDATE SET {update_set_clause};
                    """
                else:
                    # 비기본키 컬럼이 없는 경우 기본키만 사용
                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns})
                        SELECT {pk_columns}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO NOTHING;
                    """

            # MERGE 실행
            start_time = pd.Timestamp.now()
            cursor.execute(merge_sql)
            end_time = pd.Timestamp.now()

            # 결과 확인
            cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
            source_count = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
            target_count = cursor.fetchone()[0]

            merge_result = {
                "source_count": source_count,
                "target_count": target_count,
                "sync_mode": sync_mode,
                "execution_time": (end_time - start_time).total_seconds(),
                "status": "success",
                "message": (
                    f"MERGE 완료: {source_table} -> {target_table}, "
                    f"소스: {source_count}행, 타겟: {target_count}행"
                ),
            }

            logger.info(merge_result["message"])
            return merge_result

        except Exception as e:
            logger.error(
                f"MERGE 작업 실패: {source_table} -> {target_table}, 오류: {e!s}"
            )
            raise

    def copy_table_data(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str = "full_sync",
        where_clause: str | None = None,
        batch_size: int = 10000,
    ) -> dict[str, Any]:
        """
        테이블 데이터 복사 전체 프로세스

        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            primary_keys: 기본키 컬럼 리스트
            sync_mode: 동기화 모드
            where_clause: WHERE 절 조건
            batch_size: 배치 크기

        Returns:
            복사 작업 결과
        """
        csv_path = None
        temp_table = None

        try:
            start_time = pd.Timestamp.now()

            # 1. 소스 테이블 스키마 조회
            logger.info(f"소스 테이블 스키마 조회 시작: {source_table}")
            source_schema = self.db_ops.get_table_schema(source_table)

            if not source_schema or not source_schema.get("columns"):
                raise Exception(
                    f"소스 테이블 {source_table}의 스키마 정보를 가져올 수 없습니다."
                )

            # 2. CSV로 내보내기 (소스 DB에서 - 다른 세션)
            csv_path = os.path.join(
                self.temp_dir, f"temp_{os.path.basename(source_table)}.csv"
            )
            logger.info(f"CSV 내보내기 시작: {source_table} -> {csv_path}")
            exported_rows = self.export_to_csv(
                source_table, csv_path, where_clause, batch_size
            )

            if exported_rows == 0:
                return {
                    "status": "warning",
                    "message": f"소스 테이블 {source_table}에 데이터가 없습니다.",
                    "execution_time": 0,
                }

            # 3. 하나의 세션에서 임시 테이블 생성, 데이터 삽입, MERGE 작업 수행 (세션 격리 문제 해결)
            logger.info(
                f"타겟 DB에서 임시 테이블 생성, 데이터 삽입, MERGE 작업 시작: {target_table}"
            )

            # 타겟 DB에서 하나의 연결로 모든 작업 수행
            with self.target_hook.get_conn() as target_conn:
                with target_conn.cursor() as cursor:
                    # 3-1. 임시 테이블 생성 (소스 스키마 기반)
                    temp_table = self._create_temp_table_in_session(
                        cursor, target_table, source_schema
                    )
                    logger.info(f"임시 테이블 생성 완료: {temp_table}")

                    # 3-2. CSV 데이터 삽입
                    imported_rows = self._import_csv_in_session(
                        cursor, temp_table, csv_path, source_schema
                    )
                    logger.info(
                        f"CSV 데이터 삽입 완료: {temp_table}, 행 수: {imported_rows}"
                    )

                    # 3-3. MERGE 작업 실행 (같은 연결에서)
                    merge_result = self._execute_merge_in_session(
                        cursor, temp_table, target_table, primary_keys, sync_mode
                    )
                    logger.info(f"MERGE 작업 완료: {temp_table} -> {target_table}")

                # 모든 작업이 성공하면 커밋
                target_conn.commit()
                logger.info("모든 작업이 성공적으로 커밋되었습니다.")

            end_time = pd.Timestamp.now()
            total_time = (end_time - start_time).total_seconds()

            # 5. 결과 정리
            final_result = {
                "status": "success",
                "source_table": source_table,
                "target_table": target_table,
                "exported_rows": exported_rows,
                "imported_rows": imported_rows,
                "merge_result": merge_result,
                "total_execution_time": total_time,
                "message": (
                    f"테이블 복사 완료: {source_table} -> {target_table}, "
                    f"총 소요시간: {total_time:.2f}초"
                ),
            }

            logger.info(final_result["message"])
            return final_result

        except Exception as e:
            error_result = {
                "status": "error",
                "source_table": source_table,
                "target_table": target_table,
                "error": str(e),
                "message": (
                    f"테이블 복사 실패: {source_table} -> {target_table}, "
                    f"오류: {e!s}"
                ),
            }

            logger.error(error_result["message"])
            return error_result

        finally:
            # 6. 정리 작업
            if csv_path:
                self.cleanup_temp_files(csv_path)

            if temp_table:
                try:
                    # 임시 테이블은 자동으로 삭제되지만, 명시적으로 삭제
                    self.target_hook.run(f"DROP TABLE IF EXISTS {temp_table}")
                    logger.info(f"임시 테이블 정리 완료: {temp_table}")
                except Exception as e:
                    logger.warning(f"임시 테이블 정리 실패: {temp_table}, 오류: {e!s}")
