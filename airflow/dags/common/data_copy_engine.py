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
                # 컬럼명을 안전하게 가져오기
                try:
                    cursor = self.source_hook.get_cursor()
                    if cursor and cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                    else:
                        # description이 None인 경우 기본 컬럼명 생성
                        columns = [f"column_{i}" for i in range(len(all_data[0]))]
                        logger.warning(
                            f"컬럼 정보를 가져올 수 없어 기본 컬럼명을 사용합니다: {columns}"
                        )
                except Exception:
                    # 컬럼 정보 가져오기 실패 시 기본 컬럼명 생성
                    columns = [f"column_{i}" for i in range(len(all_data[0]))]
                    logger.warning(
                        f"컬럼 정보 가져오기 실패, 기본 컬럼명을 사용합니다: {columns}"
                    )

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
                # public 스키마 사용
                pass

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

            # 임시 테이블 생성
            create_temp_table_sql = f"""
                CREATE TEMP TABLE {temp_table} (
                    {', '.join(column_definitions)}
                ) ON COMMIT DROP
            """

            self.target_hook.run(create_temp_table_sql)
            logger.info(f"임시 테이블 생성 완료: {temp_table}")

            return temp_table

        except Exception as e:
            logger.error(f"임시 테이블 생성 실패: {target_table}, 오류: {e!s}")
            raise

    def import_from_csv(self, csv_path: str, temp_table: str) -> int:
        """
        CSV를 임시 테이블로 가져오기

        Args:
            csv_path: CSV 파일 경로
            temp_table: 임시 테이블명

        Returns:
            가져온 행 수
        """
        try:
            # CSV 파일 읽기
            df = pd.read_csv(csv_path, encoding="utf-8")

            if df.empty:
                logger.warning(f"CSV 파일 {csv_path}에 데이터가 없습니다.")
                return 0

            # 임시 테이블에 데이터 삽입
            rows_imported = 0
            batch_size = 1000

            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]

                # DataFrame을 리스트로 변환
                batch_data = [tuple(row) for row in batch_df.values]

                # 컬럼명
                columns = list(df.columns)
                placeholders = ", ".join(["%s"] * len(columns))

                # INSERT 쿼리 실행
                insert_sql = (
                    f"INSERT INTO {temp_table} ({', '.join(columns)}) "
                    f"VALUES ({placeholders})"
                )
                self.target_hook.run(insert_sql, parameters=batch_data)

                rows_imported += len(batch_data)
                logger.info(f"배치 가져오기 진행률: {rows_imported}/{len(df)}")

            logger.info(
                f"CSV 가져오기 완료: {csv_path} -> {temp_table}, 행 수: {rows_imported}"
            )
            return rows_imported

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
                logger.warning(f"비기본키 컬럼이 없습니다. 기본키만 사용하여 MERGE를 수행합니다.")
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
                # 증분 동기화: UPSERT (INSERT ... ON CONFLICT)
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

            # 2. CSV로 내보내기
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

            # 3. 임시 테이블 생성
            logger.info(f"임시 테이블 생성 시작: {target_table}")
            temp_table = self.create_temp_table(target_table, source_schema)

            # 4. CSV를 임시 테이블로 가져오기
            logger.info(f"CSV 가져오기 시작: {csv_path} -> {temp_table}")
            imported_rows = self.import_from_csv(csv_path, temp_table)

            # 5. MERGE 작업 실행
            logger.info(f"MERGE 작업 시작: {temp_table} -> {target_table}")
            merge_result = self.execute_merge_operation(
                temp_table, target_table, primary_keys, sync_mode
            )

            end_time = pd.Timestamp.now()
            total_time = (end_time - start_time).total_seconds()

            # 6. 결과 정리
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
            # 7. 정리 작업
            if csv_path:
                self.cleanup_temp_files(csv_path)

            if temp_table:
                try:
                    # 임시 테이블은 자동으로 삭제되지만, 명시적으로 삭제
                    self.target_hook.run(f"DROP TABLE IF EXISTS {temp_table}")
                    logger.info(f"임시 테이블 정리 완료: {temp_table}")
                except Exception as e:
                    logger.warning(f"임시 테이블 정리 실패: {temp_table}, 오류: {e!s}")
