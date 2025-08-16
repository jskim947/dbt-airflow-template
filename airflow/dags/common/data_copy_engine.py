"""
Data Copy Engine Module
데이터 복사 핵심 로직, CSV 내보내기/가져오기, 임시 테이블 관리, MERGE 작업 등을 담당
"""

import logging
import os
import time
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
        order_by_field: str | None = None,
    ) -> int:
        """
        테이블을 CSV로 내보내기

        Args:
            table_name: 테이블명
            csv_path: CSV 파일 경로
            where_clause: WHERE 절 조건
            batch_size: 배치 크기
            order_by_field: 정렬 기준 필드

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

            # ORDER BY 절 구성
            if order_by_field:
                order_by_clause = f"ORDER BY {order_by_field}"
                logger.info(f"정렬 기준: {order_by_field}")
            else:
                order_by_clause = "ORDER BY 1"
                logger.info("기본 정렬 사용")

            # 배치 단위로 데이터 추출
            offset = 0
            all_data = []

            while offset < total_count:
                if where_clause:
                    query = f"""
                        SELECT * FROM {table_name}
                        WHERE {where_clause}
                        {order_by_clause}
                        LIMIT {batch_size} OFFSET {offset}
                    """
                else:
                    query = f"""
                        SELECT * FROM {table_name}
                        {order_by_clause}
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

                # 🚨 데이터 타입 검증 및 변환 - CSV 저장 전 처리
                logger.info("=== 데이터 타입 검증 및 변환 시작 ===")
                df = self._validate_and_convert_data_types(df, table_name)
                logger.info("=== 데이터 타입 검증 및 변환 완료 ===")

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

    def _validate_and_convert_data_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        데이터프레임의 모든 컬럼에 대해 데이터 타입 검증 및 변환 수행

        Args:
            df: 변환할 데이터프레임
            table_name: 테이블명 (스키마 정보 조회용)

        Returns:
            변환된 데이터프레임
        """
        try:
            logger.info(f"테이블 {table_name}의 데이터 타입 검증 및 변환 시작")

            # 테이블 스키마 정보 조회
            source_schema = self.db_ops.get_table_schema(table_name)
            if not source_schema or not source_schema.get("columns"):
                logger.warning(f"테이블 {table_name}의 스키마 정보를 가져올 수 없어 기본 변환만 수행합니다.")
                return self._basic_data_type_conversion(df)

            # 컬럼별 타입 정보 매핑
            column_types = {}
            for col in source_schema["columns"]:
                column_types[col["name"]] = col["type"]

            logger.info(f"컬럼 타입 정보: {column_types}")

            # 각 컬럼별로 데이터 타입 검증 및 변환
            for col_name in df.columns:
                if col_name in column_types:
                    col_type = column_types[col_name]
                    logger.info(f"컬럼 {col_name} ({col_type}) 검증 및 변환 시작")

                    # 컬럼 타입별 변환 함수 호출
                    df[col_name] = self._convert_column_by_type(df[col_name], col_type, col_name)

                    # 변환 후 검증
                    self._validate_column_after_conversion(df[col_name], col_type, col_name)
                else:
                    logger.warning(f"컬럼 {col_name}의 타입 정보를 찾을 수 없습니다.")

            logger.info(f"테이블 {table_name}의 모든 컬럼 변환 완료")
            return df

        except Exception as e:
            logger.error(f"데이터 타입 검증 및 변환 중 오류 발생: {e}")
            # 오류가 발생해도 기본 변환은 수행
            logger.info("기본 데이터 타입 변환을 수행합니다.")
            return self._basic_data_type_conversion(df)

    def _convert_column_by_type(self, column_series: pd.Series, col_type: str, col_name: str) -> pd.Series:
        """
        컬럼 타입에 따라 데이터 변환 수행

        Args:
            column_series: 변환할 컬럼 시리즈
            col_type: 컬럼 타입
            col_type: 컬럼명

        Returns:
            변환된 컬럼 시리즈
        """
        try:
            # 🚨 PRIORITY 컬럼 특별 처리
            if col_name == "priority" and col_type in ["BIGINT", "INTEGER", "SMALLINT"]:
                logger.info(f"🚨 PRIORITY 컬럼 특별 처리 시작: {col_type}")
                return self._convert_priority_column(column_series)

            # 정수 타입 컬럼 처리
            if col_type in ["BIGINT", "INTEGER", "SMALLINT"]:
                logger.info(f"정수 타입 컬럼 {col_name} 변환: {col_type}")
                return self._convert_integer_column(column_series, col_name)

            # 실수 타입 컬럼 처리
            elif col_type in ["DOUBLE PRECISION", "REAL", "NUMERIC", "DECIMAL"]:
                logger.info(f"실수 타입 컬럼 {col_name} 변환: {col_type}")
                return self._convert_float_column(column_series, col_name)

            # 날짜/시간 타입 컬럼 처리
            elif col_type in ["DATE", "TIMESTAMP", "TIME"]:
                logger.info(f"날짜/시간 타입 컬럼 {col_name} 변환: {col_type}")
                return self._convert_datetime_column(column_series, col_name)

            # 불린 타입 컬럼 처리
            elif col_type in ["BOOLEAN"]:
                logger.info(f"불린 타입 컬럼 {col_name} 변환: {col_type}")
                return self._convert_boolean_column(column_series, col_name)

            # 문자열 타입 컬럼 처리
            else:
                logger.info(f"문자열 타입 컬럼 {col_name} 변환: {col_type}")
                return self._convert_string_column(column_series, col_name)

        except Exception as e:
            logger.error(f"컬럼 {col_name} 변환 중 오류 발생: {e}")
            # 오류 발생 시 원본 반환
            return column_series

    def _convert_priority_column(self, column_series: pd.Series) -> pd.Series:
        """
        PRIORITY 컬럼 특별 처리 - 소수점 값을 정수로 변환

        Args:
            column_series: 변환할 컬럼 시리즈

        Returns:
            변환된 컬럼 시리즈
        """
        try:
            logger.info("🚨 PRIORITY 컬럼 변환 시작")

            # 현재 값 확인
            sample_values = column_series.head(10).tolist()
            logger.info(f"PRIORITY 컬럼 현재 값 샘플: {sample_values}")

            # 소수점 값이 있는지 확인
            decimal_count = column_series.astype(str).str.contains(r'\.', na=False).sum()
            logger.info(f"PRIORITY 컬럼 소수점 값 개수: {decimal_count}")

            if decimal_count > 0:
                logger.info("🚨 PRIORITY 컬럼에 소수점 값이 발견되었습니다. 변환을 시작합니다.")

                # 소수점 값을 정수로 변환
                def convert_priority_value(x):
                    if pd.isna(x) or x is None:
                        return None
                    if isinstance(x, str):
                        x = x.strip()
                        if not x or x in ["", "nan", "None", "NULL", "null"]:
                            return None
                        if "." in x:
                            try:
                                float_val = float(x)
                                int_val = int(float_val)
                                logger.debug(f"PRIORITY 값 변환: '{x}' → '{int_val}'")
                                return str(int_val)
                            except (ValueError, TypeError):
                                logger.warning(f"PRIORITY 값 '{x}'을 정수로 변환할 수 없습니다.")
                                return x
                    elif isinstance(x, (int, float)):
                        return str(int(x))
                    return str(x) if x is not None else None

                # 변환 전후 비교
                before_values = column_series.copy()
                column_series = column_series.apply(convert_priority_value)
                after_values = column_series.copy()

                # 변환된 값 확인
                changed_mask = before_values != after_values
                changed_count = changed_mask.sum()
                if changed_count > 0:
                    logger.info(f"🚨 PRIORITY 컬럼 {changed_count}개 값이 변환되었습니다.")
                    # 변환된 값들 로깅
                    changed_indices = changed_mask[changed_mask].index
                    for idx in changed_indices[:5]:  # 처음 5개만
                        logger.info(f"PRIORITY 변환: 행 {idx}: '{before_values[idx]}' → '{after_values[idx]}'")

                # 최종 검증
                final_decimal_count = column_series.astype(str).str.contains(r'\.', na=False).sum()
                if final_decimal_count == 0:
                    logger.info("✅ PRIORITY 컬럼의 모든 소수점 값이 성공적으로 변환되었습니다.")
                else:
                    logger.warning(f"⚠️ PRIORITY 컬럼에 여전히 {final_decimal_count}개의 소수점 값이 남아있습니다.")
            else:
                logger.info("✅ PRIORITY 컬럼에 소수점 값이 없습니다.")

            return column_series

        except Exception as e:
            logger.error(f"PRIORITY 컬럼 변환 중 오류 발생: {e}")
            return column_series

    def _convert_integer_column(self, column_series: pd.Series, col_name: str) -> pd.Series:
        """
        정수 타입 컬럼 변환 - 소수점 값, 빈 문자열, NaN 처리

        Args:
            column_series: 변환할 컬럼 시리즈
            col_name: 컬럼명

        Returns:
            변환된 컬럼 시리즈
        """
        try:
            logger.info(f"정수 타입 컬럼 {col_name} 변환 시작")

            # 빈 문자열, 공백, 'nan' 문자열을 None(NULL)로 변환
            column_series = column_series.replace({
                "": None,
                " ": None,
                "nan": None,
                "None": None,
                "NULL": None,
                "null": None
            })

            # 소수점 값을 정수로 변환
            def convert_decimal_to_int(x):
                if pd.isna(x) or x is None:
                    return None
                if isinstance(x, str):
                    x = x.strip()
                    if not x or x in ["", "nan", "None", "NULL", "null"]:
                        return None
                    if "." in x:
                        try:
                            float_val = float(x)
                            int_val = int(float_val)
                            return str(int_val)
                        except (ValueError, TypeError):
                            logger.warning(f"컬럼 {col_name}의 값 '{x}'을 정수로 변환할 수 없습니다.")
                            return x
                elif isinstance(x, (int, float)):
                    return str(int(x))
                return str(x) if x is not None else None

            # 변환 전후 값 확인
            before_values = column_series.copy()
            column_series = column_series.apply(convert_decimal_to_int)
            after_values = column_series.copy()

            # 변환된 값이 있는지 확인
            changed_count = sum(1 for b, a in zip(before_values, after_values) if b != a)
            if changed_count > 0:
                logger.info(f"정수 타입 컬럼 {col_name}의 {changed_count}개 값이 변환되었습니다.")

            # 추가로 pandas의 NaN 값도 None으로 변환
            column_series = column_series.replace({pd.NA: None, pd.NaT: None})
            logger.info(f"정수 타입 컬럼 {col_name} 변환 완료")

            return column_series

        except Exception as e:
            logger.error(f"정수 타입 컬럼 {col_name} 변환 중 오류 발생: {e}")
            return column_series

    def _convert_float_column(self, column_series: pd.Series, col_name: str) -> pd.Series:
        """
        실수 타입 컬럼 변환 - 빈 문자열, NaN 처리

        Args:
            column_series: 변환할 컬럼 시리즈
            col_name: 컬럼명

        Returns:
            변환된 컬럼 시리즈
        """
        try:
            logger.info(f"실수 타입 컬럼 {col_name} 변환 시작")

            # 빈 문자열, 공백, 'nan' 문자열을 None(NULL)로 변환
            column_series = column_series.replace({
                "": None,
                " ": None,
                "nan": None,
                "None": None,
                "NULL": None,
                "null": None
            })

            # 추가로 pandas의 NaN 값도 None으로 변환
            column_series = column_series.replace({pd.NA: None, pd.NaT: None})
            logger.info(f"실수 타입 컬럼 {col_name} 변환 완료")

            return column_series

        except Exception as e:
            logger.error(f"실수 타입 컬럼 {col_name} 변환 중 오류 발생: {e}")
            return column_series

    def _convert_datetime_column(self, column_series: pd.Series, col_name: str) -> pd.Series:
        """
        날짜/시간 타입 컬럼 변환 - 빈 문자열, 잘못된 형식 처리

        Args:
            column_series: 변환할 컬럼 시리즈
            col_name: 컬럼명

        Returns:
            변환된 컬럼 시리즈
        """
        try:
            logger.info(f"날짜/시간 타입 컬럼 {col_name} 변환 시작")

            # 빈 문자열, 공백, 'nan' 문자열을 None(NULL)로 변환
            column_series = column_series.replace({
                "": None,
                " ": None,
                "nan": None,
                "None": None,
                "NULL": None,
                "null": None
            })

            # 추가로 pandas의 NaN 값도 None으로 변환
            column_series = column_series.replace({pd.NA: None, pd.NaT: None})
            logger.info(f"날짜/시간 타입 컬럼 {col_name} 변환 완료")

            return column_series

        except Exception as e:
            logger.error(f"날짜/시간 타입 컬럼 {col_name} 변환 중 오류 발생: {e}")
            return column_series

    def _convert_boolean_column(self, column_series: pd.Series, col_name: str) -> pd.Series:
        """
        불린 타입 컬럼 변환 - 문자열 값을 불린으로 변환

        Args:
            column_series: 변환할 컬럼 시리즈
            col_name: 컬럼명

        Returns:
            변환된 컬럼 시리즈
        """
        try:
            logger.info(f"불린 타입 컬럼 {col_name} 변환 시작")

            # 빈 문자열, 공백, 'nan' 문자열을 None(NULL)로 변환
            column_series = column_series.replace({
                "": None,
                " ": None,
                "nan": None,
                "None": None,
                "NULL": None,
                "null": None
            })

            # 문자열 값을 불린으로 변환
            def convert_to_boolean(x):
                if pd.isna(x) or x is None:
                    return None
                if isinstance(x, str):
                    x = x.strip().lower()
                    if x in ["true", "1", "yes", "on"]:
                        return "true"
                    elif x in ["false", "0", "no", "off"]:
                        return "false"
                    else:
                        return x
                elif isinstance(x, bool):
                    return str(x).lower()
                elif isinstance(x, (int, float)):
                    return "true" if x else "false"
                return str(x) if x is not None else None

            column_series = column_series.apply(convert_to_boolean)

            # 추가로 pandas의 NaN 값도 None으로 변환
            column_series = column_series.replace({pd.NA: None, pd.NaT: None})
            logger.info(f"불린 타입 컬럼 {col_name} 변환 완료")

            return column_series

        except Exception as e:
            logger.error(f"불린 타입 컬럼 {col_name} 변환 중 오류 발생: {e}")
            return column_series

    def _convert_string_column(self, column_series: pd.Series, col_name: str) -> pd.Series:
        """
        문자열 타입 컬럼 변환 - 빈 문자열, NaN 처리

        Args:
            column_series: 변환할 컬럼 시리즈
            col_name: 컬럼명

        Returns:
            변환된 컬럼 시리즈
        """
        try:
            logger.info(f"문자열 타입 컬럼 {col_name} 변환 시작")

            # 빈 문자열, 공백, 'nan' 문자열을 None(NULL)로 변환
            column_series = column_series.replace({
                "": None,
                " ": None,
                "nan": None,
                "None": None,
                "NULL": None,
                "null": None
            })

            # 추가로 pandas의 NaN 값도 None으로 변환
            column_series = column_series.replace({pd.NA: None, pd.NaT: None})
            logger.info(f"문자열 타입 컬럼 {col_name} 변환 완료")

            return column_series

        except Exception as e:
            logger.error(f"문자열 타입 컬럼 {col_name} 변환 중 오류 발생: {e}")
            return column_series

    def _validate_column_after_conversion(self, column_series: pd.Series, col_type: str, col_name: str) -> None:
        """
        변환 후 컬럼 검증

        Args:
            column_series: 검증할 컬럼 시리즈
            col_type: 컬럼 타입
            col_name: 컬럼명
        """
        try:
            # 정수 타입 컬럼의 경우 소수점 값이 남아있는지 확인
            if col_type in ["BIGINT", "INTEGER", "SMALLINT"]:
                decimal_count = column_series.astype(str).str.contains(r'\.', na=False).sum()
                if decimal_count > 0:
                    logger.warning(f"컬럼 {col_name}에 여전히 {decimal_count}개의 소수점 값이 남아있습니다!")
                    # 문제가 있는 값들을 로깅
                    problem_values = column_series[column_series.astype(str).str.contains(r'\.', na=False)]
                    logger.warning(f"문제가 있는 값들: {problem_values.head(5).tolist()}")
                else:
                    logger.info(f"컬럼 {col_name} 검증 완료: 소수점 값 없음")

            # 모든 타입 컬럼의 경우 빈 문자열이 남아있는지 확인
            empty_count = (column_series.astype(str).str.strip() == "").sum()
            if empty_count > 0:
                logger.warning(f"컬럼 {col_name}에 여전히 {empty_count}개의 빈 문자열이 남아있습니다!")
            else:
                logger.info(f"컬럼 {col_name} 검증 완료: 빈 문자열 없음")

        except Exception as e:
            logger.error(f"컬럼 {col_name} 검증 중 오류 발생: {e}")

    def _basic_data_type_conversion(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        기본 데이터 타입 변환 (스키마 정보가 없을 때 사용)

        Args:
            df: 변환할 데이터프레임

        Returns:
            변환된 데이터프레임
        """
        try:
            logger.info("기본 데이터 타입 변환 시작")

            for col_name in df.columns:
                # 모든 컬럼에서 공통적으로 처리할 변환
                df[col_name] = df[col_name].replace({
                    "": None,
                    " ": None,
                    "nan": None,
                    "None": None,
                    "NULL": None,
                    "null": None
                })

                # pandas의 NaN 값도 None으로 변환
                df[col_name] = df[col_name].replace({pd.NA: None, pd.NaT: None})

            logger.info("기본 데이터 타입 변환 완료")
            return df

        except Exception as e:
            logger.error(f"기본 데이터 타입 변환 중 오류 발생: {e}")
            return df

    def _validate_and_convert_data_types_after_csv_read(self, df: pd.DataFrame, source_schema: dict) -> pd.DataFrame:
        """
        CSV 읽기 후 데이터프레임의 데이터 타입 검증 및 변환 수행

        Args:
            df: 변환할 데이터프레임
            source_schema: 소스 스키마 정보

        Returns:
            변환된 데이터프레임
        """
        try:
            logger.info("=== CSV 읽기 후 데이터 타입 검증 및 변환 시작 ===")

            # 컬럼별 타입 정보 매핑
            column_types = {}
            for col in source_schema["columns"]:
                column_types[col["name"]] = col["type"]

            logger.info(f"컬럼 타입 정보: {column_types}")

            # 🚨 PRIORITY 컬럼 특별 처리
            logger.info("=== 🚨 PRIORITY 컬럼 특별 처리 시작 ===")
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]

                if col_name == "priority" and col_type in ["BIGINT", "INTEGER"]:
                    logger.info(f"🚨 PRIORITY 컬럼 발견! 타입: {col_type}")
                    if col_name in df.columns:
                        # 현재 값 확인
                        sample_values = df[col_name].head(10).tolist()
                        logger.info(f"PRIORITY 컬럼 현재 값 샘플: {sample_values}")

                        # 소수점 값이 있는지 확인
                        decimal_count = df[col_name].astype(str).str.contains(r'\.', na=False).sum()
                        logger.info(f"PRIORITY 컬럼 소수점 값 개수: {decimal_count}")

                        if decimal_count > 0:
                            logger.info("🚨 PRIORITY 컬럼에 소수점 값이 발견되었습니다. 변환을 시작합니다.")

                            # 소수점 값을 정수로 변환
                            def convert_priority_value(x):
                                if pd.isna(x) or x is None:
                                    return None
                                if isinstance(x, str):
                                    x = x.strip()
                                    if not x or x in ["", "nan", "None", "NULL", "null"]:
                                        return None
                                    if "." in x:
                                        try:
                                            float_val = float(x)
                                            int_val = int(float_val)
                                            logger.debug(f"PRIORITY 값 변환: '{x}' → '{int_val}'")
                                            return str(int_val)
                                        except (ValueError, TypeError):
                                            logger.warning(f"PRIORITY 값 '{x}'을 정수로 변환할 수 없습니다.")
                                            return x
                                elif isinstance(x, (int, float)):
                                    return str(int(x))
                                return str(x) if x is not None else None

                            # 변환 전후 비교
                            before_values = df[col_name].copy()
                            df[col_name] = df[col_name].apply(convert_priority_value)
                            after_values = df[col_name].copy()

                            # 변환된 값 확인
                            changed_mask = before_values != after_values
                            changed_count = changed_mask.sum()
                            if changed_count > 0:
                                logger.info(f"🚨 PRIORITY 컬럼 {changed_count}개 값이 변환되었습니다.")
                                # 변환된 값들 로깅
                                changed_indices = changed_mask[changed_mask].index
                                for idx in changed_indices[:5]:  # 처음 5개만
                                    logger.info(f"PRIORITY 변환: 행 {idx}: '{before_values[idx]}' → '{after_values[idx]}'")

                            # 최종 검증
                            final_decimal_count = df[col_name].astype(str).str.contains(r'\.', na=False).sum()
                            if final_decimal_count == 0:
                                logger.info("✅ PRIORITY 컬럼의 모든 소수점 값이 성공적으로 변환되었습니다.")
                            else:
                                logger.warning(f"⚠️ PRIORITY 컬럼에 여전히 {final_decimal_count}개의 소수점 값이 남아있습니다.")
                        else:
                            logger.info("✅ PRIORITY 컬럼에 소수점 값이 없습니다.")
                    else:
                        logger.warning(f"🚨 PRIORITY 컬럼이 데이터프레임에 존재하지 않습니다.")

            # 각 컬럼별로 데이터 타입 검증 및 변환
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]

                if col_name in df.columns:
                    logger.info(f"컬럼 {col_name} ({col_type}) 검증 및 변환 시작")

                    # 컬럼 타입별 변환 함수 호출
                    df[col_name] = self._convert_column_by_type(df[col_name], col_type, col_name)

                    # 변환 후 검증
                    self._validate_column_after_conversion(df[col_name], col_type, col_name)
                else:
                    logger.warning(f"컬럼 {col_name}의 타입 정보를 찾을 수 없습니다.")

            logger.info("=== CSV 읽기 후 데이터 타입 검증 및 변환 완료 ===")
            return df

        except Exception as e:
            logger.error(f"CSV 읽기 후 데이터 타입 검증 및 변환 중 오류 발생: {e}")
            # 오류가 발생해도 기본 변환은 수행
            logger.info("기본 데이터 타입 변환을 수행합니다.")
            return self._basic_data_type_conversion(df)

    def create_temp_table_and_import_csv(
        self, target_table: str, source_schema: dict[str, Any], csv_path: str, batch_size: int = 1000
    ) -> tuple[str, int]:
        """
        임시 테이블 생성과 CSV 가져오기를 하나의 연결에서 실행

        Args:
            target_table: 타겟 테이블명
            source_schema: 소스 테이블 스키마 정보
            csv_path: CSV 파일 경로
            batch_size: 배치 크기

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

                # PostgreSQL 타입 매핑 - _convert_to_postgres_type 메서드 사용
                col_type = self._convert_to_postgres_type(col_type, col.get('max_length'))

                nullable = "" if col["nullable"] else " NOT NULL"
                column_definitions.append(f"{col_name} {col_type}{nullable}")

            # 임시 테이블 생성 (스키마를 명시적으로 지정)
            create_temp_table_sql = f"""
                CREATE TEMP TABLE {temp_table} (
                    {', '.join(column_definitions)}
                ) ON COMMIT DROP
            """

            logger.info(f"임시 테이블 생성 SQL: {create_temp_table_sql}")

            # 같은 연결에서 임시 테이블 생성 및 데이터 가져오기
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_temp_table_sql)
                    logger.info(f"임시 테이블 생성 완료: {temp_table}")

                    # 임시 테이블이 실제로 생성되었는지 확인
                    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{temp_table}')")
                    exists = cursor.fetchone()[0]
                    if not exists:
                        raise Exception(f"임시 테이블 {temp_table}이 생성되지 않았습니다.")

                    return temp_table

        except Exception as e:
            logger.error(f"임시 테이블 생성 실패: {target_table}, 오류: {e!s}")
            raise

    def import_from_csv(
        self,
        csv_path: str,
        temp_table: str,
        source_schema: dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> int:
        """
        CSV를 임시 테이블로 가져오기

        Args:
            csv_path: CSV 파일 경로
            temp_table: 임시 테이블명
            source_schema: 소스 테이블 스키마 정보 (선택사항)
            batch_size: 배치 크기

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
                    schema_columns = [col["name"] for col in source_schema["columns"]]
                    logger.info(
                        f"소스 스키마에서 컬럼명을 가져왔습니다: {schema_columns}"
                    )

                    # CSV 컬럼명 가져오기
                    csv_columns = list(df.columns)
                    logger.info(f"CSV 파일의 실제 컬럼명: {csv_columns}")

                    # CSV 컬럼명과 소스 스키마 컬럼명이 일치하는지 확인
                    if len(csv_columns) != len(schema_columns):
                        logger.warning(
                            f"CSV 컬럼 수({len(csv_columns)})와 소스 스키마 컬럼 수({len(schema_columns)})가 일치하지 않습니다."
                        )

                    # CSV에 실제로 존재하는 컬럼만 사용 (안전한 처리)
                    available_columns = [col for col in schema_columns if col in csv_columns]
                    missing_columns = [col for col in schema_columns if col not in csv_columns]

                    if missing_columns:
                        logger.warning(f"CSV에 없는 컬럼: {missing_columns}")

                    if not available_columns:
                        logger.warning("CSV와 스키마 간에 공통 컬럼이 없습니다. CSV 컬럼명을 그대로 사용합니다.")
                        available_columns = csv_columns

                    # 사용 가능한 컬럼으로 데이터프레임 재구성
                    df_reordered = df[available_columns]
                    temp_columns = available_columns

                    logger.info(
                        f"사용 가능한 컬럼으로 데이터프레임 재구성: {len(available_columns)}개 컬럼"
                    )
                    logger.info(f"최종 사용 컬럼: {temp_columns}")
                    logger.info(f"데이터프레임 형태: {df_reordered.shape}")
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

                # 숫자 타입 컬럼은 NULL 허용으로 설정 (CSV 빈 문자열 처리)
                # DOUBLE PRECISION, BIGINT, INTEGER 등에서 빈 문자열 오류 방지
                if pg_type in ["DOUBLE PRECISION", "BIGINT", "INTEGER", "NUMERIC", "REAL"]:
                    nullable_clause = ""  # NULL 허용
                    logger.info(f"숫자 타입 컬럼 {col_name} ({pg_type})을 NULL 허용으로 설정")
                else:
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
        self, cursor, temp_table: str, csv_path: str, source_schema: dict, batch_size: int = 1000
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

            # 🚨 데이터 타입 검증 및 변환 - CSV 읽기 후 재실행
            logger.info("=== 🚨 CSV 읽기 후 데이터 타입 검증 및 변환 시작 ===")
            df_reordered = self._validate_and_convert_data_types_after_csv_read(df_reordered, source_schema)
            logger.info("=== CSV 읽기 후 데이터 타입 검증 및 변환 완료 ===")

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

                # 모든 컬럼을 문자열로 변환하여 일관성 유지
                df_reordered[col] = df_reordered[col].astype(str)

            # 숫자 타입 컬럼의 빈 문자열을 NULL로 변환
            # PostgreSQL에서 빈 문자열을 숫자 타입으로 변환할 때 오류 방지
            logger.info(f"=== 컬럼 타입 검사 시작 ===")
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]
                logger.info(f"컬럼 {col_name}: 타입={col_type}")

                # priority 컬럼 특별 로깅
                if col_name == "priority":
                    logger.info(f"🚨 PRIORITY 컬럼 발견! 타입: {col_type}, BIGINT 포함 여부: {col_type in ['BIGINT', 'INTEGER']}")
                    logger.info(f"데이터프레임에 컬럼 존재 여부: {col_name in df_reordered.columns}")
                    if col_name in df_reordered.columns:
                        sample_values = df_reordered[col_name].head(5).tolist()
                        logger.info(f"PRIORITY 컬럼 샘플 값: {sample_values}")

                if col_name in df_reordered.columns and col_type in ["DOUBLE PRECISION", "BIGINT", "INTEGER", "NUMERIC", "REAL"]:
                    # 빈 문자열, 공백, 'nan' 문자열을 None(NULL)로 변환
                    df_reordered[col_name] = df_reordered[col_name].replace({
                        "": None,
                        " ": None,
                        "nan": None,
                        "None": None,
                        "NULL": None,
                        "null": None
                    })

                    # BIGINT/INTEGER 컬럼의 소수점 값을 정수로 변환
                    logger.info(f"컬럼 {col_name} 타입 검사: {col_type} (BIGINT/INTEGER 포함 여부: {col_type in ['BIGINT', 'INTEGER']})")
                    if col_type in ["BIGINT", "INTEGER"]:
                        logger.info(f"정수 타입 컬럼 {col_name}의 소수점 값 변환 시작")
                        try:
                            # 더 강력한 소수점 값 변환 로직
                            def convert_decimal_to_int(x):
                                if pd.isna(x) or x is None:
                                    return None
                                if isinstance(x, str):
                                    x = x.strip()
                                    if not x or x in ["", "nan", "None", "NULL", "null"]:
                                        return None
                                    if "." in x:
                                        try:
                                            # 소수점 값을 정수로 변환
                                            float_val = float(x)
                                            int_val = int(float_val)
                                            return str(int_val)
                                        except (ValueError, TypeError):
                                            logger.warning(f"컬럼 {col_name}의 값 '{x}'을 정수로 변환할 수 없습니다.")
                                            return x
                                elif isinstance(x, (int, float)):
                                    return str(int(x))
                                return str(x) if x is not None else None

                            # 변환 전후 값 확인
                            before_values = df_reordered[col_name].tolist()
                            df_reordered[col_name] = df_reordered[col_name].apply(convert_decimal_to_int)
                            after_values = df_reordered[col_name].tolist()

                            # 변환된 값이 있는지 확인
                            changed_count = sum(1 for b, a in zip(before_values, after_values) if b != a)
                            if changed_count > 0:
                                logger.info(f"정수 타입 컬럼 {col_name}의 {changed_count}개 소수점 값이 정수로 변환되었습니다.")
                                # 변환된 값 샘플 로깅
                                for i, (b, a) in enumerate(zip(before_values, after_values)):
                                    if b != a:
                                        logger.info(f"컬럼 {col_name} 행 {i}: '{b}' → '{a}'")
                                        if i >= 2:  # 처음 3개만 로깅
                                            break
                            else:
                                logger.info(f"정수 타입 컬럼 {col_name}에 변환할 소수점 값이 없습니다.")

                            # 변환 후 추가 검증: 여전히 소수점이 있는 값이 있는지 확인
                            remaining_decimals = df_reordered[col_name].astype(str).str.contains(r'\.', na=False).sum()
                            if remaining_decimals > 0:
                                logger.warning(f"컬럼 {col_name}에 여전히 {remaining_decimals}개의 소수점 값이 남아있습니다.")
                                # 문제가 있는 값들을 로깅
                                problem_values = df_reordered[col_name][df_reordered[col_name].astype(str).str.contains(r'\.', na=False)]
                                logger.warning(f"문제가 있는 값들: {problem_values.head(5).tolist()}")

                                # 강제로 모든 소수점 값을 정수로 변환
                                logger.info(f"컬럼 {col_name}의 남은 소수점 값들을 강제 변환합니다.")
                                def force_convert_decimal(x):
                                    if pd.isna(x) or x is None:
                                        return None
                                    if isinstance(x, str) and "." in x:
                                        try:
                                            return str(int(float(x)))
                                        except (ValueError, TypeError):
                                            logger.error(f"강제 변환 실패: '{x}' → NULL로 설정")
                                            return None
                                    return x

                                df_reordered[col_name] = df_reordered[col_name].apply(force_convert_decimal)

                                # 최종 검증
                                final_decimals = df_reordered[col_name].astype(str).str.contains(r'\.', na=False).sum()
                                if final_decimals > 0:
                                    logger.error(f"컬럼 {col_name}에 여전히 {final_decimals}개의 소수점 값이 남아있습니다!")
                                else:
                                    logger.info(f"컬럼 {col_name}의 모든 소수점 값이 성공적으로 변환되었습니다.")
                        except Exception as e:
                            logger.warning(f"컬럼 {col_name}의 소수점 값 변환 중 오류 발생: {e}")

                    # 추가로 pandas의 NaN 값도 None으로 변환
                    df_reordered[col_name] = df_reordered[col_name].replace({pd.NA: None, pd.NaT: None})
                    logger.info(f"숫자 타입 컬럼 {col_name}의 빈 문자열과 NaN을 NULL로 변환했습니다.")

            logger.info(f"데이터 타입 변환 완료: {len(df_reordered)}행")

            # 🚨 CSV 저장 전 최종 PRIORITY 컬럼 검증
            logger.info("=== 🚨 CSV 저장 전 최종 PRIORITY 컬럼 검증 ===")
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]

                if col_name == "priority" and col_type in ["BIGINT", "INTEGER"]:
                    if col_name in df_reordered.columns:
                        # 최종 소수점 값 검사
                        final_decimal_count = df_reordered[col_name].astype(str).str.contains(r'\.', na=False).sum()
                        if final_decimal_count > 0:
                            logger.error(f"🚨 🚨 🚨 CSV 저장 전 PRIORITY 컬럼에 여전히 {final_decimal_count}개의 소수점 값이 남아있습니다!")
                            # 문제가 있는 값들을 강제로 변환
                            problem_values = df_reordered[col_name][df_reordered[col_name].astype(str).str.contains(r'\.', na=False)]
                            logger.error(f"문제가 있는 값들: {problem_values.head(10).tolist()}")

                            # 강제 변환
                            def force_convert_final(x):
                                if pd.isna(x) or x is None:
                                    return None
                                if isinstance(x, str) and "." in x:
                                    try:
                                        return str(int(float(x)))
                                    except (ValueError, TypeError):
                                        logger.error(f"최종 강제 변환 실패: '{x}' → NULL로 설정")
                                        return None
                                return x

                            df_reordered[col_name] = df_reordered[col_name].apply(force_convert_final)

                            # 최종 검증
                            final_check = df_reordered[col_name].astype(str).str.contains(r'\.', na=False).sum()
                            if final_check == 0:
                                logger.info("✅ 최종 강제 변환 후 모든 소수점 값이 제거되었습니다.")
                            else:
                                logger.error(f"🚨 🚨 🚨 최종 강제 변환 후에도 {final_check}개의 소수점 값이 남아있습니다!")
                        else:
                            logger.info("✅ PRIORITY 컬럼에 소수점 값이 없습니다. CSV 저장을 진행합니다.")

            # 데이터 샘플 로깅 (디버깅용)
            logger.info("처리된 데이터 샘플 (처음 3행):")
            for i, row in df_reordered.head(3).iterrows():
                logger.info(f"행 {i}: {dict(row)}")

            # INSERT 실행 - 파라미터로 받은 batch_size 사용
            total_inserted = 0

            for i in range(0, len(df_reordered), batch_size):
                batch_df = df_reordered.iloc[i : i + batch_size]

                # None 값을 제대로 처리하기 위해 데이터 변환
                batch_data = []
                for _, row in batch_df.iterrows():
                    # None 값을 PostgreSQL의 NULL로 변환
                    row_data = []
                    for col_idx, value in enumerate(row):
                        # 컬럼명과 타입 정보 가져오기
                        col_name = temp_columns[col_idx]
                        col_type = None
                        for col in source_schema["columns"]:
                            if col["name"] == col_name:
                                col_type = col["type"]
                                break

                        # priority 컬럼 특별 로깅 (INSERT 단계)
                        if col_name == "priority":
                            logger.info(f"🚨 INSERT 단계 - PRIORITY 컬럼: 값='{value}', 타입='{col_type}'")
                            if isinstance(value, str) and "." in value:
                                logger.info(f"🚨 PRIORITY 컬럼에 소수점 값 발견: '{value}'")

                        # None 값 처리
                        if value is None or value == "None" or value == "null" or value == "NULL":
                            row_data.append(None)
                        elif value == "" or value == " " or value == "nan":
                            row_data.append(None)
                        # BIGINT/INTEGER 컬럼의 소수점 값 처리
                        elif col_type in ["BIGINT", "INTEGER"] and isinstance(value, str) and "." in value:
                            try:
                                # 소수점 값을 정수로 변환
                                int_value = int(float(value))
                                row_data.append(str(int_value))
                                logger.debug(f"컬럼 {col_name}의 값 '{value}'을 '{int_value}'로 변환")
                            except (ValueError, TypeError):
                                # 변환 실패 시 원본 값 사용
                                row_data.append(value)
                                logger.warning(f"컬럼 {col_name}의 값 '{value}'을 정수로 변환할 수 없습니다.")
                        # 추가 안전장치: BIGINT/INTEGER 컬럼의 모든 값 검증
                        elif col_type in ["BIGINT", "INTEGER"] and isinstance(value, str):
                            # 빈 문자열이나 공백을 NULL로 변환
                            if not value.strip():
                                row_data.append(None)
                            # 소수점이 있는지 다시 한번 확인하고 변환
                            elif "." in value:
                                try:
                                    int_value = int(float(value))
                                    row_data.append(str(int_value))
                                    logger.debug(f"컬럼 {col_name}의 값 '{value}'을 '{int_value}'로 변환 (추가 검증)")
                                except (ValueError, TypeError):
                                    logger.error(f"컬럼 {col_name}의 값 '{value}'을 정수로 변환할 수 없습니다. NULL로 설정합니다.")
                                    row_data.append(None)
                            else:
                                row_data.append(value)
                        else:
                            row_data.append(value)
                    batch_data.append(tuple(row_data))

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

    def _build_where_clause(
        self,
        custom_where: str | None = None,
        where_clause: str | None = None,
        sync_mode: str = "full_sync",
        incremental_field: str | None = None,
        incremental_field_type: str | None = None,
        target_table: str | None = None,
    ) -> str | None:
        """
        최종 WHERE 조건 구성

        Args:
            custom_where: 커스텀 WHERE 조건
            where_clause: 기본 WHERE 절 조건
            sync_mode: 동기화 모드
            incremental_field: 증분 필드명
            incremental_field_type: 증분 필드 타입
            target_table: 타겟 테이블명

        Returns:
            구성된 WHERE 조건 문자열
        """
        conditions = []

        # 1. custom_where가 있으면 우선 적용
        if custom_where:
            conditions.append(f"({custom_where})")
            logger.info(f"커스텀 WHERE 조건 추가: {custom_where}")

        # 2. 증분 동기화 모드일 때 incremental_field 조건 추가
        if sync_mode == "incremental_sync" and incremental_field and target_table:
            incremental_condition = self._build_incremental_condition(
                incremental_field, incremental_field_type, target_table
            )
            if incremental_condition:
                conditions.append(f"({incremental_condition})")
                logger.info(f"증분 조건 추가: {incremental_condition}")

        # 3. 기본 where_clause가 있으면 추가
        if where_clause:
            conditions.append(f"({where_clause})")
            logger.info(f"기본 WHERE 조건 추가: {where_clause}")

        # 4. 모든 조건을 AND로 결합
        if conditions:
            final_where = " AND ".join(conditions)
            logger.info(f"최종 WHERE 조건 구성: {final_where}")
            return final_where
        else:
            logger.info("WHERE 조건 없음 - 전체 데이터 처리")
            return None

    def _build_incremental_condition(
        self,
        incremental_field: str,
        incremental_field_type: str | None,
        target_table: str,
    ) -> str | None:
        """
        증분 동기화를 위한 WHERE 조건 구성

        Args:
            incremental_field: 증분 필드명
            incremental_field_type: 증분 필드 타입
            target_table: 타겟 테이블명

        Returns:
            증분 조건 문자열
        """
        try:
            # 타겟 테이블에서 마지막 업데이트 시간 조회
            last_update_query = f"""
                SELECT MAX({incremental_field})
                FROM {target_table}
                WHERE {incremental_field} IS NOT NULL
            """

            last_update_result = self.target_hook.get_first(last_update_query)
            last_update_time = last_update_result[0] if last_update_result and last_update_result[0] else None

            if not last_update_time:
                logger.info(f"타겟 테이블 {target_table}에 증분 필드 {incremental_field} 데이터가 없음 - 전체 데이터 처리")
                return None

            # 필드 타입에 따른 조건 구성
            if incremental_field_type == "yyyymmdd":
                # YYYYMMDD 형식 (예: 20250812) - 이상(>=) 조건 사용
                # 날짜 형식에서는 정확한 시점을 잡기 어려우므로 >= 사용
                condition = f"{incremental_field} >= '{last_update_time}'"
                logger.info(f"YYYYMMDD 형식 감지 - 이상(>=) 조건 사용: {condition}")
            elif incremental_field_type == "timestamp":
                # TIMESTAMP 형식 - 초과(>) 조건 사용 (밀리초 단위 정밀도)
                condition = f"{incremental_field} > '{last_update_time}'"
            elif incremental_field_type == "date":
                # DATE 형식 - 이상(>=) 조건 사용 (날짜 단위)
                condition = f"{incremental_field} >= '{last_update_time}'"
            elif incremental_field_type == "datetime":
                # DATETIME 형식 - 초과(>) 조건 사용 (초 단위 정밀도)
                condition = f"{incremental_field} > '{last_update_time}'"
            elif incremental_field_type == "integer":
                # 정수형 (예: Unix timestamp) - 초과(>) 조건 사용
                condition = f"{incremental_field} > {last_update_time}"
            else:
                # 기본값: 문자열 비교 - 초과(>) 조건 사용
                condition = f"{incremental_field} > '{last_update_time}'"

            logger.info(f"증분 조건 구성: {condition} (마지막 업데이트: {last_update_time})")
            return condition

        except Exception as e:
            logger.warning(f"증분 조건 구성 실패: {e!s} - 증분 조건 없이 진행")
            return None

    def copy_table_data(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str = "incremental_sync",
        batch_size: int = 10000,
        custom_where: str | None = None,
        incremental_field: str | None = None,
        incremental_field_type: str | None = None,
    ) -> dict[str, Any]:
        """
        테이블 데이터 복사 (기본 메서드)
        """
        return self._copy_table_data_internal(
            source_table=source_table,
            target_table=target_table,
            primary_keys=primary_keys,
            sync_mode=sync_mode,
            batch_size=batch_size,
            custom_where=custom_where,
            incremental_field=incremental_field,
            incremental_field_type=incremental_field_type,
        )

    def copy_table_data_with_custom_sql(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str = "incremental_sync",
        batch_size: int = 10000,
        custom_where: str | None = None,
        incremental_field: str | None = None,
        incremental_field_type: str | None = None,
        count_sql: str | None = None,
        select_sql: str | None = None,
    ) -> dict[str, Any]:
        """
        사용자 정의 SQL을 사용하여 테이블 데이터 복사

        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            primary_keys: 기본키 컬럼 리스트
            sync_mode: 동기화 모드
            batch_size: 배치 크기
            custom_where: 커스텀 WHERE 조건
            incremental_field: 증분 필드명
            incremental_field_type: 증분 필드 타입
            count_sql: 사용자 정의 COUNT SQL
            select_sql: 사용자 정의 SELECT SQL

        Returns:
            복사 결과 딕셔너리
        """
        # 사용자 정의 SQL이 제공된 경우 이를 사용
        if count_sql and select_sql:
            return self._copy_table_data_with_custom_sql_internal(
                source_table=source_table,
                target_table=target_table,
                primary_keys=primary_keys,
                sync_mode=sync_mode,
                batch_size=batch_size,
                custom_where=custom_where,
                incremental_field=incremental_field,
                incremental_field_type=incremental_field_type,
                count_sql=count_sql,
                select_sql=select_sql,
            )
        else:
            # 기본 메서드 사용
            return self._copy_table_data_internal(
                source_table=source_table,
                target_table=target_table,
                primary_keys=primary_keys,
                sync_mode=sync_mode,
                batch_size=batch_size,
                custom_where=custom_where,
                incremental_field=incremental_field,
                incremental_field_type=incremental_field_type,
            )

    def _copy_table_data_internal(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str,
        batch_size: int,
        custom_where: str | None,
        incremental_field: str | None,
        incremental_field_type: str | None,
    ) -> dict[str, Any]:
        """
        테이블 데이터 복사 (내부 메서드)
        """
        csv_path = None
        temp_table = None

        try:
            start_time = pd.Timestamp.now()

            # WHERE 조건 구성 (custom_where 우선, where_clause는 기본값)
            final_where_clause = self._build_where_clause(
                custom_where=custom_where,
                where_clause=None,
                sync_mode=sync_mode,
                incremental_field=incremental_field,
                incremental_field_type=incremental_field_type,
                target_table=target_table
            )

            logger.info(f"최종 WHERE 조건: {final_where_clause}")

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
                source_table, csv_path, final_where_clause, batch_size, incremental_field
            )

            if exported_rows == 0:
                return {
                    "status": "warning",
                    "message": f"소스 테이블 {source_table}에 조건에 맞는 데이터가 없습니다. WHERE: {final_where_clause}",
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
                        cursor, temp_table, csv_path, source_schema, batch_size
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
                "where_clause_used": final_where_clause,
                "sync_mode": sync_mode,
                "message": (
                    f"테이블 복사 완료: {source_table} -> {target_table}, "
                    f"WHERE: {final_where_clause}, "
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

    def _copy_table_data_with_custom_sql_internal(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str,
        batch_size: int,
        custom_where: str | None,
        incremental_field: str | None,
        incremental_field_type: str | None,
        count_sql: str,
        select_sql: str,
    ) -> dict[str, Any]:
        """
        사용자 정의 SQL을 사용하여 테이블 데이터 복사 (내부 메서드)
        """
        csv_path = None
        start_time = time.time()

        try:
            # 1단계: 데이터 내보내기 (사용자 정의 SQL 사용)
            logger.info(f"사용자 정의 SQL을 사용하여 데이터 내보내기 시작: {source_table}")

            # 전체 행 수 조회 (사용자 정의 COUNT SQL 사용)
            if count_sql:
                total_count = self.source_hook.get_first(count_sql)[0]
                logger.info(f"사용자 정의 COUNT SQL로 조회된 행 수: {total_count}")
            else:
                total_count = self.db_ops.get_table_row_count(source_table, where_clause=custom_where)

            if total_count == 0:
                logger.warning(f"테이블 {source_table}에 데이터가 없습니다.")
                return {
                    "status": "success",
                    "exported_rows": 0,
                    "imported_rows": 0,
                    "total_execution_time": 0,
                    "message": "데이터가 없습니다."
                }

            # CSV 파일 경로 생성
            csv_path = f"/tmp/temp_{source_table.replace('.', '_')}.csv"

            # 사용자 정의 SELECT SQL을 사용하여 데이터 추출
            if select_sql:
                logger.info(f"사용자 정의 SELECT SQL 사용: {select_sql}")
                exported_rows = self._export_with_custom_sql(select_sql, csv_path, batch_size)
            else:
                # 기본 내보내기 사용
                exported_rows = self.export_to_csv(
                    source_table, csv_path, custom_where, batch_size, incremental_field
                )

            if exported_rows == 0:
                logger.warning(f"내보낸 데이터가 없습니다.")
                return {
                    "status": "success",
                    "exported_rows": 0,
                    "imported_rows": 0,
                    "total_execution_time": time.time() - start_time,
                    "message": "내보낸 데이터가 없습니다."
                }

            # 2단계: 데이터 가져오기
            logger.info(f"타겟 DB에서 임시 테이블 생성, 데이터 삽입, MERGE 작업 시작: {target_table}")
            imported_rows = self._import_and_merge_data(
                csv_path, target_table, primary_keys, sync_mode
            )

            # 3단계: 결과 정리
            total_time = time.time() - start_time

            result = {
                "status": "success",
                "exported_rows": exported_rows,
                "imported_rows": imported_rows,
                "total_execution_time": total_time,
                "message": f"데이터 복사 완료: {exported_rows}행 내보내기, {imported_rows}행 가져오기"
            }

            logger.info(f"사용자 정의 SQL을 사용한 데이터 복사 완료: {result}")
            return result

        except Exception as e:
            total_time = time.time() - start_time
            error_msg = f"사용자 정의 SQL을 사용한 데이터 복사 실패: {e}"
            logger.error(error_msg)

            result = {
                "status": "error",
                "exported_rows": 0,
                "imported_rows": 0,
                "total_execution_time": total_time,
                "error": error_msg
            }

            return result

        finally:
            # 임시 파일 정리
            if csv_path and os.path.exists(csv_path):
                try:
                    os.remove(csv_path)
                    logger.info(f"임시 파일 삭제 완료: {csv_path}")
                except Exception as e:
                    logger.warning(f"임시 파일 삭제 실패: {e}")

    def _export_with_custom_sql(self, select_sql: str, csv_path: str, batch_size: int, source_schema: dict[str, Any] = None) -> int:
        """
        사용자 정의 SELECT SQL을 사용하여 데이터를 CSV로 내보내기

        Args:
            select_sql: SELECT SQL 쿼리
            csv_path: CSV 파일 경로
            batch_size: 배치 크기
            source_schema: 소스 테이블 스키마 정보 (컬럼명 추출용)
        """
        try:
            # 사용자 정의 SQL로 데이터 추출
            all_data = []
            offset = 0

            while True:
                # LIMIT와 OFFSET을 추가한 SQL 생성
                paginated_sql = f"{select_sql} LIMIT {batch_size} OFFSET {offset}"
                logger.info(f"페이지네이션 SQL 실행: OFFSET {offset}")

                batch_data = self.source_hook.get_records(paginated_sql)

                if not batch_data:
                    break

                all_data.extend(batch_data)
                offset += batch_size

                logger.info(f"배치 처리 진행률: {len(all_data)}행 수집")

                # 무한 루프 방지
                if len(batch_data) < batch_size:
                    break

            if not all_data:
                logger.warning("사용자 정의 SQL로 데이터를 찾을 수 없습니다.")
                return 0

            # DataFrame으로 변환 (컬럼명 명시)
            if source_schema and source_schema.get("columns"):
                # 소스 스키마에서 컬럼명 가져오기
                column_names = [col["name"] for col in source_schema["columns"]]
            else:
                # 기본 컬럼명 사용
                column_names = [f"col_{i}" for i in range(len(all_data[0]) if all_data else 0)]

            df = pd.DataFrame(all_data, columns=column_names)

            # CSV로 저장 (컬럼명 포함)
            df.to_csv(csv_path, index=False, header=True)
            logger.info(f"사용자 정의 SQL로 CSV 내보내기 완료: {csv_path}, 행 수: {len(all_data)}")

            return len(all_data)

        except Exception as e:
            logger.error(f"사용자 정의 SQL로 CSV 내보내기 실패: {e}")
            raise

    def _import_and_merge_data(self, csv_path: str, target_table: str, primary_keys: list[str], sync_mode: str) -> int:
        """
        임시 테이블에서 데이터를 가져오고 MERGE 작업 수행 (개선된 버전)
        """
        try:
            # 1단계: 타겟 테이블 스키마 가져오기 (이미 검증된 스키마 사용)
            target_schema = self.db_ops.get_table_schema(target_table)

            if not target_schema or not target_schema.get("columns"):
                raise Exception(f"타겟 테이블 {target_table}의 스키마 정보를 가져올 수 없습니다")

            # 2단계: 임시 테이블 생성 (검증된 스키마 사용)
            temp_table = self.create_temp_table(target_table, target_schema)

            # 3단계: CSV에서 임시 테이블로 데이터 가져오기
            imported_rows = self.import_from_csv(csv_path, temp_table, target_schema)

            if imported_rows == 0:
                logger.warning(f"임시 테이블 {temp_table}에서 데이터를 가져오는데 실패했습니다.")
                return 0

            # 4단계: MERGE 작업 수행 (temp_table을 소스로 사용)
            merge_result = self.execute_merge_operation(
                temp_table, target_table, primary_keys, sync_mode
            )

            if merge_result["status"] != "success":
                logger.error(f"MERGE 작업 실패: {merge_result['message']}")
                return 0

            return imported_rows

        except Exception as e:
            logger.error(f"임시 테이블에서 데이터 가져오기 및 MERGE 작업 실패: {e}")
            return 0

    def create_target_table_and_verify_schema(
        self, target_table: str, source_schema: dict[str, Any]
    ) -> dict[str, Any]:
        """
        타겟 테이블 생성과 스키마 검증을 한 번에 처리

        Args:
            target_table: 타겟 테이블명
            source_schema: 소스 테이블 스키마 정보

        Returns:
            검증된 타겟 테이블 스키마 정보
        """
        try:
            logger.info(f"타겟 테이블 생성 및 스키마 검증 시작: {target_table}")

            # DatabaseOperations의 새로운 메서드 사용
            verified_schema = self.db_ops.create_table_and_verify_schema(
                target_table, source_schema
            )

            logger.info(f"타겟 테이블 생성 및 스키마 검증 완료: {target_table}")
            return verified_schema

        except Exception as e:
            logger.error(f"타겟 테이블 생성 및 스키마 검증 실패: {target_table}, 오류: {e}")
            raise

    def copy_data_with_custom_sql(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str = "incremental_sync",
        incremental_field: str | None = None,
        incremental_field_type: str | None = None,
        custom_where: str | None = None,
        batch_size: int = 10000,
        verified_target_schema: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        검증된 타겟 스키마를 사용하여 사용자 정의 SQL로 데이터 복사

        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            primary_keys: 기본키 목록
            sync_mode: 동기화 모드
            incremental_field: 증분 필드명
            incremental_field_type: 증분 필드 타입
            custom_where: 사용자 정의 WHERE 조건
            batch_size: 배치 크기
            verified_target_schema: 검증된 타겟 테이블 스키마

        Returns:
            복사 결과
        """
        try:
            start_time = time.time()

            # 1단계: 소스 테이블 스키마 조회
            source_schema = self.db_ops.get_table_schema(source_table)

            # 2단계: 사용자 정의 SQL 생성
            count_sql, select_sql = self._build_custom_sql_queries(
                source_table, source_schema, incremental_field,
                incremental_field_type, custom_where
            )

            # 3단계: 데이터 개수 확인
            row_count = self._get_source_row_count(count_sql)

            if row_count == 0:
                logger.info(f"소스 테이블 {source_table}에 조건에 맞는 데이터가 없습니다.")
                return {
                    "status": "success",
                    "exported_rows": 0,
                    "imported_rows": 0,
                    "total_execution_time": time.time() - start_time,
                    "message": "조건에 맞는 데이터가 없음"
                }

            # 4단계: CSV 파일로 데이터 내보내기
            csv_path = f"/tmp/temp_{source_table.replace('.', '_')}.csv"
            exported_rows = self._export_with_custom_sql(select_sql, csv_path, batch_size, source_schema)

            if exported_rows == 0:
                raise Exception("데이터 내보내기 실패")

            # 5단계: 데이터 가져오기 및 MERGE (검증된 스키마 사용)
            if verified_target_schema:
                # 검증된 스키마가 있으면 직접 사용
                imported_rows = self._import_and_merge_with_verified_schema(
                    csv_path, target_table, primary_keys, sync_mode, verified_target_schema
                )
            else:
                # 기존 방식 사용
                imported_rows = self._import_and_merge_data(
                    csv_path, target_table, primary_keys, sync_mode
                )

            total_time = time.time() - start_time

            result = {
                "status": "success",
                "exported_rows": exported_rows,
                "imported_rows": imported_rows,
                "total_execution_time": total_time,
                "message": f"데이터 복사 완료: {exported_rows}행 내보내기, {imported_rows}행 가져오기"
            }

            logger.info(f"사용자 정의 SQL을 사용한 데이터 복사 완료: {result}")
            return result

        except Exception as e:
            total_time = time.time() - start_time
            error_msg = f"사용자 정의 SQL을 사용한 데이터 복사 실패: {e}"
            logger.error(error_msg)

            result = {
                "status": "error",
                "exported_rows": 0,
                "imported_rows": 0,
                "total_execution_time": total_time,
                "error": error_msg
            }

            return result

        finally:
            # 임시 파일 정리
            if 'csv_path' in locals() and csv_path and os.path.exists(csv_path):
                try:
                    os.remove(csv_path)
                    logger.info(f"임시 파일 삭제 완료: {csv_path}")
                except Exception as e:
                    logger.warning(f"임시 파일 삭제 실패: {e}")

    def _build_custom_sql_queries(
        self,
        source_table: str,
        source_schema: dict[str, Any],
        incremental_field: str | None = None,
        incremental_field_type: str | None = None,
        custom_where: str | None = None
    ) -> tuple[str, str]:
        """사용자 정의 SQL 쿼리 생성"""
        try:
            # 원본 컬럼명 사용 (변환 없이)
            column_names = [col["name"] for col in source_schema["columns"]]
            transformed_columns = [f'"{col_name}"' for col_name in column_names]

            # 기본 WHERE 조건
            where_conditions = []
            if custom_where:
                where_conditions.append(custom_where)
            # custom_where가 이미 증분 조건을 포함하고 있으므로 중복 추가하지 않음
            elif incremental_field and incremental_field_type:
                # custom_where가 없는 경우에만 증분 동기화 조건 추가
                if incremental_field_type == "yyyymmdd":
                    where_conditions.append(f"{incremental_field} >= '20250812'")
                # 다른 증분 필드 타입들도 추가 가능

            where_clause = " AND ".join(where_conditions) if where_conditions else ""
            where_sql = f" WHERE {where_clause}" if where_clause else ""

            # COUNT 쿼리
            count_sql = f'SELECT COUNT(*) FROM {source_table}{where_sql}'

            # SELECT 쿼리
            select_sql = f'SELECT {", ".join(transformed_columns)} FROM {source_table}{where_sql} ORDER BY {incremental_field or "1"}'

            return count_sql, select_sql

        except Exception as e:
            logger.error(f"사용자 정의 SQL 쿼리 생성 실패: {e}")
            raise

    def _apply_column_transformation(self, column_name: str, data_type: str) -> str:
        """컬럼별 데이터 변환 적용"""
        # 간단한 변환 로직 (필요에 따라 확장 가능)
        if data_type.lower() in ["bigint", "integer", "smallint"]:
            return f"""
                CASE
                    WHEN "{column_name}" IS NULL THEN NULL
                    WHEN "{column_name}"::TEXT = '' THEN NULL
                    WHEN "{column_name}"::TEXT ~ '^[0-9]+\.[0]+$' THEN CAST("{column_name}" AS BIGINT)::TEXT
                    ELSE "{column_name}"::TEXT
                END AS "{column_name}"
            """
        elif data_type.lower() in ["text", "character varying", "varchar"]:
            return f"""
                CASE
                    WHEN "{column_name}" IS NULL THEN NULL
                    WHEN "{column_name}" = '' THEN NULL
                    WHEN "{column_name}" ~ '^[0-9]+$' THEN CAST("{column_name}" AS BIGINT)::TEXT
                    WHEN "{column_name}" ~ '^[0-9]{{8}}$' AND "{column_name}" ~ '^(19|20)[0-9]{{6}}$'
                        THEN TO_CHAR(TO_DATE("{column_name}", 'YYYYMMDD'), 'YYYY-MM-DD')
                    ELSE "{column_name}"
                END AS "{column_name}"
            """
        else:
            return f'"{column_name}"'

    def _get_source_row_count(self, count_sql: str) -> int:
        """소스 테이블의 조건에 맞는 행 수 조회"""
        try:
            result = self.source_hook.get_first(count_sql)
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"소스 테이블 행 수 조회 실패: {e}")
            return 0

    def _import_and_merge_with_verified_schema(
        self,
        csv_path: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str,
        verified_schema: dict[str, Any]
    ) -> int:
        """검증된 스키마를 사용하여 데이터 가져오기 및 MERGE"""
        try:
            # 하나의 연결에서 모든 작업 수행
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # 1단계: 임시 테이블 생성 및 데이터 가져오기
                    imported_rows, temp_table = self._create_temp_table_and_import_csv_in_connection(
                        cursor, target_table, verified_schema, csv_path
                    )

                    if imported_rows == 0:
                        logger.warning(f"데이터를 가져오는데 실패했습니다.")
                        return 0

                    # 2단계: 같은 연결에서 MERGE 작업 수행
                    merge_result = self._execute_merge_operation_with_cursor(
                        cursor, temp_table, target_table, primary_keys, sync_mode, verified_schema
                    )

                    if merge_result["status"] != "success":
                        logger.error(f"MERGE 작업 실패: {merge_result['message']}")
                        return 0

                    # 커밋
                    conn.commit()
                    return imported_rows

        except Exception as e:
            logger.error(f"검증된 스키마를 사용한 데이터 가져오기 및 MERGE 실패: {e}")
            return 0

    def _create_temp_table_and_import_csv(
        self,
        target_table: str,
        source_schema: dict[str, Any],
        csv_path: str,
        batch_size: int = 1000
    ) -> tuple[int, str]:
        """
        임시 테이블 생성과 CSV 가져오기를 하나의 세션에서 처리

        Args:
            target_table: 타겟 테이블명
            source_schema: 소스 테이블 스키마 정보
            csv_path: CSV 파일 경로
            batch_size: 배치 크기

        Returns:
            가져온 행 수
        """
        try:
            # CSV 파일 읽기
            df = pd.read_csv(csv_path, encoding="utf-8")

            if df.empty:
                logger.warning(f"CSV 파일 {csv_path}에 데이터가 없습니다.")
                return 0

            # 임시 테이블명 생성
            temp_table = (
                f"temp_{target_table.replace('.', '_')}_"
                f"{int(pd.Timestamp.now().timestamp())}"
            )

            # 컬럼 정의 생성
            column_definitions = []
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]
                col_type = self._convert_to_postgres_type(col_type, col.get('max_length'))
                nullable = "" if col["nullable"] else " NOT NULL"
                column_definitions.append(f"{col_name} {col_type}{nullable}")

            # 임시 테이블 생성 SQL
            create_temp_table_sql = f"""
                CREATE TEMP TABLE {temp_table} (
                    {', '.join(column_definitions)}
                ) ON COMMIT DROP
            """

            logger.info(f"임시 테이블 생성 SQL: {create_temp_table_sql}")

            # 같은 연결에서 임시 테이블 생성 및 데이터 가져오기
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # 1. 임시 테이블 생성
                    cursor.execute(create_temp_table_sql)
                    logger.info(f"임시 테이블 생성 완료: {temp_table}")

                    # 2. 임시 테이블이 실제로 생성되었는지 확인
                    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{temp_table}')")
                    exists = cursor.fetchone()[0]
                    if not exists:
                        raise Exception(f"임시 테이블 {temp_table}이 생성되지 않았습니다.")

                    # 3. CSV 데이터를 임시 테이블로 가져오기
                    # 컬럼명 매핑
                    if source_schema and source_schema.get("columns"):
                        schema_columns = [col["name"] for col in source_schema["columns"]]
                        available_columns = [col for col in schema_columns if col in df.columns]

                        if not available_columns:
                            logger.warning("CSV와 스키마 간에 공통 컬럼이 없습니다.")
                            return 0

                        df_reordered = df[available_columns]
                        temp_columns = available_columns
                    else:
                        temp_columns = list(df.columns)
                        df_reordered = df

                    # 스키마 기반 데이터 타입 변환 및 정리
                    for col in df_reordered.columns:
                        # NaN 값을 None으로 변환
                        df_reordered[col] = df_reordered[col].where(pd.notna(df_reordered[col]), None)

                        # 스키마에서 해당 컬럼의 타입 정보 찾기
                        col_schema = next((c for c in source_schema["columns"] if c["name"] == col), None)
                        col_type = col_schema["type"].lower() if col_schema else "text"

                        # 숫자 컬럼의 경우 문자열로 변환하여 안전하게 처리
                        if df_reordered[col].dtype in ["int64", "float64"]:
                            df_reordered[col] = df_reordered[col].astype(str)

                        # 문자열 컬럼에서 'nan', 'NaN' 값을 None으로 변환
                        if df_reordered[col].dtype == "object":
                            df_reordered[col] = df_reordered[col].replace(['nan', 'NaN', 'None', ''], None)

                        # BIGINT 컬럼의 경우 부동소수점 값을 정수로 변환 (예: "1.0" -> "1")
                        if col_type in ["bigint", "integer", "int"] and df_reordered[col].dtype == "object":
                            df_reordered[col] = df_reordered[col].apply(
                                lambda x: str(int(float(x))) if x and isinstance(x, str) and x.replace('.', '').replace('-', '').isdigit() and float(x).is_integer() else x
                            )

                    # 배치 단위로 INSERT 실행
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

                    # 커밋
                    conn.commit()

                    logger.info(f"CSV 가져오기 완료: {csv_path} -> {temp_table}, 행 수: {total_inserted}")
                    return total_inserted, temp_table

        except Exception as e:
            logger.error(f"임시 테이블 생성 및 CSV 가져오기 실패: {e}")
            raise

    def _create_temp_table_and_import_csv_in_connection(
        self,
        cursor,
        target_table: str,
        source_schema: dict[str, Any],
        csv_path: str,
        batch_size: int = 1000
    ) -> tuple[int, str]:
        """
        기존 커서를 사용하여 임시 테이블 생성과 CSV 가져오기를 수행
        """
        try:
            # CSV 파일 읽기
            df = pd.read_csv(csv_path, encoding="utf-8")

            if df.empty:
                logger.warning(f"CSV 파일 {csv_path}에 데이터가 없습니다.")
                return 0, ""

            # 임시 테이블명 생성
            temp_table = (
                f"temp_{target_table.replace('.', '_')}_"
                f"{int(pd.Timestamp.now().timestamp())}"
            )

            # 컬럼 정의 생성
            column_definitions = []
            for col in source_schema["columns"]:
                col_name = col["name"]
                col_type = col["type"]
                col_type = self._convert_to_postgres_type(col_type, col.get('max_length'))
                nullable = "" if col["nullable"] else " NOT NULL"
                column_definitions.append(f"{col_name} {col_type}{nullable}")

            # 임시 테이블 생성 SQL
            create_temp_table_sql = f"""
                CREATE TEMP TABLE {temp_table} (
                    {', '.join(column_definitions)}
                ) ON COMMIT DROP
            """

            logger.info(f"임시 테이블 생성 SQL: {create_temp_table_sql}")

            # 1. 임시 테이블 생성
            cursor.execute(create_temp_table_sql)
            logger.info(f"임시 테이블 생성 완료: {temp_table}")

            # 2. 임시 테이블이 실제로 생성되었는지 확인
            cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{temp_table}')")
            exists = cursor.fetchone()[0]
            if not exists:
                raise Exception(f"임시 테이블 {temp_table}이 생성되지 않았습니다.")

            # 3. CSV 데이터를 임시 테이블로 가져오기
            # 컬럼명 매핑
            if source_schema and source_schema.get("columns"):
                schema_columns = [col["name"] for col in source_schema["columns"]]
                available_columns = [col for col in schema_columns if col in df.columns]

                if not available_columns:
                    logger.warning("CSV와 스키마 간에 공통 컬럼이 없습니다.")
                    return 0, ""

                df_reordered = df[available_columns]
                temp_columns = available_columns
            else:
                temp_columns = list(df.columns)
                df_reordered = df

            # 스키마 기반 데이터 타입 변환 및 정리
            for col in df_reordered.columns:
                # NaN 값을 None으로 변환
                df_reordered[col] = df_reordered[col].where(pd.notna(df_reordered[col]), None)

                # 스키마에서 해당 컬럼의 타입 정보 찾기
                col_schema = next((c for c in source_schema["columns"] if c["name"] == col), None)
                col_type = col_schema["type"].lower() if col_schema else "text"

                # 숫자 컬럼의 경우 문자열로 변환하여 안전하게 처리
                if df_reordered[col].dtype in ["int64", "float64"]:
                    df_reordered[col] = df_reordered[col].astype(str)

                # 문자열 컬럼에서 'nan', 'NaN' 값을 None으로 변환
                if df_reordered[col].dtype == "object":
                    df_reordered[col] = df_reordered[col].replace(['nan', 'NaN', 'None', ''], None)

                # BIGINT 컬럼의 경우 부동소수점 값을 정수로 변환 (예: "1.0" -> "1")
                if col_type in ["bigint", "integer", "int"] and df_reordered[col].dtype == "object":
                    df_reordered[col] = df_reordered[col].apply(
                        lambda x: str(int(float(x))) if x and isinstance(x, str) and x.replace('.', '').replace('-', '').isdigit() and float(x).is_integer() else x
                    )

            # 배치 단위로 INSERT 실행
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

            logger.info(f"CSV 가져오기 완료: {csv_path} -> {temp_table}, 행 수: {total_inserted}")
            return total_inserted, temp_table

        except Exception as e:
            logger.error(f"임시 테이블 생성 및 CSV 가져오기 실패: {e}")
            raise

    def _execute_merge_operation_same_connection(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str = "full_sync",
        verified_schema: dict[str, Any] = None
    ) -> dict[str, Any]:
        """
        같은 연결에서 MERGE 작업 실행 (임시 테이블과 함께 사용)
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

            # 같은 연결에서 컬럼 정보 조회
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
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
                        cursor.execute(merge_sql)
                        conn.commit()
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
                    cursor.execute(merge_sql)
                    conn.commit()
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
            logger.error(f"MERGE 작업 실패: {source_table} -> {target_table}, 오류: {e}")
            return {
                "status": "error",
                "message": str(e),
                "source_count": 0,
                "target_count": 0,
                "sync_mode": sync_mode,
                "execution_time": 0
            }

    def _execute_merge_operation_with_cursor(
        self,
        cursor,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str = "full_sync",
        verified_schema: dict[str, Any] = None
    ) -> dict[str, Any]:
        """
        기존 커서를 사용하여 MERGE 작업 실행
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

            # 기본키 제약조건이 있는지 확인하고 없으면 생성
            constraint_name = f"pk_{table}_{'_'.join(primary_keys)}"
            check_constraint_sql = f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE constraint_name = '{constraint_name}'
                    AND table_schema = '{schema}'
                    AND table_name = '{table}'
                )
            """
            cursor.execute(check_constraint_sql)
            constraint_exists = cursor.fetchone()[0]

            if not constraint_exists:
                logger.info(f"기본키 제약조건 생성: {constraint_name}")
                create_constraint_sql = f"""
                    ALTER TABLE {target_table}
                    ADD CONSTRAINT {constraint_name}
                    PRIMARY KEY ({pk_columns})
                """
                cursor.execute(create_constraint_sql)
                logger.info(f"기본키 제약조건 생성 완료: {constraint_name}")

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
            logger.error(f"MERGE 작업 실패: {source_table} -> {target_table}, 오류: {e}")
            return {
                "status": "error",
                "message": str(e),
                "source_count": 0,
                "target_count": 0,
                "sync_mode": sync_mode,
                "execution_time": 0
            }
