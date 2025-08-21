"""
Data Copy Engine Module
데이터 복사 핵심 로직, 스트리밍 파이프라인, 스테이징 테이블 관리, MERGE 작업 등을 담당
스트리밍 방식과 스테이징 테이블 방식만 지원 (CSV 및 임시 테이블 방식 제거됨)
"""

import logging
import os
import time

import json
import gc
import psutil
import subprocess
from typing import Any
from contextlib import contextmanager

import pandas as pd
import psycopg2

from common.database_operations import DatabaseOperations

logger = logging.getLogger(__name__)


class DataCopyEngine:
    """데이터 복사 엔진 클래스"""
    
    # 메모리 사용량 임계값 상수
    class MemoryThresholds:
        """메모리 사용량 임계값 정의"""
        WARNING_MB = 500      # 경고 임계값 (500MB)
        CRITICAL_MB = 1000    # 위험 임계값 (1GB)
        EMERGENCY_MB = 1500   # 긴급 임계값 (1.5GB)
    
    # 청크 방식 기본 설정 상수
    class ChunkModeDefaults:
        """청크 방식 기본 설정"""
        DEFAULT_CHUNK_MODE = True
        DEFAULT_ENABLE_CHECKPOINT = True
        DEFAULT_MAX_RETRIES = 3
        DEFAULT_BATCH_SIZE = 10000

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
        
        # 워커 설정 및 성능 최적화 설정 가져오기
        try:
            from common.settings import BatchSettings
            self.worker_config = BatchSettings.get_worker_config()
            self.performance_config = BatchSettings.get_performance_optimization_config()
            logger.info(f"워커 설정 로드 완료: {self.worker_config}")
            logger.info(f"성능 최적화 설정 로드 완료: {self.performance_config}")
        except ImportError:
            # 설정 모듈을 가져올 수 없는 경우 기본값 사용
            self.worker_config = {
                "default_workers": 4,
                "max_workers": 8,
                "min_workers": 2,
                "worker_timeout_seconds": 600,
                "worker_memory_limit_mb": 1024,
            }
            self.performance_config = {
                "enable_session_optimization": True,
                "enable_unlogged_staging": True,
                "enable_auto_analyze": True,
                "enable_auto_index": True,
                "enable_streaming_pipe": False,
                "session_parameters": {
                    "synchronous_commit": "off",
                    "statement_timeout": "0",
                    "work_mem": "128MB",
                    "lock_timeout": "300s",
                },
                "batch_size_optimization": {
                    "large_table_threshold": 1000000,
                    "large_table_batch_size": 50000,
                    "medium_table_threshold": 100000,
                    "medium_table_batch_size": 20000,
                    "small_table_batch_size": 10000,
                },
                "parallel_processing": {
                    "max_concurrent_tables": 2,
                    "max_concurrent_chunks": 4,
                    "pool_name": "postgres_copy_pool",
                }
            }
            logger.warning("설정 모듈을 가져올 수 없어 기본 설정을 사용합니다")

    def create_staging_table(self, target_table: str, source_schema: dict[str, Any]) -> str:
        """
        타겟 DB에 스테이징 테이블 생성
        
        Args:
            target_table: 최종 타겟 테이블명 (예: raw_data.sym_v1_sym_coverage)
            source_schema: 소스 테이블 스키마 정보
        
        Returns:
            생성된 스테이징 테이블명
        """
        try:
            # 고유한 스테이징 테이블명 생성
            table_name = target_table.split('.')[-1]
            staging_table_name = f"staging_{table_name}_{int(time.time())}"
            
            # 스키마와 테이블명 분리
            if "." in target_table:
                schema, _ = target_table.split(".", 1)
            else:
                schema = "public"
            
            full_staging_name = f"{schema}.{staging_table_name}"
            
            # 스테이징 테이블 생성 SQL 생성
            create_sql = self._generate_staging_table_sql(full_staging_name, source_schema)
            
            # 타겟 DB에 스테이징 테이블 생성
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_sql)
                    conn.commit()
                    
                    # 스테이징 테이블이 실제로 생성되었는지 확인
                    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{staging_table_name}' AND table_schema = '{schema}')")
                    exists = cursor.fetchone()[0]
                    if not exists:
                        raise Exception(f"스테이징 테이블 {full_staging_name}이 생성되지 않았습니다.")
            
            logger.info(f"스테이징 테이블 생성 완료: {full_staging_name}")
            return full_staging_name
            
        except Exception as e:
            logger.error(f"스테이징 테이블 생성 실패: {target_table}, 오류: {e}")
            raise

    def _generate_staging_table_sql(self, staging_table: str, source_schema: dict[str, Any]) -> str:
        """
        스테이징 테이블 생성 SQL 생성
        
        Args:
            staging_table: 스테이징 테이블명
            source_schema: 소스 테이블 스키마 정보
        
        Returns:
            CREATE TABLE SQL 문
        """
        # 컬럼 정의 생성
        column_definitions = []
        for col in source_schema["columns"]:
            col_name = col["name"]
            col_type = col["type"]

            # PostgreSQL 타입 매핑
            col_type = self._convert_to_postgres_type(col_type, col.get('max_length'))

            nullable = "" if col["nullable"] else " NOT NULL"
            column_definitions.append(f"{col_name} {col_type}{nullable}")

        # UNLOGGED 테이블로 생성 (성능 향상)
        create_sql = f"""
            CREATE UNLOGGED TABLE {staging_table} (
                {', '.join(column_definitions)}
            )
        """
        
        return create_sql

    def copy_data_to_staging(self, source_table: str, staging_table: str, where_condition: str = None) -> dict[str, Any]:
        """
        소스 테이블에서 스테이징 테이블로 데이터 복사
        
        Args:
            source_table: 소스 테이블명
            staging_table: 스테이징 테이블명
            where_condition: WHERE 조건
        
        Returns:
            복사 결과 정보
        """
        try:
            # 1. 소스 데이터 카운트 조회
            source_count = self._get_source_count(source_table, where_condition)
            
            # 2. 스테이징 테이블로 직접 복사 (psql COPY)
            copy_result = self._copy_with_psql_copy(source_table, staging_table, where_condition)
            
            # 3. 복사된 데이터 검증
            staging_count = self._get_staging_count(staging_table)
            
            return {
                'status': 'success',
                'source_count': source_count,
                'staging_count': staging_count,
                'copy_time': copy_result['execution_time']
            }
            
        except Exception as e:
            logger.error(f"스테이징 테이블로 데이터 복사 실패: {str(e)}")
            raise

    def _copy_with_psql_copy(self, source_table: str, staging_table: str, where_condition: str = None) -> dict[str, Any]:
        """
        psql COPY 명령어를 사용한 데이터 복사
        """
        start_time = time.time()
        export_process = None
        import_process = None
        
        try:
            # psql 명령어 사용 가능 여부 확인
            try:
                subprocess.run(['psql', '--version'], capture_output=True, check=True)
                logger.info("psql 명령어 사용 가능 확인됨")
            except (subprocess.CalledProcessError, FileNotFoundError):
                logger.warning("psql 명령어를 찾을 수 없습니다. Python 기반 복사로 전환합니다.")
                return self._copy_with_python_fallback(source_table, staging_table, where_condition)
            
            # 소스 DB 연결 정보 가져오기
            source_conn = self.source_hook.get_conn()
            source_host = source_conn.info.host
            source_port = source_conn.info.port
            source_user = source_conn.info.user
            source_database = source_conn.info.dbname
            source_password = source_conn.info.password
            
            # 타겟 DB 연결 정보 가져오기
            target_conn = self.target_hook.get_conn()
            target_host = target_conn.info.host
            target_port = target_conn.info.port
            target_user = target_conn.info.user
            target_database = target_conn.info.dbname
            target_password = target_conn.info.password
            
            # WHERE 조건이 있는 경우 서브쿼리로 처리
            if where_condition:
                source_query = f"SELECT * FROM {source_table} WHERE {where_condition}"
            else:
                source_query = f"SELECT * FROM {source_table}"
            
            # 환경변수로 비밀번호 설정 (보안 강화)
            env = os.environ.copy()
            env['PGPASSWORD'] = source_password
            
            # COPY 명령어 실행 (소스에서 CSV로 내보내기)
            export_command = [
                'psql', '-h', source_host, '-p', str(source_port), '-U', source_user, '-d', source_database,
                '-c', f"\\copy ({source_query}) TO STDOUT CSV HEADER"
            ]
            
            # 환경변수로 비밀번호 설정 (보안 강화)
            env_target = os.environ.copy()
            env_target['PGPASSWORD'] = target_password
            
            # CSV를 타겟으로 가져오기
            import_command = [
                'psql', '-h', target_host, '-p', str(target_port), '-U', target_user, '-d', target_database,
                '-c', f"\\copy {staging_table} FROM STDIN CSV HEADER"
            ]
            
            # 디버깅을 위한 상세 로깅
            logger.info(f"psql COPY 명령어 실행 시작:")
            logger.info(f"  소스 DB: {source_host}:{source_database} (사용자: {source_user})")
            logger.info(f"  타겟 DB: {target_host}:{target_database} (사용자: {target_user})")
            logger.info(f"  소스 쿼리: {source_query}")
            logger.info(f"  스테이징 테이블: {staging_table}")
            logger.info(f"  export 명령어: {' '.join(export_command)}")
            logger.info(f"  import 명령어: {' '.join(import_command)}")
            
            # 명령어 실행 전 환경 확인
            logger.info(f"  현재 작업 디렉토리: {os.getcwd()}")
            logger.info(f"  PATH 환경변수: {os.environ.get('PATH', 'Not set')}")
            
            # 순차적 실행으로 변경 (파이프라인 대신)
            logger.info("순차적 실행 방식으로 변경...")
            
            # 1단계: export_process 실행
            logger.info("1단계: 소스 데이터 내보내기 시작...")
            export_process = subprocess.Popen(
                export_command, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                env=env
            )
            logger.info(f"export_process 시작됨 (PID: {export_process.pid})")
            
            # export_process 완료 대기
            try:
                export_stdout, export_stderr = export_process.communicate(timeout=300)  # 5분 타임아웃
                logger.info("export_process 완료")
                
                if export_process.returncode != 0:
                    export_stderr_text = export_stderr.decode() if export_stderr else "Unknown error"
                    logger.error(f"export_process 실패 - 종료코드: {export_process.returncode}")
                    logger.error(f"export_process stderr: {export_stderr_text}")
                    raise Exception(f"소스 데이터 내보내기 실패 (코드: {export_process.returncode}): {export_stderr_text}")
                
            except subprocess.TimeoutExpired:
                logger.error("export_process 타임아웃 (5분)")
                export_process.kill()
                raise Exception("소스 데이터 내보내기 타임아웃")
            
            # 2단계: import_process 실행
            logger.info("2단계: 스테이징 테이블로 데이터 가져오기 시작...")
            import_process = subprocess.Popen(
                import_command,
                stdin=subprocess.PIPE,  # export_process의 출력을 직접 전달
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env_target
            )
            logger.info(f"import_process 시작됨 (PID: {import_process.pid})")
            
            # export_process의 출력을 import_process에 전달
            try:
                stdout, stderr = import_process.communicate(
                    input=export_stdout,  # export_process의 출력을 직접 전달
                    timeout=300  # 5분 타임아웃
                )
                logger.info("import_process 완료")
                
            except subprocess.TimeoutExpired:
                logger.error("import_process 타임아웃 (5분)")
                import_process.kill()
                raise Exception("스테이징 테이블로 데이터 가져오기 타임아웃")
            
            # 프로세스 종료 코드 확인 (에러 메시지 수집)
            if import_process.returncode is None:
                logger.error("import_process가 정상적으로 종료되지 않음")
                logger.error(f"import_process 상태: {import_process.poll()}")
                import_stderr = stderr.decode() if stderr else "Unknown error"
                raise Exception(f"스테이징 테이블로 데이터 가져오기 실패: 프로세스가 정상적으로 종료되지 않음 (stderr: {import_stderr})")
            
            if import_process.returncode != 0:
                import_stderr = stderr.decode() if stderr else "Unknown error"
                logger.error(f"import_process 실패 - 종료코드: {import_process.returncode}")
                logger.error(f"import_process stderr: {import_stderr}")
                raise Exception(f"스테이징 테이블로 데이터 가져오기 실패 (코드: {import_process.returncode}): {import_stderr}")
            
            execution_time = time.time() - start_time
            logger.info(f"psql COPY 명령어 실행 성공 - 소요시간: {execution_time:.2f}초")
            
            return {
                'status': 'success',
                'execution_time': execution_time,
                'stdout': stdout.decode() if stdout else "",
                'stderr': stderr.decode() if stderr else ""
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"psql COPY 명령어 실행 실패: {str(e)}")
            raise
            
        finally:
            # 프로세스 정리 보장
            if export_process:
                try:
                    export_process.terminate()
                    export_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    export_process.kill()
                except Exception as cleanup_error:
                    logger.warning(f"export_process 정리 중 오류: {cleanup_error}")
            
            if import_process:
                try:
                    import_process.terminate()
                    import_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    import_process.kill()
                except Exception as cleanup_error:
                    logger.warning(f"import_process 정리 중 오류: {cleanup_error}")

    def _copy_with_python_fallback(self, source_table: str, staging_table: str, where_condition: str = None) -> dict[str, Any]:
        """
        psql 명령어를 사용할 수 없을 때 Python 기반으로 데이터 복사
        """
        start_time = time.time()
        
        try:
            logger.info(f"Python 기반 데이터 복사 시작: {source_table} -> {staging_table}")
            
            # WHERE 조건이 있는 경우 서브쿼리로 처리
            if where_condition:
                source_query = f"SELECT * FROM {source_table} WHERE {where_condition}"
            else:
                source_query = f"SELECT * FROM {source_table}"
            
            # 소스에서 데이터 조회
            with self.source_hook.get_conn() as source_conn:
                with source_conn.cursor() as source_cursor:
                    source_cursor.execute(source_query)
                    columns = [desc[0] for desc in source_cursor.description]
                    
                    # 배치 단위로 데이터 처리
                    batch_size = 10000
                    total_rows = 0
                    
                    while True:
                        rows = source_cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        
                        # 타겟에 데이터 삽입
                        with self.target_hook.get_conn() as target_conn:
                            with target_conn.cursor() as target_cursor:
                                # 첫 번째 배치에서 테이블 생성 (이미 생성되어 있음)
                                if total_rows == 0:
                                    # 컬럼명을 따옴표로 감싸서 안전하게 처리
                                    quoted_columns = [f'"{col}"' for col in columns]
                                    placeholders = ','.join(['%s'] * len(columns))
                                    
                                    # 기존 데이터 삭제
                                    target_cursor.execute(f"DELETE FROM {staging_table}")
                                
                                # 데이터 삽입
                                insert_sql = f"INSERT INTO {staging_table} ({','.join(quoted_columns)}) VALUES ({placeholders})"
                                target_cursor.executemany(insert_sql, rows)
                                target_conn.commit()
                                
                                total_rows += len(rows)
                                logger.info(f"배치 처리 완료: {total_rows}행")
            
            execution_time = time.time() - start_time
            logger.info(f"Python 기반 데이터 복사 완료 - 총 {total_rows}행, 소요시간: {execution_time:.2f}초")
            
            return {
                'status': 'success',
                'execution_time': execution_time,
                'rows_copied': total_rows,
                'method': 'python_fallback'
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Python 기반 데이터 복사 실패: {str(e)}")
            raise

    def _get_source_count(self, source_table: str, where_condition: str = None) -> int:
        """
        소스 테이블의 데이터 개수 조회
        """
        try:
            # 입력값 유효성 검증
            if not source_table:
                raise ValueError("source_table이 비어있습니다")
            
            # 테이블명 유효성 검증
            if not self._is_valid_table_name(source_table):
                raise ValueError(f"유효하지 않은 테이블명: {source_table}")
            
            # WHERE 조건이 있는 경우 안전한 쿼리 구성
            if where_condition:
                # WHERE 조건의 기본적인 유효성 검증 (복잡한 조건은 별도 검증 필요)
                if not isinstance(where_condition, str) or len(where_condition.strip()) == 0:
                    raise ValueError("유효하지 않은 WHERE 조건")
                
                count_sql = f"SELECT COUNT(*) FROM {source_table} WHERE {where_condition}"
            else:
                count_sql = f"SELECT COUNT(*) FROM {source_table}"
            
            with self.source_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(count_sql)
                    count = cursor.fetchone()[0]
                    return count
                    
        except Exception as e:
            logger.error(f"소스 테이블 데이터 개수 조회 실패: {str(e)}")
            raise

    def _get_staging_count(self, staging_table: str) -> int:
        """
        스테이징 테이블의 데이터 개수 조회
        """
        try:
            # 입력값 유효성 검증
            if not staging_table:
                raise ValueError("staging_table이 비어있습니다")
            
            # 테이블명 유효성 검증
            if not self._is_valid_table_name(staging_table):
                raise ValueError(f"유효하지 않은 테이블명: {staging_table}")
            
            count_sql = f"SELECT COUNT(*) FROM {staging_table}"
            
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(count_sql)
                    count = cursor.fetchone()[0]
                    return count
                    
        except Exception as e:
            logger.error(f"스테이징 테이블 데이터 개수 조회 실패: {str(e)}")
            raise

    def merge_from_staging(self, target_table: str, staging_table: str, primary_keys: list[str]) -> dict[str, Any]:
        """
        스테이징 테이블에서 최종 테이블로 데이터 MERGE
        
        Args:
            target_table: 최종 타겟 테이블명
            staging_table: 스테이징 테이블명
            primary_keys: 기본키 리스트
        
        Returns:
            MERGE 결과 정보
        """
        try:
            # 1. MERGE 전 데이터 검증
            staging_count = self._get_staging_count(staging_table)
            target_before_count = self._get_target_count(target_table)
            
            # 2. MERGE 작업 실행
            merge_result = self._execute_merge(target_table, staging_table, primary_keys)
            
            # 3. MERGE 후 데이터 검증
            target_after_count = self._get_target_count(target_table)
            
            return {
                'status': 'success',
                'staging_count': staging_count,
                'target_before_count': target_before_count,
                'target_after_count': target_after_count,
                'merged_rows': target_after_count - target_before_count,
                'merge_time': merge_result['execution_time']
            }
            
        except Exception as e:
            logger.error(f"MERGE 작업 실패: {str(e)}")
            raise

    def _execute_merge(self, target_table: str, staging_table: str, primary_keys: list[str]) -> dict[str, Any]:
        """
        MERGE SQL 실행
        """
        start_time = time.time()
        
        try:
            # 입력값 유효성 검증
            if not target_table or not staging_table:
                raise ValueError("target_table 또는 staging_table이 비어있습니다")
            
            if not primary_keys or not isinstance(primary_keys, list):
                raise ValueError("primary_keys가 비어있거나 리스트가 아닙니다")
            
            # 테이블명 유효성 검증
            if not self._is_valid_table_name(target_table):
                raise ValueError(f"유효하지 않은 target_table: {target_table}")
            
            if not self._is_valid_table_name(staging_table):
                raise ValueError(f"유효하지 않은 staging_table: {staging_table}")
            
            # 기본키 유효성 검증
            for pk in primary_keys:
                if not self._is_valid_field_name(pk):
                    raise ValueError(f"유효하지 않은 기본키: {pk}")
            
            # 기본키 기반 MERGE SQL 생성
            pk_conditions = ' AND '.join([f"t.{pk} = s.{pk}" for pk in primary_keys])
            
            merge_sql = f"""
            BEGIN;
            
            -- 기존 데이터 삭제 (기본키 기준)
            DELETE FROM {target_table} t
            WHERE EXISTS (
                SELECT 1 FROM {staging_table} s
                WHERE {pk_conditions}
            );
            
            -- 새 데이터 삽입
            INSERT INTO {target_table}
            SELECT * FROM {staging_table};
            
            COMMIT;
            """
            
            # MERGE SQL 실행
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(merge_sql)
                    conn.commit()
            
            execution_time = time.time() - start_time
            
            return {
                'status': 'success',
                'execution_time': execution_time
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"MERGE SQL 실행 실패: {str(e)}")
            raise

    def _get_target_count(self, target_table: str) -> int:
        """
        타겟 테이블의 데이터 개수 조회
        """
        try:
            # 입력값 유효성 검증
            if not target_table:
                raise ValueError("target_table이 비어있습니다")
            
            # 테이블명 유효성 검증
            if not self._is_valid_table_name(target_table):
                raise ValueError(f"유효하지 않은 테이블명: {target_table}")
            
            count_sql = f"SELECT COUNT(*) FROM {target_table}"
            
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(count_sql)
                    count = cursor.fetchone()[0]
                    return count
                    
        except Exception as e:
            logger.error(f"타겟 테이블 데이터 개수 조회 실패: {str(e)}")
            raise

    def cleanup_staging_table(self, staging_table: str) -> bool:
        """
        스테이징 테이블 정리
        
        Args:
            staging_table: 정리할 스테이징 테이블명
        
        Returns:
            정리 성공 여부
        """
        try:
            cleanup_sql = f"DROP TABLE IF EXISTS {staging_table}"
            
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(cleanup_sql)
                    conn.commit()
            
            logger.info(f"스테이징 테이블 정리 완료: {staging_table}")
            return True
            
        except Exception as e:
            logger.error(f"스테이징 테이블 정리 실패: {str(e)}")
            raise

    def copy_with_staging_table(
        self, 
        source_table: str, 
        target_table: str, 
        primary_keys: list[str],
        where_condition: str = None
    ) -> dict[str, Any]:
        """
        스테이징 테이블을 사용한 전체 데이터 복사 프로세스
        
        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            primary_keys: 기본키 리스트
            where_condition: WHERE 조건
        
        Returns:
            복사 결과 정보
        """
        staging_table = None
        
        try:
            logger.info(f"스테이징 테이블 방식 데이터 복사 시작: {source_table} -> {target_table}")
            
            # 1. 소스 테이블 스키마 조회
            source_schema = self.db_ops.get_table_schema(source_table)
            if not source_schema:
                raise Exception(f"소스 테이블 {source_table}의 스키마 정보를 가져올 수 없습니다")
            
            # 2. 스테이징 테이블 생성
            staging_table = self.create_staging_table(target_table, source_schema)
            
            # 3. 데이터 복사 (소스 → 스테이징)
            copy_result = self.copy_data_to_staging(source_table, staging_table, where_condition)
            
            # 4. MERGE 작업 (스테이징 → 최종)
            merge_result = self.merge_from_staging(target_table, staging_table, primary_keys)
            
            # 5. 결과 반환
            result = {
                'status': 'success',
                'source_table': source_table,
                'target_table': target_table,
                'staging_table': staging_table,
                'copy_result': copy_result,
                'merge_result': merge_result,
                'total_execution_time': copy_result['copy_time'] + merge_result['merge_time']
            }
            
            logger.info(f"스테이징 테이블 방식 데이터 복사 완료: {result}")
            return result
            
        except Exception as e:
            logger.error(f"스테이징 테이블 방식 데이터 복사 실패: {str(e)}")
            raise
            
        finally:
            # 6. 스테이징 테이블 정리 (항상 실행 보장)
            if staging_table:
                self.cleanup_staging_table(staging_table)



    def _optimize_target_session(self, hook) -> None:
        """
        타겟 세션에서 성능 최적화를 위한 세션 파라미터 설정
        
        Args:
            hook: 타겟 데이터베이스 훅
        """
        # 성능 최적화 설정이 비활성화된 경우 스킵
        if not self.performance_config.get("enable_session_optimization", True):
            logger.info("세션 최적화가 비활성화되어 있습니다.")
            return
            
        try:
            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # 설정에서 세션 파라미터 가져오기
                    session_params = self.performance_config.get("session_parameters", {})
                    
                    # WAL/동기화 비용 절감
                    if "synchronous_commit" in session_params:
                        cursor.execute(f"SET synchronous_commit = {session_params['synchronous_commit']};")
                    
                    # 타임아웃 설정 (태스크 레벨 타임아웃으로 보호)
                    if "statement_timeout" in session_params:
                        cursor.execute(f"SET statement_timeout = '{session_params['statement_timeout']}';")
                    
                    # 조인/정렬을 위한 메모리 설정
                    if "work_mem" in session_params:
                        cursor.execute(f"SET work_mem = '{session_params['work_mem']}';")
                    
                    # 잠금 타임아웃 설정
                    if "lock_timeout" in session_params:
                        cursor.execute(f"SET lock_timeout = '{session_params['lock_timeout']}';")
                    
                    logger.info(f"타겟 세션 최적화 완료: {session_params}")
                    
        except Exception as e:
            logger.warning(f"타겟 세션 최적화 실패 (기본 설정 사용): {e}")

    def _analyze_target_table(self, target_table: str) -> None:
        """
        타겟 테이블에 ANALYZE 실행하여 통계 최신화
        
        Args:
            target_table: 타겟 테이블명
        """
        # 자동 ANALYZE가 비활성화된 경우 스킵
        if not self.performance_config.get("enable_auto_analyze", True):
            logger.info("자동 ANALYZE가 비활성화되어 있습니다.")
            return
            
        try:
            with self.target_hook.get_conn() as conn:
                # CREATE INDEX CONCURRENTLY는 트랜잭션 블록 밖에서 실행되어야 함 → autocommit 활성화
                original_autocommit = getattr(conn, "autocommit", False)
                try:
                    conn.autocommit = True
                except Exception:
                    pass
                with conn.cursor() as cursor:
                    cursor.execute(f"ANALYZE {target_table};")
                    logger.info(f"타겟 테이블 {target_table} ANALYZE 완료")
                    
        except Exception as e:
            logger.warning(f"타겟 테이블 {target_table} ANALYZE 실패: {e}")

    def _check_and_create_indexes(self, table_name: str, primary_keys: list[str]) -> None:
        """
        MERGE 키에 대한 인덱스 존재 여부 점검 및 필요 시 자동 생성
        
        Args:
            table_name: 테이블명
            primary_keys: 기본키 목록
        """
        # 자동 인덱스 생성이 비활성화된 경우 스킵
        if not self.performance_config.get("enable_auto_index", True):
            logger.info("자동 인덱스 생성이 비활성화되어 있습니다.")
            return
            
        try:
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # 기존 인덱스 확인
                    for pk in primary_keys:
                        index_name = f"idx_{table_name.replace('.', '_')}_{pk}"
                        
                        # 인덱스 존재 여부 확인
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT 1 FROM pg_indexes 
                                WHERE tablename = '{table_name.split('.')[-1]}' 
                                AND indexname = '{index_name}'
                            )
                        """)
                        
                        exists = cursor.fetchone()[0]
                        
                        if not exists:
                            # 인덱스가 없으면 CONCURRENTLY로 생성
                            try:
                                create_index_sql = f"CREATE INDEX CONCURRENTLY {index_name} ON {table_name} ({pk})"
                                cursor.execute(create_index_sql)
                                logger.info(f"인덱스 생성 완료: {index_name} on {table_name}({pk})")
                            except Exception as e:
                                logger.warning(f"인덱스 생성 실패: {index_name}, 오류: {e}")
                        else:
                            logger.info(f"인덱스 이미 존재: {index_name}")
                # autocommit 복원
                try:
                    conn.autocommit = original_autocommit
                except Exception:
                    pass
                
        except Exception as e:
            logger.warning(f"인덱스 점검 및 생성 실패: {e}")

    def copy_data_with_streaming_pipe(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str = "incremental_sync",
        incremental_field: str | None = None,
        incremental_field_type: str | None = None,
        custom_where: str | None = None,
        batch_size: int = 10000,
        verified_target_schema: dict[str, Any] | None = None,
        chunk_mode: bool = True,
        enable_checkpoint: bool = True,
        max_retries: int = 3
    ) -> dict[str, Any]:
        """
        스트리밍 파이프를 사용한 데이터 복사 (스테이징 테이블 방식)
        
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
            chunk_mode: 청크 모드 활성화 여부
            enable_checkpoint: 체크포인트 활성화 여부
            max_retries: 최대 재시도 횟수

        Returns:
            복사 결과
        """
        try:
            start_time = time.time()
            
            # 1단계: 소스 테이블 스키마 조회
            source_schema = self.db_ops.get_table_schema(source_table)
            
            # 2단계: WHERE 조건 구성
            where_condition = None
            if custom_where:
                where_condition = custom_where
            elif incremental_field and incremental_field_type:
                # 증분 동기화 조건 구성
                where_condition = self._build_incremental_where_condition(
                    incremental_field, incremental_field_type, target_table
                )
            
            # 3단계: 스테이징 테이블 방식으로 데이터 복사 실행
            logger.info(f"스테이징 테이블 방식으로 데이터 복사 시작: {source_table} -> {target_table}")
            
            staging_result = self.copy_with_staging_table(
                source_table=source_table,
                target_table=target_table,
                primary_keys=primary_keys,
                where_condition=where_condition
            )
            
            total_time = time.time() - start_time
            
            # 스테이징 결과에서 필요한 정보 추출
            copy_result = staging_result.get('copy_result', {})
            exported_rows = copy_result.get('staging_count', 0)
            imported_rows = copy_result.get('target_count', 0)
            
            result = {
                "status": "success",
                "exported_rows": exported_rows,
                "imported_rows": imported_rows,
                "total_execution_time": total_time,
                "message": f"스테이징 테이블 방식으로 데이터 복사 완료: {imported_rows}행",
                "staging_result": staging_result
            }
            
            logger.info(f"스트리밍 파이프를 사용한 데이터 복사 완료: {result}")
            return result
            
        except Exception as e:
            total_time = time.time() - start_time
            error_msg = f"스트리밍 파이프를 사용한 데이터 복사 실패: {e}"
            logger.error(error_msg)
            
            result = {
                "status": "error",
                "exported_rows": 0,
                "imported_rows": 0,
                "total_execution_time": total_time,
                "error": error_msg
            }
            
            return result

    def _stream_data_directly(
        self,
        select_sql: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str,
        verified_target_schema: dict[str, Any] | None,
        source_schema: dict[str, Any]
    ) -> int:
        """
        스트리밍 파이프를 사용하여 소스에서 타겟으로 직접 데이터 복사
        
        Args:
            select_sql: SELECT SQL
            target_table: 타겟 테이블명
            primary_keys: 기본키 목록
            sync_mode: 동기화 모드
            verified_target_schema: 검증된 타겟 스키마
            source_schema: 소스 스키마

        Returns:
            복사된 행 수
        """
        try:
            # 1단계: 스테이징 테이블 생성 (UNLOGGED)
            staging_table = self.create_staging_table(target_table, verified_target_schema or source_schema)
            
            # 2단계: psql 파이프라인을 사용한 스트리밍 복사
            imported_rows = self._copy_with_psql_pipe(select_sql, staging_table, source_schema)
            
            if imported_rows == 0:
                logger.warning("스트리밍 복사 실패")
                return 0
            
            # 3단계: MERGE 작업 수행
            merge_result = self.execute_merge_operation(
                staging_table, target_table, primary_keys, sync_mode
            )
            
            if merge_result["status"] != "success":
                logger.error(f"MERGE 작업 실패: {merge_result['message']}")
                # 실패 시 스테이징 테이블 정리 시도
                self.cleanup_staging_table(staging_table)
                return 0
            
            # 4단계: 성능 최적화
            try:
                self._analyze_target_table(target_table)
                self._check_and_create_indexes(target_table, primary_keys)
            except Exception as e:
                logger.warning(f"성능 최적화 실패: {e}")
            
            return imported_rows
            
        except Exception as e:
            logger.error(f"스트리밍 데이터 복사 실패: {e}")
            return 0

    def _copy_with_psql_pipe(
        self,
        select_sql: str,
        temp_table: str,
        source_schema: dict[str, Any]
    ) -> int:
        """
        psql 파이프라인을 사용한 스트리밍 복사
        
        Args:
            select_sql: SELECT SQL
            temp_table: 임시 테이블명
            source_schema: 소스 스키마

        Returns:
            복사된 행 수
        """
        try:
            import subprocess
            import os
            
            # 환경 변수 설정
            source_env = os.environ.copy()
            target_env = os.environ.copy()
            
            # 소스 DB 연결 정보 설정
            source_conn = self.source_hook.get_connection(self.db_ops.source_conn_id)
            source_env.update({
                'PGPASSWORD': source_conn.password,
                'PGHOST': source_conn.host,
                'PGPORT': str(source_conn.port),
                'PGUSER': source_conn.login,
                'PGDATABASE': source_conn.schema
            })
            
            # 타겟 DB 연결 정보 설정
            target_conn = self.target_hook.get_connection(self.db_ops.target_conn_id)
            target_env.update({
                'PGPASSWORD': target_conn.password,
                'PGHOST': target_conn.host,
                'PGPORT': str(target_conn.port),
                'PGUSER': target_conn.login,
                'PGDATABASE': target_conn.schema
            })
            
            # 소스에서 데이터 추출 (STDOUT으로)
            source_cmd = [
                'psql',
                '-h', source_conn.host,
                '-p', str(source_conn.port),
                '-U', source_conn.login,
                '-d', source_conn.schema,
                '-c', f"\\copy ({select_sql}) TO STDOUT WITH CSV HEADER"
            ]
            
            # 타겟으로 데이터 적재 (STDIN에서)
            target_cmd = [
                'psql',
                '-h', target_conn.host,
                '-p', str(target_conn.port),
                '-U', target_conn.login,
                '-d', target_conn.schema,
                '-c', f"\\copy {temp_table} FROM STDIN WITH CSV HEADER"
            ]
            
            logger.info(f"스트리밍 파이프 시작: {temp_table}")
            
            # 파이프라인 실행 (스트리밍, 대용량 버퍼링 회피)
            source_process = subprocess.Popen(
                source_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=source_env
            )

            target_process = subprocess.Popen(
                target_cmd,
                stdin=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=target_env
            )

            # 소스 stdout을 타겟 stdin으로 스트리밍 전달
            assert source_process.stdout is not None
            assert target_process.stdin is not None
            
            try:
                # 청크 단위로 데이터 스트리밍
                for chunk in iter(lambda: source_process.stdout.read(1024 * 1024), b""):
                    if chunk:  # 빈 청크가 아닌 경우에만 처리
                        target_process.stdin.write(chunk)
                        target_process.stdin.flush()  # 즉시 플러시
                
                # 소스 프로세스 완료 대기
                source_process.wait()
                
                # 타겟 stdin 닫기 (데이터 전송 완료 후)
                target_process.stdin.close()
                
                # 타겟 프로세스 완료 대기
                target_process.wait()
                
            except Exception as stream_error:
                logger.error(f"스트리밍 중 오류: {stream_error}")
                # 프로세스 정리
                if source_process.poll() is None:
                    source_process.terminate()
                if target_process.poll() is None:
                    target_process.terminate()
                raise stream_error

            # 에러 수집
            source_stderr = source_process.stderr.read() if source_process.stderr else b""
            target_stderr = target_process.stderr.read() if target_process.stderr else b""
            
            # 결과 확인
            if source_process.returncode != 0:
                logger.error(f"소스 추출 실패: {source_stderr.decode()}")
                return 0
                
            if target_process.returncode != 0:
                logger.error(f"타겟 적재 실패: {target_stderr.decode()}")
                return 0
            
            # 복사된 행 수 확인
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
                    row_count = cursor.fetchone()[0]
            
            logger.info(f"스트리밍 파이프 완료: {temp_table}, {row_count}행")
            return row_count
            
        except Exception as e:
            logger.error(f"psql 파이프라인 복사 실패: {e}")
            return 0

    def copy_data_with_binary_format(
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
        바이너리 포맷을 사용한 데이터 복사 (CSV 대비 성능 향상)
        
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
            
            # 4단계: 바이너리 포맷으로 직접 복사
            imported_rows = self._copy_with_binary_format(
                select_sql, target_table, primary_keys, sync_mode, 
                verified_target_schema, source_schema
            )
            
            total_time = time.time() - start_time
            
            result = {
                "status": "success",
                "exported_rows": row_count,
                "imported_rows": imported_rows,
                "total_execution_time": total_time,
                "message": f"바이너리 포맷으로 데이터 복사 완료: {imported_rows}행"
            }
            
            logger.info(f"바이너리 포맷을 사용한 데이터 복사 완료: {result}")
            return result
            
        except Exception as e:
            total_time = time.time() - start_time
            error_msg = f"바이너리 포맷을 사용한 데이터 복사 실패: {e}"
            logger.error(error_msg)
            
            result = {
                "status": "error",
                "exported_rows": 0,
                "imported_rows": 0,
                "total_execution_time": total_time,
                "error": error_msg
            }
            
            return result

    def _copy_with_binary_format(
        self,
        select_sql: str,
        target_table: str,
        primary_keys: list[str],
        sync_mode: str,
        verified_target_schema: dict[str, Any] | None,
        source_schema: dict[str, Any]
    ) -> int:
        """
        바이너리 포맷을 사용하여 소스에서 타겟으로 직접 데이터 복사
        
        Args:
            select_sql: SELECT SQL
            target_table: 타겟 테이블명
            primary_keys: 기본키 목록
            sync_mode: 동기화 모드
            verified_target_schema: 검증된 타겟 스키마
            source_schema: 소스 스키마

        Returns:
            복사된 행 수
        """
        try:
            # 1단계: 스테이징 테이블 생성 (UNLOGGED)
            staging_table = self.create_staging_table(target_table, verified_target_schema or source_schema)
            
            # 2단계: psql 바이너리 파이프라인을 사용한 스트리밍 복사
            imported_rows = self._copy_with_psql_binary_pipe(select_sql, staging_table, source_schema)
            
            if imported_rows == 0:
                logger.warning("바이너리 복사 실패")
                return 0
            
            # 3단계: MERGE 작업 수행
            merge_result = self.execute_merge_operation(
                staging_table, target_table, primary_keys, sync_mode
            )
            
            if merge_result["status"] != "success":
                logger.error(f"MERGE 작업 실패: {merge_result['message']}")
                return 0
            
            # 4단계: 성능 최적화
            try:
                self._analyze_target_table(target_table)
                self._check_and_create_indexes(target_table, primary_keys)
            except Exception as e:
                logger.warning(f"성능 최적화 실패: {e}")
            
            return imported_rows
            
        except Exception as e:
            logger.error(f"바이너리 데이터 복사 실패: {e}")
            return 0

    def _copy_with_psql_binary_pipe(
        self,
        select_sql: str,
        temp_table: str,
        source_schema: dict[str, Any]
    ) -> int:
        """
        psql 바이너리 파이프라인을 사용한 스트리밍 복사
        
        Args:
            select_sql: SELECT SQL
            temp_table: 임시 테이블명
            source_schema: 소스 스키마

        Returns:
            복사된 행 수
        """
        try:
            import subprocess
            import os
            
            # 환경 변수 설정
            source_env = os.environ.copy()
            target_env = os.environ.copy()
            
            # 소스 DB 연결 정보 설정
            source_conn = self.source_hook.get_connection(self.db_ops.source_conn_id)
            source_env.update({
                'PGPASSWORD': source_conn.password,
                'PGHOST': source_conn.host,
                'PGPORT': str(source_conn.port),
                'PGUSER': source_conn.login,
                'PGDATABASE': source_conn.schema
            })
            
            # 타겟 DB 연결 정보 설정
            target_conn = self.target_hook.get_connection(self.db_ops.target_conn_id)
            target_env.update({
                'PGPASSWORD': target_conn.password,
                'PGHOST': target_conn.host,
                'PGPORT': str(target_conn.port),
                'PGUSER': target_conn.login,
                'PGDATABASE': target_conn.schema
            })
            
            # 소스에서 데이터 추출 (바이너리 포맷, STDOUT으로)
            source_cmd = [
                'psql',
                '-h', source_conn.host,
                '-p', str(source_conn.port),
                '-U', source_conn.login,
                '-d', source_conn.schema,
                '-c', f"\\copy ({select_sql}) TO STDOUT WITH (FORMAT binary)"
            ]
            
            # 타겟으로 데이터 적재 (바이너리 포맷, STDIN에서)
            target_cmd = [
                'psql',
                '-h', target_conn.host,
                '-p', str(target_conn.port),
                '-U', target_conn.login,
                '-d', target_conn.schema,
                '-c', f"\\copy {temp_table} FROM STDIN WITH (FORMAT binary)"
            ]
            
            logger.info(f"바이너리 스트리밍 파이프 시작: {temp_table}")
            
            # 파이프라인 실행
            source_process = subprocess.Popen(
                source_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=source_env
            )
            
            target_process = subprocess.Popen(
                target_cmd,
                stdin=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=target_env
            )
            
            # 데이터 스트리밍 (대용량 버퍼링 회피)
            assert source_process.stdout is not None
            assert target_process.stdin is not None
            
            try:
                # 청크 단위로 데이터 스트리밍
                for chunk in iter(lambda: source_process.stdout.read(1024 * 1024), b""):
                    if chunk:  # 빈 청크가 아닌 경우에만 처리
                        target_process.stdin.write(chunk)
                        target_process.stdin.flush()  # 즉시 플러시
                
                # 소스 프로세스 완료 대기
                source_process.wait()
                
                # 타겟 stdin 닫기 (데이터 전송 완료 후)
                target_process.stdin.close()
                
                # 타겟 프로세스 완료 대기
                target_process.wait()
                
            except Exception as stream_error:
                logger.error(f"바이너리 스트리밍 중 오류: {stream_error}")
                # 프로세스 정리
                if source_process.poll() is None:
                    source_process.terminate()
                if target_process.poll() is None:
                    target_process.terminate()
                raise stream_error

            # 에러 수집
            source_stderr = source_process.stderr.read() if source_process.stderr else b""
            target_stderr = target_process.stderr.read() if target_process.stderr else b""
            
            # 결과 확인
            if source_process.returncode != 0:
                logger.error(f"소스 바이너리 추출 실패: {source_stderr.decode()}")
                return 0
                
            if target_process.returncode != 0:
                logger.error(f"타겟 바이너리 적재 실패: {target_stderr.decode()}")
                return 0
            
            # 복사된 행 수 확인
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
                    row_count = cursor.fetchone()[0]
            
            logger.info(f"바이너리 스트리밍 파이프 완료: {temp_table}, {row_count}행")
            return row_count
            
        except Exception as e:
            logger.error(f"psql 바이너리 파이프라인 복사 실패: {e}")
            return 0

    def _export_to_csv_chunked(
        self, 
        table_name: str, 
        csv_path: str, 
        where_clause: str | None, 
        batch_size: int, 
        order_by_field: str | None, 
        enable_checkpoint: bool = True, 
        max_retries: int = 3
    ) -> int:
        """
        세션 관리와 트랜잭션 기반 청크 처리 (메모리 누적 없음)
        """
        try:
            # 전체 행 수 조회
            total_count = self.db_ops.get_table_row_count(table_name, where_clause=where_clause)
            
            if total_count == 0:
                logger.warning(f"테이블 {table_name}에 데이터가 없습니다.")
                return 0

            # 체크포인트에서 복구 시도
            start_offset, total_exported = 0, 0
            if enable_checkpoint:
                start_offset, total_exported = self._resume_from_checkpoint(table_name, csv_path)
            
            # 성능 모니터링 변수
            session_refresh_interval = 200  # 200개 청크마다 세션 갱신 (빈도 감소)
            chunk_count = 0
            current_batch_size = batch_size
            performance_metrics = {
                'start_time': time.time(),
                'chunk_processing_times': [],
                'memory_usage_history': [],
                'session_refresh_count': 0
            }
            
            # 테이블 컬럼 정보 가져오기
            columns = self._get_table_columns(table_name)
            
            with open(csv_path, 'a' if start_offset > 0 else 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # 헤더는 처음에만 쓰기
                if start_offset == 0:
                    writer.writerow(columns)
                
                offset = start_offset
                
                while offset < total_count:
                    chunk_start_time = time.time()
                    try:
                        # 세션 상태 확인 및 갱신 (더 효율적으로)
                        if chunk_count % session_refresh_interval == 0:
                            # 세션 상태가 실제로 불량한 경우에만 갱신
                            if not self._check_session_health():
                                self._refresh_database_session()
                                logger.info(f"데이터베이스 세션 갱신 (청크 {chunk_count})")
                                performance_metrics['session_refresh_count'] += 1
                            else:
                                logger.debug(f"세션 상태 정상, 갱신 생략 (청크 {chunk_count})")
                        
                        # 트랜잭션 기반 청크 처리
                        batch_result = self._process_single_chunk_with_transaction(
                            table_name, where_clause, current_batch_size, offset, order_by_field, writer, max_retries
                        )
                        
                        if batch_result['success']:
                            # 성능 메트릭 수집
                            chunk_time = time.time() - chunk_start_time
                            performance_metrics['chunk_processing_times'].append(chunk_time)
                            
                            total_exported += batch_result['rows_processed']
                            offset += current_batch_size
                            chunk_count += 1
                            
                            # 체크포인트 저장
                            if enable_checkpoint:
                                self._save_checkpoint(table_name, offset, total_exported, csv_path)
                            
                            # 진행률 로깅
                            logger.info(f"청크 처리 진행률: {total_exported}/{total_count}")
                            
                            # 메모리 사용량 모니터링
                            memory_usage = self._check_memory_usage()
                            performance_metrics['memory_usage_history'].append(memory_usage)
                            
                            # 배치 크기 동적 조정
                            if chunk_count % 50 == 0:  # 50개 청크마다 조정 (빈도 감소)
                                new_batch_size = self._optimize_batch_size_dynamically(
                                    current_batch_size, memory_usage, chunk_time, self._check_session_health()
                                )
                                if new_batch_size != current_batch_size:
                                    logger.info(f"배치 크기 동적 조정: {current_batch_size} → {new_batch_size}")
                                    current_batch_size = new_batch_size
                            
                            # 성능 모니터링 로깅
                            if chunk_count % 500 == 0:  # 500개 청크마다 로깅 (빈도 감소)
                                self._log_performance_metrics(performance_metrics, chunk_count, total_exported, total_count)
                        else:
                                                        # 청크 실패 시 오류 처리
                            error_action = self._handle_chunk_error(batch_result['error'], offset, current_batch_size)
                            if error_action == 'skip':
                                offset += current_batch_size
                                logger.warning(f"청크 건너뛰기 (offset {offset})")
                            elif error_action == 'retry':
                                continue  # 재시도
                            else:
                                raise Exception(f"치명적 오류 발생: {batch_result['error']}")
                        
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_error:
                        # 데이터베이스 연결 오류 시 세션 재생성
                        logger.error(f"데이터베이스 오류 발생: {db_error}")
                        self._refresh_database_session()
                        
                        # 실패한 청크 재시도
                        continue
                        
                    except Exception as chunk_error:
                        # 기타 오류 시 로깅 및 재시도
                        logger.error(f"청크 처리 오류 (offset {offset}): {chunk_error}")
                        error_action = self._handle_chunk_error(chunk_error, offset, current_batch_size)
                        if error_action == 'skip':
                            offset += current_batch_size
                        elif error_action == 'retry':
                                continue
                        else:
                            raise chunk_error
            
            # 체크포인트 정리
            if enable_checkpoint:
                self._cleanup_checkpoint(csv_path)
            
            # 최종 성능 리포트
            total_time = time.time() - performance_metrics['start_time']
            self._log_final_performance_report(performance_metrics, total_exported, total_time)
            
            logger.info(f"청크 방식 CSV 내보내기 완료: {table_name} -> {csv_path}, 총 {total_exported}행")
            return total_exported
            
        except Exception as e:
            logger.error(f"청크 방식 CSV 내보내기 실패: {e}")
            raise

    def _export_to_csv_legacy(
        self, 
        table_name: str, 
        csv_path: str, 
        where_clause: str | None, 
        batch_size: int, 
        order_by_field: str | None
    ) -> int:
        """
        기존 방식의 CSV 내보내기 (메모리 누적 방식)
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

    def _get_table_columns(self, table_name: str) -> list[str]:
        """
        테이블의 컬럼명 목록을 가져옵니다.
        """
        try:
            # 스키마와 테이블명 분리
            if "." in table_name:
                schema, table = table_name.split(".", 1)
            else:
                schema = "public"
                table = table_name

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
                logger.info(f"테이블 {table_name}의 컬럼명을 가져왔습니다: {columns}")
                return columns
            else:
                raise Exception(f"테이블 {table_name}에서 컬럼명을 찾을 수 없습니다.")
                
        except Exception as e:
            logger.error(f"테이블 컬럼명 가져오기 실패: {e}")
            raise

    def _refresh_database_session(self):
        """
        데이터베이스 세션 상태 확인 및 갱신 (개선된 버전)
        """
        try:
            logger.info("데이터베이스 세션 상태 확인 및 갱신 시작")
            
            # 현재 세션 상태 확인
            if hasattr(self, 'db_ops') and hasattr(self.db_ops, 'connection'):
                # 연결 상태 확인
                if self.db_ops.connection.closed:
                    logger.warning("데이터베이스 연결이 끊어짐, 재연결 시도")
                    self.db_ops.reconnect()
                    logger.info("데이터베이스 재연결 완료")
                else:
                    # 간단한 쿼리로 세션 상태 확인
                    try:
                        self.db_ops.execute_query("SELECT 1")
                        logger.debug("세션 상태 정상")
                    except Exception as query_error:
                        logger.warning(f"세션 상태 확인 쿼리 실패: {query_error}")
                        # 쿼리 실패 시 재연결 시도
                        logger.info("세션 재연결 시도")
                        self.db_ops.reconnect()
            else:
                logger.warning("데이터베이스 연결 객체를 찾을 수 없음")
                
        except Exception as e:
            logger.error(f"세션 상태 확인 실패: {e}")
            # 강제 재연결
            try:
                logger.info("강제 재연결 시도")
                self.db_ops.reconnect()
                logger.info("강제 재연결 완료")
            except Exception as reconnect_error:
                logger.error(f"재연결 실패: {reconnect_error}")
                # 재연결 실패 시 일정 시간 대기 후 재시도
                logger.info("5초 후 재연결 재시도")
                time.sleep(5)
                try:
                    self.db_ops.reconnect()
                    logger.info("지연 재연결 성공")
                except Exception as final_error:
                    logger.error(f"최종 재연결 실패: {final_error}")
                    raise Exception(f"데이터베이스 연결 복구 실패: {final_error}")

    def _check_session_health(self) -> bool:
        """
        세션 상태 종합 점검 (개선된 버전)
        """
        try:
            # 연결 상태 확인
            if hasattr(self, 'db_ops') and hasattr(self.db_ops, 'connection'):
                if self.db_ops.connection.closed:
                    logger.debug("세션 상태: 연결이 끊어짐")
                    return False
                
                # 간단한 쿼리로 세션 상태 확인 (타임아웃 설정)
                try:
                    # 더 빠른 쿼리로 세션 상태 확인
                    self.db_ops.execute_query("SELECT 1", timeout=5)
                    logger.debug("세션 상태: 정상")
                    return True
                except Exception as query_error:
                    # 특정 에러는 세션 상태와 무관할 수 있음
                    if "timeout" in str(query_error).lower() or "connection" in str(query_error).lower():
                        logger.debug(f"세션 상태: 연결 문제 - {query_error}")
                        return False
                    else:
                        # 다른 에러는 세션 상태와 무관할 수 있음
                        logger.debug(f"세션 상태: 쿼리 에러 (세션 상태와 무관) - {query_error}")
                        return True  # 세션은 정상으로 간주
            else:
                logger.debug("세션 상태: 연결 객체 없음")
                return False
                
        except Exception as e:
            logger.error(f"세션 상태 점검 실패: {e}")
            return False

    def _check_memory_usage(self) -> float:
        """
        메모리 사용량 모니터링 및 관리
        """
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            # 상수로 정의된 메모리 임계값 사용
            if memory_mb > self.MemoryThresholds.EMERGENCY_MB:
                logger.critical(f"메모리 사용량 긴급 상황: {memory_mb:.1f}MB (임계값: {self.MemoryThresholds.EMERGENCY_MB}MB)")
                self._force_memory_cleanup()
            elif memory_mb > self.MemoryThresholds.CRITICAL_MB:
                logger.error(f"메모리 사용량 위험: {memory_mb:.1f}MB (임계값: {self.MemoryThresholds.CRITICAL_MB}MB)")
                self._force_memory_cleanup()
            elif memory_mb > self.MemoryThresholds.WARNING_MB:
                logger.warning(f"메모리 사용량 높음: {memory_mb:.1f}MB (임계값: {self.MemoryThresholds.WARNING_MB}MB)")
                gc.collect()
            
            return memory_mb
            
        except Exception as e:
            logger.error(f"메모리 사용량 체크 실패: {e}")
            return 0.0

    @contextmanager
    def _create_chunk_context(self, table_name: str, csv_path: str):
        """
        청크 처리 컨텍스트 매니저
        
        Args:
            table_name: 처리할 테이블명
            csv_path: CSV 파일 경로
        """
        context = {
            "start_time": time.time(),
            "table_name": table_name,
            "csv_path": csv_path,
            "chunks_processed": 0,
            "total_rows": 0,
            "memory_usage_history": [],
            "performance_metrics": {}
        }
        
        try:
            logger.info(f"청크 처리 컨텍스트 시작: {table_name}")
            yield context
        except Exception as e:
            logger.error(f"청크 처리 중 오류 발생: {table_name}, 오류: {e}")
            # 컨텍스트 정보를 로그에 기록
            logger.error(f"컨텍스트 상태: {context}")
            raise
        finally:
            # 처리 완료 후 정리 작업
            execution_time = time.time() - context["start_time"]
            logger.info(f"청크 처리 컨텍스트 종료: {table_name}, "
                       f"처리된 청크: {context['chunks_processed']}, "
                       f"총 행 수: {context['total_rows']}, "
                       f"실행 시간: {execution_time:.2f}초")
            
            # 메모리 정리
            if context["memory_usage_history"]:
                avg_memory = sum(context["memory_usage_history"]) / len(context["memory_usage_history"])
                logger.info(f"평균 메모리 사용량: {avg_memory:.1f}MB")

    def _force_memory_cleanup(self):
        """
        강제 메모리 정리 (개선된 버전)
        """
        try:
            logger.info("강제 메모리 정리 시작")
            
            # 1단계: 가비지 컬렉션 강제 실행
            collected = gc.collect()
            logger.info(f"가비지 컬렉션 완료: {collected}개 객체 정리")
            
            # 2단계: 메모리 사용량 확인
            process = psutil.Process()
            before_memory = process.memory_info().rss / 1024 / 1024
            
            # 3단계: Python 객체 참조 정리 (안전하게)
            cleared_count = 0
            for obj in gc.get_objects():
                try:
                    if hasattr(obj, '__dict__') and obj.__dict__:
                        # 중요한 시스템 객체는 건너뛰기
                        if not obj.__class__.__name__.startswith('_'):
                            obj.__dict__.clear()
                            cleared_count += 1
                except Exception:
                    continue
            
            logger.info(f"객체 참조 정리 완료: {cleared_count}개 객체")
            
            # 4단계: 추가 가비지 컬렉션
            gc.collect()
            
            # 5단계: 메모리 사용량 재확인
            after_memory = process.memory_info().rss / 1024 / 1024
            memory_reduced = before_memory - after_memory
            
            logger.info(f"강제 메모리 정리 완료: {before_memory:.1f}MB → {after_memory:.1f}MB (감소: {memory_reduced:.1f}MB)")
            
        except Exception as e:
            logger.error(f"강제 메모리 정리 실패: {e}")
            # 실패해도 기본 가비지 컬렉션은 수행
            try:
                gc.collect()
                logger.info("기본 가비지 컬렉션 수행")
            except Exception:
                pass

    def _process_single_chunk_with_transaction(
        self, 
        table_name: str, 
        where_clause: str | None, 
        batch_size: int, 
        offset: int, 
        order_by_field: str | None, 
        writer: csv.writer, 
        max_retries: int = 3
    ) -> dict:
        """
        트랜잭션 기반 청크 처리 (개선된 버전)
        """
        retry_count = 0
        start_time = time.time()
        
        while retry_count < max_retries:
            try:
                # 트랜잭션 시작
                with self.source_hook.get_conn() as conn:
                    # 트랜잭션 설정
                    conn.autocommit = False
                    
                    with conn.cursor() as cursor:
                        # 청크 데이터 조회
                        query = self._build_chunk_query(table_name, where_clause, batch_size, offset, order_by_field)
                        
                        # 쿼리 실행 시간 측정
                        query_start = time.time()
                        cursor.execute(query)
                        query_time = time.time() - query_start
                        
                        # 데이터 처리 및 CSV 쓰기
                        rows_processed = 0
                        csv_write_start = time.time()
                        
                        for row in cursor:
                            writer.writerow(row)
                            rows_processed += 1
                        
                        csv_write_time = time.time() - csv_write_start
                        
                        # 트랜잭션 커밋
                        commit_start = time.time()
                        conn.commit()
                        commit_time = time.time() - commit_start
                        
                        # 성능 로깅
                        total_time = time.time() - start_time
                        logger.debug(f"청크 처리 성공: {rows_processed}행, 쿼리: {query_time:.2f}초, CSV: {csv_write_time:.2f}초, 커밋: {commit_time:.2f}초, 총: {total_time:.2f}초")
                        
                        return {
                            'rows_processed': rows_processed,
                            'success': True,
                            'error': None,
                            'performance': {
                                'query_time': query_time,
                                'csv_write_time': csv_write_time,
                                'commit_time': commit_time,
                                'total_time': total_time
                            }
                        }
                        
            except Exception as e:
                # 트랜잭션 롤백
                try:
                    if 'conn' in locals():
                        conn.rollback()
                        logger.debug("트랜잭션 롤백 완료")
                except Exception as rollback_error:
                    logger.warning(f"트랜잭션 롤백 실패: {rollback_error}")
                
                retry_count += 1
                
                if retry_count >= max_retries:
                    logger.error(f"청크 처리 최대 재시도 횟수 초과: {e}")
                    return {
                        'rows_processed': 0,
                        'success': False,
                        'error': str(e),
                        'retry_count': retry_count
                    }
                
                # 지수 백오프 + 지터 추가
                backoff_time = min(2 ** retry_count + (time.time() % 1), 60)  # 최대 60초
                logger.warning(f"청크 처리 재시도 {retry_count}/{max_retries}: {e}, {backoff_time:.1f}초 후 재시도")
                time.sleep(backoff_time)

    def _build_chunk_query(
        self, 
        table_name: str, 
        where_clause: str | None, 
        batch_size: int, 
        offset: int, 
        order_by_field: str | None
    ) -> str:
        """
        청크 쿼리 구성
        """
        # ORDER BY 절 구성
        if order_by_field:
            order_by_clause = f"ORDER BY {order_by_field}"
        else:
            order_by_clause = "ORDER BY 1"
        
        # WHERE 절 구성
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
        
        return query

    def _handle_chunk_error(self, error: str, offset: int, batch_size: int) -> str:
        """
        청크 오류 처리 및 데이터 무결성 보장 (개선된 버전)
        """
        try:
            # 오류 타입 확인
            if isinstance(error, str):
                # 문자열 오류인 경우 타입 추론
                if "connection" in error.lower() or "timeout" in error.lower():
                    error_type = "connection_error"
                elif "data" in error.lower() or "integrity" in error.lower():
                    error_type = "data_error"
                elif "internal" in error.lower() or "server" in error.lower():
                    error_type = "internal_error"
                else:
                    error_type = "unknown_error"
            else:
                # 실제 예외 객체인 경우
                error_type = type(error).__name__
            
            logger.info(f"청크 오류 분석: 타입={error_type}, offset={offset}, 오류={error}")
            
            # 오류 타입별 처리 전략
            if error_type in ["OperationalError", "InterfaceError", "connection_error"]:
                # 데이터베이스 연결 오류 - 재시도 가능
                logger.warning(f"데이터베이스 연결 오류로 인한 청크 실패 (offset {offset}), 재시도 예정")
                return 'retry'
                
            elif error_type in ["DataError", "IntegrityError", "data_error"]:
                # 데이터 오류 - 재시도 불가능, 건너뛰기
                logger.error(f"데이터 오류로 인한 청크 실패 (offset {offset}), 건너뛰기")
                return 'skip'
                
            elif error_type in ["InternalError", "internal_error"]:
                # 내부 오류 - 재시도 가능
                logger.warning(f"내부 오류로 인한 청크 실패 (offset {offset}), 재시도 예정")
                return 'retry'
                
            elif error_type in ["TimeoutError", "timeout"]:
                # 타임아웃 오류 - 재시도 가능
                logger.warning(f"타임아웃 오류로 인한 청크 실패 (offset {offset}), 재시도 예정")
                return 'retry'
                
            elif error_type in ["MemoryError", "memory"]:
                # 메모리 오류 - 강제 정리 후 재시도
                logger.error(f"메모리 오류로 인한 청크 실패 (offset {offset}), 메모리 정리 후 재시도")
                self._force_memory_cleanup()
                return 'retry'
                
            else:
                # 기타 오류 - 로깅 후 재시도
                logger.error(f"예상치 못한 오류로 인한 청크 실패 (offset {offset}): {error}")
                return 'retry'
                
        except Exception as e:
            logger.error(f"오류 처리 중 예외 발생: {e}")
            return 'retry'  # 안전하게 재시도

    def _save_checkpoint(self, table_name: str, offset: int, total_exported: int, csv_path: str):
        """
        청크 처리 진행 상황 저장 (개선된 버전)
        """
        try:
            # 메모리 사용량 및 성능 정보 추가
            memory_usage = self._check_memory_usage()
            
            checkpoint_data = {
                'table_name': table_name,
                'offset': offset,
                'total_exported': total_exported,
                'csv_path': csv_path,
                'timestamp': pd.Timestamp.now().isoformat(),
                'status': 'in_progress',
                'checksum': self._calculate_csv_checksum(csv_path),
                'memory_usage_mb': memory_usage,
                'system_info': {
                    'python_version': f"{os.sys.version_info.major}.{os.sys.version_info.minor}.{os.sys.version_info.micro}",
                    'platform': os.sys.platform,
                    'process_id': os.getpid()
                }
            }
            
            checkpoint_file = f"{csv_path}.checkpoint"
            
            # 임시 파일에 먼저 쓰기 (원자성 보장)
            temp_checkpoint_file = f"{checkpoint_file}.tmp"
            with open(temp_checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            # 원자적 이동
            os.rename(temp_checkpoint_file, checkpoint_file)
            
            logger.debug(f"체크포인트 저장: offset {offset}, 처리된 행 {total_exported}, 메모리 {memory_usage:.1f}MB")
            
        except Exception as e:
            logger.error(f"체크포인트 저장 실패: {e}")
            # 체크포인트 저장 실패 시에도 계속 진행
            logger.warning("체크포인트 저장 실패했지만 처리 계속 진행")

    def _resume_from_checkpoint(self, table_name: str, csv_path: str) -> tuple[int, int]:
        """
        체크포인트에서 복구 (개선된 버전)
        """
        checkpoint_file = f"{csv_path}.checkpoint"
        
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                
                if checkpoint_data['status'] == 'in_progress':
                    # 체크포인트 정보 로깅
                    logger.info(f"체크포인트 발견: {checkpoint_data['table_name']}, offset: {checkpoint_data['offset']}, 처리된 행: {checkpoint_data['total_exported']}")
                    
                    # 체크섬 검증
                    if self._verify_checkpoint_integrity(checkpoint_data, csv_path):
                        # 체크포인트 유효성 추가 검증
                        if self._validate_checkpoint_data(checkpoint_data):
                            logger.info(f"체크포인트에서 복구: {checkpoint_data['offset']}부터 시작")
                            
                            # 체크포인트 백업 생성
                            self._backup_checkpoint(checkpoint_file)
                            
                            return checkpoint_data['offset'], checkpoint_data['total_exported']
                        else:
                            logger.warning("체크포인트 데이터 유효성 검증 실패, 처음부터 시작")
                            return 0, 0
                    else:
                        logger.warning("체크포인트 무결성 검증 실패, 처음부터 시작")
                        return 0, 0
                else:
                    logger.info(f"체크포인트 상태: {checkpoint_data['status']}")
                    if checkpoint_data['status'] == 'completed':
                        logger.info("이미 완료된 작업입니다.")
                    return 0, 0
                    
            except Exception as e:
                logger.error(f"체크포인트 읽기 실패: {e}")
                # 손상된 체크포인트 파일 백업
                try:
                    corrupted_file = f"{checkpoint_file}.corrupted"
                    os.rename(checkpoint_file, corrupted_file)
                    logger.info(f"손상된 체크포인트 파일을 {corrupted_file}로 백업")
                except Exception:
                    pass
                return 0, 0
        
        return 0, 0

    def _verify_checkpoint_integrity(self, checkpoint_data: dict, csv_path: str) -> bool:
        """
        체크포인트 무결성 검증 (개선된 버전)
        """
        try:
            if not os.path.exists(csv_path):
                logger.warning("CSV 파일이 존재하지 않음")
                return False
            
            # 파일 크기 확인
            file_size = os.path.getsize(csv_path)
            if file_size == 0:
                logger.warning("CSV 파일 크기가 0")
                return False
            
            # 체크섬 검증
            current_checksum = self._calculate_csv_checksum(csv_path)
            if current_checksum != checkpoint_data.get('checksum'):
                logger.warning(f"체크섬 불일치: 예상={checkpoint_data.get('checksum')}, 실제={current_checksum}")
                return False
            
            # 파일 수정 시간 확인 (선택적)
            file_mtime = os.path.getmtime(csv_path)
            checkpoint_time = pd.Timestamp(checkpoint_data['timestamp']).timestamp()
            
            # 체크포인트 시간보다 파일이 최신이어야 함
            if file_mtime < checkpoint_time:
                logger.warning("CSV 파일이 체크포인트보다 오래됨")
                return False
            
            logger.debug("체크포인트 무결성 검증 통과")
            return True
            
        except Exception as e:
            logger.error(f"체크포인트 무결성 검증 실패: {e}")
            return False

    def _validate_checkpoint_data(self, checkpoint_data: dict) -> bool:
        """
        체크포인트 데이터 유효성 검증
        """
        try:
            required_fields = ['table_name', 'offset', 'total_exported', 'timestamp', 'status']
            
            # 필수 필드 확인
            for field in required_fields:
                if field not in checkpoint_data:
                    logger.warning(f"체크포인트에 필수 필드 누락: {field}")
                    return False
            
            # 데이터 타입 및 값 검증
            if not isinstance(checkpoint_data['offset'], int) or checkpoint_data['offset'] < 0:
                logger.warning(f"체크포인트 offset 값이 유효하지 않음: {checkpoint_data['offset']}")
                return False
            
            if not isinstance(checkpoint_data['total_exported'], int) or checkpoint_data['total_exported'] < 0:
                logger.warning(f"체크포인트 total_exported 값이 유효하지 않음: {checkpoint_data['total_exported']}")
                return False
            
            # 타임스탬프 검증
            try:
                timestamp = pd.Timestamp(checkpoint_data['timestamp'])
                if timestamp > pd.Timestamp.now():
                    logger.warning("체크포인트 타임스탬프가 미래 시간")
                    return False
            except Exception:
                logger.warning("체크포인트 타임스탬프 형식이 유효하지 않음")
                return False
            
            logger.debug("체크포인트 데이터 유효성 검증 통과")
            return True
            
        except Exception as e:
            logger.error(f"체크포인트 데이터 유효성 검증 실패: {e}")
            return False

    def _backup_checkpoint(self, checkpoint_file: str):
        """
        체크포인트 파일 백업
        """
        try:
            backup_file = f"{checkpoint_file}.backup"
            import shutil
            shutil.copy2(checkpoint_file, backup_file)
            logger.debug(f"체크포인트 백업 생성: {backup_file}")
        except Exception as e:
            logger.warning(f"체크포인트 백업 생성 실패: {e}")

    def _calculate_csv_checksum(self, csv_path: str) -> str | None:
        """
        CSV 파일 체크섬 계산 (개선된 버전)
        """
        import hashlib
        
        try:
            # 파일 크기가 큰 경우 청크 단위로 체크섬 계산
            chunk_size = 8192  # 8KB 청크
            hasher = hashlib.md5()
            
            with open(csv_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    hasher.update(chunk)
            
            checksum = hasher.hexdigest()
            logger.debug(f"CSV 체크섬 계산 완료: {csv_path} -> {checksum[:8]}...")
            return checksum
            
        except Exception as e:
            logger.error(f"CSV 체크섬 계산 실패: {csv_path}, 오류: {e}")
            return None

    def _cleanup_checkpoint(self, csv_path: str):
        """
        체크포인트 파일 정리 (개선된 버전)
        """
        checkpoint_file = f"{csv_path}.checkpoint"
        if os.path.exists(checkpoint_file):
            try:
                # 완료 상태로 업데이트
                with open(checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                
                # 완료 정보 추가
                checkpoint_data['status'] = 'completed'
                checkpoint_data['completed_at'] = pd.Timestamp.now().isoformat()
                checkpoint_data['final_memory_usage_mb'] = self._check_memory_usage()
                
                # 임시 파일에 먼저 쓰기 (원자성 보장)
                temp_checkpoint_file = f"{checkpoint_file}.tmp"
                with open(temp_checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
                
                # 원자적 이동
                os.rename(temp_checkpoint_file, checkpoint_file)
                
                logger.info(f"체크포인트 완료 상태로 업데이트: {csv_path}")
                
                # 백업 파일 정리 (오래된 백업 삭제)
                self._cleanup_old_checkpoint_backups(csv_path)
                
                # 완료된 체크포인트는 일정 시간 후 자동 삭제 (선택사항)
                # self._schedule_checkpoint_cleanup(checkpoint_file)
                
            except Exception as e:
                logger.error(f"체크포인트 정리 실패: {e}")
                # 정리 실패 시에도 체크포인트 파일은 보존
                logger.warning("체크포인트 정리 실패했지만 파일은 보존됨")

    def _cleanup_old_checkpoint_backups(self, csv_path: str):
        """
        오래된 체크포인트 백업 파일 정리
        """
        try:
            base_path = csv_path.replace('.csv', '')
            backup_pattern = f"{base_path}.csv.checkpoint.backup*"
            
            import glob
            backup_files = glob.glob(backup_pattern)
            
            # 7일 이상 된 백업 파일 삭제
            current_time = time.time()
            for backup_file in backup_files:
                try:
                    file_age = current_time - os.path.getmtime(backup_file)
                    if file_age > 7 * 24 * 3600:  # 7일
                        os.remove(backup_file)
                        logger.debug(f"오래된 백업 파일 삭제: {backup_file}")
                except Exception:
                    continue
                    
        except Exception as e:
            logger.debug(f"백업 파일 정리 실패: {e}")

    def _schedule_checkpoint_cleanup(self, checkpoint_file: str):
        """
        체크포인트 파일 자동 삭제 스케줄링 (선택적)
        """
        try:
            # 24시간 후 삭제를 위한 타이머 설정
            import threading
            
            def delayed_delete():
                time.sleep(24 * 3600)  # 24시간 대기
                try:
                    if os.path.exists(checkpoint_file):
                        os.remove(checkpoint_file)
                        logger.info(f"완료된 체크포인트 파일 자동 삭제: {checkpoint_file}")
                except Exception as e:
                    logger.debug(f"체크포인트 자동 삭제 실패: {e}")
            
            cleanup_thread = threading.Thread(target=delayed_delete, daemon=True)
            cleanup_thread.start()
            
        except Exception as e:
            logger.debug(f"체크포인트 자동 삭제 스케줄링 실패: {e}")

    def _optimize_batch_size_dynamically(
        self, 
        initial_batch_size: int, 
        memory_usage: float, 
        processing_time: float, 
        session_health: bool
    ) -> int:
        """
        메모리 사용량, 처리 시간, 세션 상태에 따른 배치 크기 동적 조정
        """
        if not session_health:
            # 세션 상태 불량 시에도 너무 급격하게 감소하지 않도록 개선
            if initial_batch_size > 1000:
                new_batch_size = max(initial_batch_size // 2, 1000)
            else:
                new_batch_size = max(initial_batch_size // 2, 500)
            logger.info(f"세션 상태 불량, 배치 크기 조정: {initial_batch_size} → {new_batch_size}")
            return new_batch_size
        
        elif memory_usage > 200:  # 200MB로 임계값 상향 조정
            new_batch_size = max(initial_batch_size // 2, 500)
            logger.info(f"메모리 사용량 높음, 배치 크기 조정: {initial_batch_size} → {new_batch_size}")
            return new_batch_size
        
        elif processing_time > 60:  # 60초로 임계값 상향 조정
            new_batch_size = max(initial_batch_size // 2, 500)
            logger.info(f"처리 시간 길음, 배치 크기 조정: {initial_batch_size} → {new_batch_size}")
            return new_batch_size
        
        elif memory_usage < 100 and processing_time < 20 and session_health:  # 여유로운 상황
            new_batch_size = min(initial_batch_size * 1.5, 5000)  # 더 적극적으로 증가
            logger.info(f"여유로운 상황, 배치 크기 증가: {initial_batch_size} → {new_batch_size}")
            return new_batch_size
        
        return initial_batch_size

    def _log_performance_metrics(
        self, 
        performance_metrics: dict, 
        chunk_count: int, 
        total_exported: int, 
        total_count: int
    ):
        """
        성능 메트릭 로깅
        """
        try:
            if performance_metrics['chunk_processing_times']:
                avg_chunk_time = sum(performance_metrics['chunk_processing_times'][-100:]) / min(len(performance_metrics['chunk_processing_times']), 100)
                logger.info(f"성능 메트릭 - 청크 {chunk_count}: 평균 처리시간 {avg_chunk_time:.2f}초")
            
            if performance_metrics['memory_usage_history']:
                recent_memory = performance_metrics['memory_usage_history'][-1]
                logger.info(f"성능 메트릭 - 메모리 사용량: {recent_memory:.1f}MB")
            
            logger.info(f"성능 메트릭 - 진행률: {total_exported}/{total_count} ({total_exported/total_count*100:.1f}%)")
            
        except Exception as e:
            logger.error(f"성능 메트릭 로깅 실패: {e}")

    def _log_final_performance_report(
        self, 
        performance_metrics: dict, 
        total_exported: int, 
        total_time: float
    ):
        """
        최종 성능 리포트 로깅
        """
        try:
            logger.info("=== 최종 성능 리포트 ===")
            logger.info(f"총 처리 시간: {total_time:.2f}초")
            logger.info(f"총 처리된 행: {total_exported}")
            
            if performance_metrics['chunk_processing_times']:
                avg_chunk_time = sum(performance_metrics['chunk_processing_times']) / len(performance_metrics['chunk_processing_times'])
                min_chunk_time = min(performance_metrics['chunk_processing_times'])
                max_chunk_time = max(performance_metrics['chunk_processing_times'])
                logger.info(f"청크 처리 시간 - 평균: {avg_chunk_time:.2f}초, 최소: {min_chunk_time:.2f}초, 최대: {max_chunk_time:.2f}초")
            
            if performance_metrics['memory_usage_history']:
                avg_memory = sum(performance_metrics['memory_usage_history']) / len(performance_metrics['memory_usage_history'])
                max_memory = max(performance_metrics['memory_usage_history'])
                logger.info(f"메모리 사용량 - 평균: {avg_memory:.1f}MB, 최대: {max_memory:.1f}MB")
            
            logger.info(f"세션 갱신 횟수: {performance_metrics['session_refresh_count']}")
            logger.info(f"초당 처리 행 수: {total_exported/total_time:.1f}")
            logger.info("=== 성능 리포트 완료 ===")
            
        except Exception as e:
            logger.error(f"최종 성능 리포트 로깅 실패: {e}")

    def _export_to_csv_parallel(
        self, 
        table_name: str, 
        csv_path: str, 
        where_clause: str | None, 
        batch_size: int, 
        order_by_field: str | None, 
        num_workers: int = 2
    ) -> int:
        """
        병렬 처리를 통한 성능 향상 (선택적)
        """
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            # 전체 행 수 조회
            total_count = self.db_ops.get_table_row_count(table_name, where_clause=where_clause)
            
            if total_count == 0:
                logger.warning(f"테이블 {table_name}에 데이터가 없습니다.")
                return 0
            
            # 청크 범위 계산
            chunk_ranges = self._calculate_chunk_ranges(total_count, batch_size, num_workers)
            
            # 병렬 처리
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = []
                
                for chunk_start, chunk_end in chunk_ranges:
                    future = executor.submit(
                        self._process_chunk_range,
                        table_name, where_clause, chunk_start, chunk_end, order_by_field
                    )
                    futures.append(future)
                
                # 결과 수집 및 CSV 병합
                all_chunks = []
                for future in as_completed(futures):
                    chunk_data = future.result()
                    all_chunks.append(chunk_data)
                
                # 청크별로 CSV에 순차적으로 쓰기
                self._merge_chunks_to_csv(all_chunks, csv_path)
                
                return total_count
                
        except Exception as e:
            logger.error(f"병렬 처리 CSV 내보내기 실패: {e}")
            raise

    def _calculate_chunk_ranges(self, total_count: int, batch_size: int, num_workers: int) -> list[tuple[int, int]]:
        """
        청크 범위 계산
        """
        chunk_ranges = []
        for i in range(num_workers):
            start = i * (total_count // num_workers)
            end = (i + 1) * (total_count // num_workers) if i < num_workers - 1 else total_count
            chunk_ranges.append((start, end))
        return chunk_ranges

    def _process_chunk_range(
        self, 
        table_name: str, 
        where_clause: str | None, 
        chunk_start: int, 
        chunk_end: int, 
        order_by_field: str | None
    ) -> list:
        """
        특정 범위의 청크 처리
        """
        try:
            # ORDER BY 절 구성
            if order_by_field:
                order_by_clause = f"ORDER BY {order_by_field}"
            else:
                order_by_clause = "ORDER BY 1"
            
            # WHERE 절 구성
            if where_clause:
                query = f"""
                    SELECT * FROM {table_name}
                    WHERE {where_clause}
                    {order_by_clause}
                    LIMIT {chunk_end - chunk_start} OFFSET {chunk_start}
                """
            else:
                query = f"""
                    SELECT * FROM {table_name}
                    {order_by_clause}
                    LIMIT {chunk_end - chunk_start} OFFSET {chunk_start}
                """
            
            # 데이터 조회
            chunk_data = self.source_hook.get_records(query)
            return chunk_data
            
        except Exception as e:
            logger.error(f"청크 범위 처리 실패 ({chunk_start}-{chunk_end}): {e}")
            return []

    def _merge_chunks_to_csv(self, all_chunks: list, csv_path: str):
        """
        청크별로 CSV에 순차적으로 쓰기
        """
        try:
            if not all_chunks:
                return
            
            # 첫 번째 청크에서 컬럼명 추출
            columns = list(all_chunks[0][0].keys()) if all_chunks[0] else []
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # 헤더 작성
                writer.writerow(columns)
                
                # 각 청크의 데이터를 순차적으로 쓰기
                for chunk_data in all_chunks:
                    for row in chunk_data:
                        writer.writerow([row[col] for col in columns])
                        
            logger.info(f"병렬 처리 CSV 병합 완료: {csv_path}")
            
        except Exception as e:
            logger.error(f"청크 CSV 병합 실패: {e}")
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

    # CSV 관련 임시 테이블 메서드 제거됨





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
                source_count = self.db_ops.get_table_row_count(source_table, use_target_db=False)
                target_count = self.db_ops.get_table_row_count(target_table, use_target_db=True)

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

                # 증분 동기화: 중복키 스킵 (ON CONFLICT DO NOTHING)
                if non_pk_column_names:
                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns}, {', '.join(non_pk_column_names)})
                        SELECT {pk_columns}, {', '.join(non_pk_column_names)}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO NOTHING;
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
            source_count = self.db_ops.get_table_row_count(source_table, use_target_db=False)
            target_count = self.db_ops.get_table_row_count(target_table, use_target_db=True)

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

                # COPY 명령어로 고속 삽입 (INSERT보다 10-50배 빠름)
                try:
                    # COPY 명령어 시작
                    copy_sql = f"COPY {temp_table} ({', '.join(temp_columns)}) FROM STDIN"
                    
                    # StringIO를 사용하여 파일 객체 생성
                    from io import StringIO
                    copy_buffer = StringIO()
                    
                    # 데이터를 COPY 형식으로 변환하여 버퍼에 쓰기
                    for row in batch_data:
                        # 각 행을 탭으로 구분된 문자열로 변환
                        row_str = '\t'.join(str(val) if val is not None else '\\N' for val in row)
                        copy_buffer.write(row_str + '\n')
                    
                    # 버퍼를 처음으로 되돌리기
                    copy_buffer.seek(0)
                    
                    # COPY 명령어 실행
                    cursor.copy_expert(copy_sql, copy_buffer)
                    total_inserted += len(batch_data)

                    if i % 10000 == 0:
                        logger.info(f"COPY 진행률: {total_inserted}/{len(df_reordered)}")
                        
                except Exception as copy_error:
                    logger.warning(f"COPY 명령어 실패, INSERT로 폴백: {copy_error}")
                    # COPY 실패 시 기존 INSERT 방식으로 폴백
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
                # 증분 동기화: 중복키 스킵 (ON CONFLICT DO NOTHING)
                if non_pk_column_names:
                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns}, {', '.join(non_pk_column_names)})
                        SELECT {pk_columns}, {', '.join(non_pk_column_names)}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO NOTHING;
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
        # 청크 방식 파라미터 추가
        chunk_mode: bool = True,
        enable_checkpoint: bool = True,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """
        테이블 데이터 복사 (기본 메서드)
        
        sync_mode:
            - full_sync: 전체 동기화
            - incremental_sync: 타임스탬프/비즈니스 필드 기반 증분
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
        # 청크 방식 파라미터 추가
        chunk_mode: bool = True,
        enable_checkpoint: bool = True,
        max_retries: int = 3,
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
        테이블 데이터 복사 (스테이징 테이블 방식)
        """
        try:
            start_time = pd.Timestamp.now()

            # WHERE 조건 구성
            where_condition = None
            if custom_where:
                where_condition = custom_where
            elif incremental_field and incremental_field_type:
                # 증분 동기화 조건 구성
                where_condition = self._build_incremental_where_condition(
                    incremental_field, incremental_field_type, target_table
                )

            logger.info(f"최종 WHERE 조건: {where_condition}")

            # 1. 소스 테이블 스키마 조회
            logger.info(f"소스 테이블 스키마 조회 시작: {source_table}")
            source_schema = self.db_ops.get_table_schema(source_table)

            if not source_schema or not source_schema.get("columns"):
                raise Exception(
                    f"소스 테이블 {source_table}의 스키마 정보를 가져올 수 없습니다."
                )

            # 2. 스테이징 테이블 방식으로 데이터 복사 실행
            logger.info(f"스테이징 테이블 방식으로 데이터 복사 시작: {source_table} -> {target_table}")
            
            staging_result = self.copy_with_staging_table(
                source_table=source_table,
                target_table=target_table,
                primary_keys=primary_keys,
                where_condition=where_condition
            )
            
            end_time = pd.Timestamp.now()
            total_time = (end_time - start_time).total_seconds()

            # 3. 결과 정리
            copy_result = staging_result.get('copy_result', {})
            exported_rows = copy_result.get('staging_count', 0)
            imported_rows = copy_result.get('target_count', 0)
            
            final_result = {
                "status": "success",
                "source_table": source_table,
                "target_table": target_table,
                "exported_rows": exported_rows,
                "imported_rows": imported_rows,
                "total_execution_time": total_time,
                "message": f"스테이징 테이블 방식으로 데이터 복사 완료: {imported_rows}행",
                "staging_result": staging_result
            }

            logger.info(f"스테이징 테이블 방식 데이터 복사 완료: {final_result}")
            return final_result

        except Exception as e:
            end_time = pd.Timestamp.now()
            total_time = (end_time - start_time).total_seconds()
            
            error_msg = f"스테이징 테이블 방식 데이터 복사 실패: {e}"
            logger.error(error_msg)
            
            return {
                "status": "error",
                "source_table": source_table,
                "target_table": target_table,
                "exported_rows": 0,
                "imported_rows": 0,
                "total_execution_time": total_time,
                "error": error_msg
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
        사용자 정의 SQL을 사용하여 테이블 데이터 복사 (스테이징 테이블 방식)
        """
        start_time = time.time()

        try:
            # 1단계: WHERE 조건 구성
            where_condition = None
            if custom_where:
                where_condition = custom_where
            elif incremental_field and incremental_field_type:
                # 증분 동기화 조건 구성
                where_condition = self._build_incremental_where_condition(
                    incremental_field, incremental_field_type, target_table
                )

            logger.info(f"사용자 정의 SQL을 사용한 데이터 복사 시작: {source_table} -> {target_table}")
            logger.info(f"WHERE 조건: {where_condition}")

            # 2단계: 스테이징 테이블 방식으로 데이터 복사 실행
            staging_result = self.copy_with_staging_table(
                source_table=source_table,
                target_table=target_table,
                primary_keys=primary_keys,
                where_condition=where_condition
            )
            
            # 3단계: 결과 정리
            total_time = time.time() - start_time
            
            copy_result = staging_result.get('copy_result', {})
            exported_rows = copy_result.get('staging_count', 0)
            imported_rows = copy_result.get('target_count', 0)

            result = {
                "status": "success",
                "exported_rows": exported_rows,
                "imported_rows": imported_rows,
                "total_execution_time": total_time,
                "message": f"스테이징 테이블 방식으로 데이터 복사 완료: {imported_rows}행",
                "staging_result": staging_result
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

            # 5단계: MERGE 완료 후 ANALYZE 실행하여 통계 최신화
            try:
                self._analyze_target_table(target_table)
            except Exception as e:
                logger.warning(f"ANALYZE 실행 실패: {e}")

            # 6단계: MERGE 키에 대한 인덱스 점검 및 생성
            try:
                self._check_and_create_indexes(target_table, primary_keys)
            except Exception as e:
                logger.warning(f"인덱스 점검 및 생성 실패: {e}")

            # 7단계: 스테이징 테이블 정리
            try:
                with self.target_hook.get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                        conn.commit()
                logger.info(f"스테이징 테이블 정리 완료: {temp_table}")
            except Exception as e:
                logger.warning(f"스테이징 테이블 정리 실패: {temp_table}, 오류: {e}")

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
        verified_target_schema: dict[str, Any] | None = None,
        # 청크/체크포인트 관련 파라미터 (DAG 호환성 위해 수용)
        chunk_mode: bool = True,
        enable_checkpoint: bool = True,
        max_retries: int = 3,
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
            chunk_mode: 청크 방식 사용 여부 (호환성용; 현재 함수는 페이지네이션 기반 처리)
            enable_checkpoint: 체크포인트 사용 여부 (호환성용)
            max_retries: 최대 재시도 횟수 (호환성용)

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

            # 4단계: 스테이징 테이블 방식으로 데이터 복사
            logger.info(f"스테이징 테이블 방식으로 데이터 복사 시작: {source_table} -> {target_table}")
            
            # WHERE 조건 구성
            where_condition = None
            if custom_where:
                where_condition = custom_where
            elif incremental_field and incremental_field_type:
                # 증분 동기화 조건 구성
                where_condition = self._build_incremental_where_condition(
                    incremental_field, incremental_field_type, target_table
                )
            
            # 스테이징 테이블 방식으로 데이터 복사 실행
            staging_result = self.copy_with_staging_table(
                source_table=source_table,
                target_table=target_table,
                primary_keys=primary_keys,
                where_condition=where_condition
            )
            
            total_time = time.time() - start_time

            result = {
                "status": "success",
                "source_table": source_table,
                "target_table": target_table,
                "staging_result": staging_result,
                "total_execution_time": total_time,
                "message": f"스테이징 테이블 방식 데이터 복사 완료: {staging_result.get('copy_result', {}).get('staging_count', 0)}행 처리"
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

    def _build_incremental_where_condition(
        self, 
        incremental_field: str, 
        incremental_field_type: str, 
        target_table: str
    ) -> str:
        """
        증분 동기화를 위한 WHERE 조건 생성
        
        Args:
            incremental_field: 증분 필드명
            incremental_field_type: 증분 필드 타입
            target_table: 타겟 테이블명
        
        Returns:
            WHERE 조건 문자열
        """
        try:
            # 타겟 테이블에서 최신 값 조회
            latest_value = self._get_latest_incremental_value(target_table, incremental_field)
            
            if latest_value is None:
                # 최신 값이 없으면 모든 데이터 처리
                return ""
            
            # 필드 타입에 따른 조건 생성
            if incremental_field_type == "yyyymmdd":
                return f"{incremental_field} > '{latest_value}'"
            elif incremental_field_type in ["integer", "bigint", "smallint"]:
                return f"{incremental_field} > {latest_value}"
            elif incremental_field_type in ["timestamp", "timestamptz"]:
                return f"{incremental_field} > '{latest_value}'"
            else:
                # 기본적으로 문자열 비교
                return f"{incremental_field} > '{latest_value}'"
                
        except Exception as e:
            logger.error(f"증분 WHERE 조건 생성 실패: {str(e)}")
            # 에러 발생 시 모든 데이터 처리
            return ""

    def _get_latest_incremental_value(self, target_table: str, incremental_field: str) -> Any:
        """
        타겟 테이블에서 증분 필드의 최신 값 조회
        
        Args:
            target_table: 타겟 테이블명
            incremental_field: 증분 필드명
        
        Returns:
            최신 값 또는 None
        """
        try:
            # 입력값 유효성 검증
            if not target_table or not incremental_field:
                logger.warning("target_table 또는 incremental_field가 비어있습니다")
                return None
            
            # 필드명 유효성 검증 (SQL 인젝션 방지)
            if not self._is_valid_field_name(incremental_field):
                logger.warning(f"유효하지 않은 필드명: {incremental_field}")
                return None
            
            # 타겟 테이블이 존재하는지 확인
            if not self._table_exists(target_table):
                return None
            
            # 최신 값 조회 (파라미터화된 쿼리 사용)
            query = "SELECT MAX(%s) FROM %s"
            
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # 테이블명과 필드명을 식별자로 처리
                    cursor.execute(query, (incremental_field, target_table))
                    result = cursor.fetchone()
                    return result[0] if result else None
                    
        except Exception as e:
            logger.error(f"최신 증분 값 조회 실패: {str(e)}")
            return None

    def _is_valid_field_name(self, field_name: str) -> bool:
        """
        필드명 유효성 검증 (SQL 인젝션 방지)
        
        Args:
            field_name: 검증할 필드명
        
        Returns:
            유효성 여부
        """
        if not field_name or not isinstance(field_name, str):
            return False
        
        # PostgreSQL 식별자 규칙 검증 (한글 지원)
        # 허용되는 문자: 영문자, 숫자, 언더스코어, 달러 기호, 한글
        # 첫 글자는 영문자, 언더스코어, 한글이어야 함
        import re
        pattern = r'^[a-zA-Z가-힣_][a-zA-Z0-9가-힣_$]*$'
        
        return bool(re.match(pattern, field_name))

    def _table_exists(self, table_name: str) -> bool:
        """
        테이블 존재 여부 확인
        
        Args:
            table_name: 테이블명
        
        Returns:
            테이블 존재 여부
        """
        try:
            # 테이블명 유효성 검증
            if not self._is_valid_table_name(table_name):
                logger.warning(f"유효하지 않은 테이블명: {table_name}")
                return False
            
            # 스키마와 테이블명 분리
            if "." in table_name:
                schema, table = table_name.split(".", 1)
            else:
                schema = "public"
                table = table_name
            
            # 스키마와 테이블명 유효성 검증
            if not self._is_valid_schema_name(schema) or not self._is_valid_table_name(table):
                logger.warning(f"유효하지 않은 스키마 또는 테이블명: {schema}.{table}")
                return False
            
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """
            
            with self.target_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (schema, table))
                    result = cursor.fetchone()
                    return result[0] if result else False
                    
        except Exception as e:
            logger.error(f"테이블 존재 여부 확인 실패: {str(e)}")
            return False

    def _is_valid_table_name(self, table_name: str) -> bool:
        """
        테이블명 유효성 검증 (SQL 인젝션 방지)
        
        Args:
            table_name: 검증할 테이블명
        
        Returns:
            유효성 여부
        """
        if not table_name or not isinstance(table_name, str):
            return False
        
        # PostgreSQL 식별자 규칙 검증 (한글 지원)
        # 허용되는 문자: 영문자, 숫자, 언더스코어, 달러 기호, 한글, 점(.)
        # 첫 글자는 영문자, 언더스코어, 한글이어야 함
        import re
        
        # 스키마.테이블명 형식 지원
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
            # 스키마명과 테이블명 모두 검증
            return (self._is_valid_schema_name(schema) and 
                   self._is_valid_table_name(table))
        
        # 한글, 영문, 숫자, 언더스코어, 달러 기호 허용
        pattern = r'^[a-zA-Z가-힣_][a-zA-Z0-9가-힣_$]*$'
        
        return bool(re.match(pattern, table_name))

    def _is_valid_schema_name(self, schema_name: str) -> bool:
        """
        스키마명 유효성 검증 (SQL 인젝션 방지)
        
        Args:
            schema_name: 검증할 스키마명
        
        Returns:
            유효성 여부
        """
        if not schema_name or not isinstance(schema_name, str):
            return False
        
        # PostgreSQL 식별자 규칙 검증 (한글 지원)
        # 허용되는 문자: 영문자, 숫자, 언더스코어, 달러 기호, 한글
        # 첫 글자는 영문자, 언더스코어, 한글이어야 함
        import re
        pattern = r'^[a-zA-Z가-힣_][a-zA-Z0-9가-힣_$]*$'
        
        return bool(re.match(pattern, schema_name))

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

            # 컬럼 정의 생성 (스키마 구조에 따라 처리)
            column_definitions = []
            
            # 스키마가 리스트 형태인지 딕셔너리 형태인지 확인
            if isinstance(source_schema, list):
                # 리스트 형태: [{"column_name": "...", "data_type": "...", ...}]
                for col in source_schema:
                    col_name = col["column_name"]
                    col_type = col["data_type"]
                    col_type = self._convert_to_postgres_type(col_type, col.get('character_maximum_length'))
                    nullable = "" if col["is_nullable"] == "YES" else " NOT NULL"
                    column_definitions.append(f'"{col_name}" {col_type}{nullable}')
            elif isinstance(source_schema, dict) and "columns" in source_schema:
                # 딕셔너리 형태: {"columns": [{"name": "...", "type": "...", ...}]}
                for col in source_schema["columns"]:
                    col_name = col["name"]
                    col_type = col["type"]
                    col_type = self._convert_to_postgres_type(col_type, col.get('max_length'))
                    nullable = "" if col["nullable"] else " NOT NULL"
                    column_definitions.append(f"{col_name} {col_type}{nullable}")
            else:
                raise ValueError(f"지원하지 않는 스키마 구조: {type(source_schema)}")

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
                    # 컬럼명 매핑 (스키마 구조에 따라 처리)
                    if source_schema:
                        if isinstance(source_schema, list):
                            # 리스트 형태: [{"column_name": "...", "data_type": "...", ...}]
                            schema_columns = [col["column_name"] for col in source_schema]
                            available_columns = [col for col in schema_columns if col in df.columns]
                        elif isinstance(source_schema, dict) and source_schema.get("columns"):
                            # 딕셔너리 형태: {"columns": [{"name": "...", "type": "...", ...}]}
                            schema_columns = [col["name"] for col in source_schema["columns"]]
                            available_columns = [col for col in schema_columns if col in df.columns]
                        else:
                            available_columns = list(df.columns)
                        
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

                        # 스키마에서 해당 컬럼의 타입 정보 찾기 (스키마 구조에 따라 처리)
                        if isinstance(source_schema, list):
                            col_schema = next((c for c in source_schema if c["column_name"] == col), None)
                            col_type = col_schema["data_type"].lower() if col_schema else "text"
                        elif isinstance(source_schema, dict) and "columns" in source_schema:
                            col_schema = next((c for c in source_schema["columns"] if c["name"] == col), None)
                            col_type = col_schema["type"].lower() if col_schema else "text"
                        else:
                            col_type = "text"

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

            # 컬럼 정의 생성 (스키마 구조에 따라 처리)
            column_definitions = []
            
            # 스키마가 리스트 형태인지 딕셔너리 형태인지 확인
            if isinstance(source_schema, list):
                # 리스트 형태: [{"column_name": "...", "data_type": "...", ...}]
                for col in source_schema:
                    col_name = col["column_name"]
                    col_type = col["data_type"]
                    col_type = self._convert_to_postgres_type(col_type, col.get('character_maximum_length'))
                    nullable = "" if col["is_nullable"] == "YES" else " NOT NULL"
                    column_definitions.append(f'"{col_name}" {col_type}{nullable}')
            elif isinstance(source_schema, dict) and "columns" in source_schema:
                # 딕셔너리 형태: {"columns": [{"name": "...", "type": "...", ...}]}
                for col in source_schema["columns"]:
                    col_name = col["name"]
                    col_type = col["type"]
                    col_type = self._convert_to_postgres_type(col_type, col.get('max_length'))
                    nullable = "" if col["nullable"] else " NOT NULL"
                    column_definitions.append(f"{col_name} {col_type}{nullable}")
            else:
                raise ValueError(f"지원하지 않는 스키마 구조: {type(source_schema)}")

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
            # 컬럼명 매핑 (스키마 구조에 따라 처리)
            if source_schema:
                if isinstance(source_schema, list):
                    # 리스트 형태: [{"column_name": "...", "data_type": "...", ...}]
                    schema_columns = [col["column_name"] for col in source_schema]
                    available_columns = [col for col in schema_columns if col in df.columns]
                elif isinstance(source_schema, dict) and source_schema.get("columns"):
                    # 딕셔너리 형태: {"columns": [{"name": "...", "type": "...", ...}]}
                    schema_columns = [col["name"] for col in source_schema["columns"]]
                    available_columns = [col for col in schema_columns if col in df.columns]
                else:
                    available_columns = list(df.columns)
                
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

                # 스키마에서 해당 컬럼의 타입 정보 찾기 (스키마 구조에 따라 처리)
                if isinstance(source_schema, list):
                    col_schema = next((c for c in source_schema if c["column_name"] == col), None)
                    col_type = col_schema["data_type"].lower() if col_schema else "text"
                elif isinstance(source_schema, dict) and "columns" in source_schema:
                    col_schema = next((c for c in source_schema["columns"] if c["name"] == col), None)
                    col_type = col_schema["type"].lower() if col_schema else "text"
                else:
                    col_type = "text"

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

            # 배치 단위로 COPY 명령어 실행 (INSERT보다 10-50배 빠름)
            total_inserted = 0
            for i in range(0, len(df_reordered), batch_size):
                batch_df = df_reordered.iloc[i : i + batch_size]
                batch_data = [tuple(row) for row in batch_df.values]

                # COPY 명령어로 고속 삽입
                try:
                    # COPY 명령어 시작
                    copy_sql = f"COPY {temp_table} ({', '.join(temp_columns)}) FROM STDIN"
                    
                    # StringIO를 사용하여 파일 객체 생성
                    from io import StringIO
                    copy_buffer = StringIO()
                    
                    # 데이터를 COPY 형식으로 변환하여 버퍼에 쓰기
                    for row in batch_data:
                        # 각 행을 탭으로 구분된 문자열로 변환
                        row_str = '\t'.join(str(val) if val is not None else '\\N' for val in row)
                        copy_buffer.write(row_str + '\n')
                    
                    # 버퍼를 처음으로 되돌리기
                    copy_buffer.seek(0)
                    
                    # COPY 명령어 실행
                    cursor.copy_expert(copy_sql, copy_buffer)
                    total_inserted += len(batch_data)

                    if i % 10000 == 0:
                        logger.info(f"COPY 진행률: {total_inserted}/{len(df_reordered)}")
                        
                except Exception as copy_error:
                    logger.warning(f"COPY 명령어 실패, INSERT로 폴백: {copy_error}")
                    # COPY 실패 시 기존 INSERT 방식으로 폴백
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

                        # 증분 동기화: 중복키 스킵 (ON CONFLICT DO NOTHING)
                        if non_pk_column_names:
                            merge_sql = f"""
                                INSERT INTO {target_table} ({pk_columns}, {', '.join(non_pk_column_names)})
                                SELECT {pk_columns}, {', '.join(non_pk_column_names)}
                                FROM {source_table}
                                ON CONFLICT ({pk_columns})
                                DO NOTHING;
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

                # 증분 동기화: 중복키 스킵 (ON CONFLICT DO NOTHING)
                if non_pk_column_names:
                    merge_sql = f"""
                        INSERT INTO {target_table} ({pk_columns}, {', '.join(non_pk_column_names)})
                        SELECT {pk_columns}, {', '.join(non_pk_column_names)}
                        FROM {source_table}
                        ON CONFLICT ({pk_columns})
                        DO NOTHING;
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

            # 결과 확인 (증분 동기화의 경우 소스는 임시 테이블, 타겟은 실제 테이블)
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


        """
        xmin 기반 증분 데이터 복사 (실제 xmin 값 저장)
        
        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            primary_keys: 기본키 컬럼 목록
            sync_mode: 동기화 모드
            batch_size: 배치 크기
            custom_where: 커스텀 WHERE 조건
            
        Returns:
            복사 결과 딕셔너리
        """
        try:
            start_time = time.time()
            logger.info(f"xmin 기반 증분 데이터 복사 시작: {source_table} -> {target_table}")
            
            # 1단계: xmin 안정성 검증
            xmin_stability = self.db_ops.validate_xmin_stability(source_table)
            if xmin_stability == "force_full_sync":
                logger.warning("xmin 순환 위험으로 전체 동기화 모드로 전환")
                return self.copy_table_data(
                    source_table=source_table,
                    target_table=target_table,
                    primary_keys=primary_keys,
                    sync_mode="full_sync",
                    batch_size=batch_size,
                    custom_where=custom_where,
                    # 청크 방식 파라미터 전달
                    chunk_mode=chunk_mode,
                    enable_checkpoint=enable_checkpoint,
                    max_retries=max_retries
                )
            
            # 2단계: 복제 상태 확인
            if not self.db_ops.check_replication_status(source_table):
                logger.warning("복제본 환경에서 xmin 기반 처리 불가, 타임스탬프 기반으로 전환")
                return self.copy_table_data(
                    source_table=source_table,
                    target_table=target_table,
                    primary_keys=primary_keys,
                    sync_mode="incremental_sync",
                    batch_size=batch_size,
                    custom_where=custom_where,
                    # 청크 방식 파라미터 전달
                    chunk_mode=chunk_mode,
                    enable_checkpoint=enable_checkpoint,
                    max_retries=max_retries
                )
            
            # 3단계: 타겟 테이블에 source_xmin 컬럼 확인/추가
            self.db_ops.ensure_xmin_column_exists(target_table)
            
            # 4단계: 마지막으로 처리된 xmin 값 조회
            last_xmin = self.db_ops.get_last_processed_xmin_from_target(target_table)
            
            # 5단계: xmin 기반 증분 조건 생성
            incremental_condition, latest_xmin = self.db_ops.build_xmin_incremental_condition(
                source_table, target_table, last_xmin
            )
            
            # 6단계: 커스텀 WHERE 조건과 결합
            if custom_where:
                if incremental_condition == "1=1":
                    where_clause = custom_where
                else:
                    where_clause = f"({incremental_condition}) AND ({custom_where})"
            else:
                where_clause = incremental_condition
            
            # 7단계: xmin 포함 증분 데이터 조회
            select_sql = f"""
                SELECT *, xmin as source_xmin
                FROM {source_table}
                WHERE {where_clause}
                ORDER BY xmin
            """
            
            logger.info(f"xmin 기반 증분 데이터 조회 SQL: {select_sql}")
            
            # 8단계: CSV 파일로 데이터 추출 (xmin 포함, 청크 방식 지원)
            csv_path = self._export_to_csv_with_xmin(
                source_table, 
                select_sql, 
                batch_size,
                chunk_mode=chunk_mode,
                enable_checkpoint=enable_checkpoint,
                max_retries=max_retries
            )
            
            # 9단계: 임시 테이블 생성 및 데이터 로드
            temp_table = f"temp_{target_table.replace('.', '_')}"
            self._import_csv_in_session_with_xmin(temp_table, csv_path, batch_size)
            
            # 10단계: MERGE 작업 실행 (xmin 값 포함)
            merge_result = self._execute_xmin_merge(
                source_table, target_table, temp_table, primary_keys
            )
            
            # 11단계: 임시 파일 정리
            self.cleanup_temp_files(csv_path)
            
            result = {
                "status": "success",
                "source_table": source_table,
                "target_table": target_table,
                "sync_mode": sync_mode,
                "xmin_stability": xmin_stability,
                "last_processed_xmin": last_xmin,
                "latest_source_xmin": latest_xmin,
                "merge_result": merge_result,
                "batch_size": batch_size,
                "execution_time": time.time() - start_time if 'start_time' in locals() else 0
            }
            
            logger.info(f"xmin 기반 증분 데이터 복사 완료: {result}")
            return result
            
        except Exception as e:
            logger.error(f"xmin 기반 증분 데이터 복사 실패: {e}")
            # 임시 파일 정리 시도
            try:
                if 'csv_path' in locals():
                    self.cleanup_temp_files(csv_path)
            except:
                pass
            
            return {
                "status": "error",
                "message": str(e),
                "source_table": source_table,
                "target_table": target_table,
                "sync_mode": sync_mode
            }

    def validate_xmin_incremental_integrity(self, source_table: str, target_table: str) -> dict[str, Any]:
        """
        xmin 기반 증분 동기화의 무결성을 검증
        
        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            
        Returns:
            무결성 검증 결과
        """
        try:
            logger.info(f"xmin 기반 증분 동기화 무결성 검증 시작: {source_table} -> {target_table}")
            
            # 1단계: 소스 테이블의 최신 xmin 값 조회
            source_max_xmin = self.get_source_max_xmin(source_table)
            
            # 2단계: 타겟 테이블의 마지막 처리된 xmin 값 조회
            target_last_xmin = self.db_ops.get_last_processed_xmin_from_target(target_table)
            
            # 3단계: 동기화 상태 확인
            is_synchronized = target_last_xmin >= source_max_xmin if source_max_xmin > 0 else True
            sync_gap = max(0, source_max_xmin - target_last_xmin) if source_max_xmin > 0 else 0
            
            # 4단계: 데이터 일치성 확인
            source_count = self.db_ops.get_table_row_count(source_table)
            target_count = self.db_ops.get_table_row_count(target_table)
            
            # 5단계: xmin 값 분포 확인
            xmin_distribution = self._get_xmin_distribution(source_table)
            
            integrity_result = {
                "source_table": source_table,
                "target_table": target_table,
                "source_max_xmin": source_max_xmin,
                "target_last_xmin": target_last_xmin,
                "is_synchronized": is_synchronized,
                "sync_gap": sync_gap,
                "source_count": source_count,
                "target_count": target_count,
                "xmin_distribution": xmin_distribution,
                "integrity_score": self._calculate_integrity_score(
                    is_synchronized, sync_gap, source_count, target_count
                ),
                "status": "success"
            }
            
            logger.info(f"xmin 무결성 검증 완료: {integrity_result}")
            return integrity_result
            
        except Exception as e:
            logger.error(f"xmin 무결성 검증 실패: {e}")
            return {
                "status": "error",
                "message": str(e),
                "source_table": source_table,
                "target_table": target_table
            }

    def _get_xmin_distribution(self, source_table: str) -> dict[str, Any]:
        """
        소스 테이블의 xmin 값 분포 조회
        
        Args:
            source_table: 소스 테이블명
            
        Returns:
            xmin 분포 정보
        """
        try:
            source_hook = self.source_hook
            
            # xmin 값 분포 조회
            distribution_sql = f"""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(xmin::text::bigint) as min_xmin,
                    MAX(xmin::text::bigint) as max_xmin,
                    AVG(xmin::text::bigint) as avg_xmin,
                    STDDEV(xmin::text::bigint) as stddev_xmin
                FROM {source_table}
            """
            
            result = source_hook.get_first(distribution_sql)
            if not result:
                return {}
            
            total_records, min_xmin, max_xmin, avg_xmin, stddev_xmin = result
            
            return {
                "total_records": total_records,
                "min_xmin": min_xmin,
                "max_xmin": max_xmin,
                "avg_xmin": round(avg_xmin) if avg_xmin else 0,
                "stddev_xmin": round(stddev_xmin) if stddev_xmin else 0,
                "xmin_range": max_xmin - min_xmin if max_xmin and min_xmin else 0
            }
            
        except Exception as e:
            logger.error(f"xmin 분포 조회 실패: {e}")
            return {}

    def _calculate_integrity_score(self, is_synchronized: bool, sync_gap: int, 
                                 source_count: int, target_count: int) -> float:
        """
        무결성 점수 계산
        
        Args:
            is_synchronized: 동기화 상태
            sync_gap: 동기화 간격
            source_count: 소스 테이블 행 수
            target_count: 타겟 테이블 행 수
            
        Returns:
            무결성 점수 (0.0 ~ 1.0)
        """
        try:
            score = 1.0
            
            # 동기화 상태에 따른 점수 차감
            if not is_synchronized:
                score -= 0.3
            
            # 동기화 간격에 따른 점수 차감
            if sync_gap > 1000000:  # 100만 이상
                score -= 0.2
            elif sync_gap > 100000:  # 10만 이상
                score -= 0.1
            
            # 데이터 일치성에 따른 점수 차감
            if source_count > 0 and target_count > 0:
                count_ratio = min(source_count, target_count) / max(source_count, target_count)
                score -= (1.0 - count_ratio) * 0.2
            
            return max(0.0, score)
            
        except Exception as e:
            logger.error(f"무결성 점수 계산 실패: {e}")
            return 0.0

    def _export_to_csv_with_xmin(
        self,
        source_table: str,
        select_sql: str,
        batch_size: int
    ) -> str:
        """
        xmin 포함 데이터를 CSV로 추출
        
        Args:
            source_table: 소스 테이블명
            select_sql: SELECT SQL 쿼리
            batch_size: 배치 크기
            
        Returns:
            CSV 파일 경로
        """
        try:
            # CSV 파일 경로 생성
            timestamp = int(time.time())
            safe_table_name = source_table.replace('.', '_').replace('-', '_')
            csv_path = f"{self.temp_dir}/{safe_table_name}_{timestamp}.csv"
            
            # psql 명령어로 데이터 추출 (xmin 포함)
            # 환경변수에서 연결 정보 가져오기
            source_conn = self.source_hook.get_conn()
            source_host = source_conn.info.host
            source_port = source_conn.info.port
            source_user = source_conn.info.user
            source_database = source_conn.info.dbname
            source_password = source_conn.info.password
            
            # psql 명령어 구성
            export_cmd = [
                "psql",
                "-h", str(source_host),
                "-p", str(source_port),
                "-U", str(source_user),
                "-d", str(source_database),
                "-c", f"\\copy ({select_sql}) TO '{csv_path}' WITH CSV HEADER"
            ]
            
            # 환경변수 설정
            env = os.environ.copy()
            if source_password:
                env["PGPASSWORD"] = str(source_password)
            
            # psql 명령어 실행
            import subprocess
            result = subprocess.run(
                export_cmd, 
                env=env, 
                capture_output=True, 
                text=True,
                timeout=3600  # 1시간 타임아웃
            )
            
            if result.returncode != 0:
                raise Exception(f"CSV 추출 실패: {result.stderr}")
            
            # 파일 크기 확인
            if os.path.exists(csv_path):
                file_size = os.path.getsize(csv_path)
                logger.info(f"xmin 포함 데이터 CSV 추출 완료: {csv_path}, 크기: {file_size} bytes")
            else:
                raise Exception("CSV 파일이 생성되지 않았습니다")
            
            return csv_path
            
        except Exception as e:
            logger.error(f"xmin 포함 데이터 CSV 추출 실패: {e}")
            raise

    def _import_csv_in_session_with_xmin(
        self,
        temp_table: str,
        csv_path: str,
        batch_size: int
    ) -> None:
        """
        xmin 포함 CSV 데이터를 임시 테이블에 로드
        
        Args:
            temp_table: 임시 테이블명
            csv_path: CSV 파일 경로
            batch_size: 배치 크기
        """
        try:
            target_hook = self.target_hook
            
            # CSV 파일에서 컬럼 정보 추출
            import pandas as pd
            df_sample = pd.read_csv(csv_path, nrows=1)
            columns = list(df_sample.columns)
            
            # source_xmin 컬럼이 있는지 확인
            if 'source_xmin' not in columns:
                raise Exception("source_xmin 컬럼이 CSV에 포함되지 않았습니다")
            
            # 임시 테이블 생성
            create_temp_sql = f"""
                CREATE TEMP TABLE {temp_table} (
                    {', '.join([f'"{col}" TEXT' for col in columns])}
                ) ON COMMIT DROP
            """
            
            target_hook.run(create_temp_sql)
            logger.info(f"임시 테이블 생성 완료: {temp_table}")
            
            # CSV 데이터 로드
            copy_sql = f"""
                COPY {temp_table} FROM '{csv_path}' WITH CSV HEADER
            """
            
            target_hook.run(copy_sql)
            
            # 로드된 데이터 수 확인
            count_sql = f"SELECT COUNT(*) FROM {temp_table}"
            loaded_count = target_hook.get_first(count_sql)[0]
            
            logger.info(f"CSV 데이터 로드 완료: {temp_table}, {loaded_count}행")
            
        except Exception as e:
            logger.error(f"CSV 데이터 임시 테이블 로드 실패: {e}")
            raise

    def _execute_xmin_merge(
        self,
        source_table: str,
        target_table: str,
        temp_table: str,
        primary_keys: list[str]
    ) -> dict[str, Any]:
        """
        xmin 기반 MERGE 작업 실행 (source_xmin 컬럼 포함)
        
        Args:
            source_table: 소스 테이블명
            target_table: 타겟 테이블명
            temp_table: 임시 테이블명
            primary_keys: 기본키 컬럼 목록
            
        Returns:
            MERGE 결과 딕셔너리
        """
        try:
            target_hook = self.target_hook
            
            # 기본키 조건 구성
            pk_conditions = " AND ".join([
                f"target.{pk} = source.{pk}" for pk in primary_keys
            ])
            
            # 업데이트할 컬럼 목록 (source_xmin 포함)
            update_columns = self._get_update_columns_for_xmin(target_table)
            
            # MERGE SQL 실행
            merge_sql = f"""
                MERGE INTO {target_table} AS target
                USING {temp_table} AS source
                ON {pk_conditions}
                
                WHEN MATCHED THEN
                    UPDATE SET
                        {', '.join([f"{col} = source.{col}" for col in update_columns])},
                        updated_at = CURRENT_TIMESTAMP
                
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({', '.join(self._get_all_columns_for_xmin(target_table))})
                    VALUES ({', '.join([f"source.{col}" for col in self._get_all_columns_for_xmin(target_table)])});
            """
            
            logger.info(f"xmin 기반 MERGE SQL 실행: {merge_sql}")
            
            # MERGE 실행
            start_time = pd.Timestamp.now()
            target_hook.run(merge_sql)
            end_time = pd.Timestamp.now()
            
            # 결과 확인
            result_sql = f"SELECT COUNT(*) FROM {temp_table}"
            total_processed = target_hook.get_first(result_sql)[0]
            
            # 임시 테이블 삭제
            target_hook.run(f"DROP TABLE IF EXISTS {temp_table}")
            
            merge_result = {
                "total_processed": total_processed,
                "merge_sql": merge_sql,
                "execution_time": (end_time - start_time).total_seconds(),
                "status": "success"
            }
            
            logger.info(f"xmin 기반 MERGE 완료: {merge_result}")
            return merge_result
            
        except Exception as e:
            logger.error(f"xmin 기반 MERGE 실패: {e}")
            raise

    def _get_update_columns_for_xmin(self, target_table: str) -> list[str]:
        """
        xmin 기반 업데이트를 위한 컬럼 목록 조회 (source_xmin 포함)
        
        Args:
            target_table: 타겟 테이블명
            
        Returns:
            업데이트할 컬럼 목록
        """
        try:
            target_hook = self.target_hook
            
            # 테이블의 모든 컬럼 조회
            if "." in target_table:
                schema_name, table_name = target_table.split(".", 1)
            else:
                schema_name = "raw_data"
                table_name = target_table
            
            columns_sql = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            
            columns = target_hook.get_records(columns_sql, parameters=(schema_name, table_name))
            column_names = [col[0] for col in columns]
            
            # 제외할 컬럼들
            exclude_columns = ["created_at", "id"]  # id는 기본키이므로 제외
            
            # source_xmin 컬럼은 업데이트 대상에 포함
            update_columns = [col for col in column_names if col not in exclude_columns]
            
            # source_xmin 컬럼이 없으면 추가
            if "source_xmin" not in update_columns:
                update_columns.append("source_xmin")
            
            logger.info(f"xmin 업데이트 컬럼 목록: {update_columns}")
            return update_columns
            
        except Exception as e:
            logger.error(f"업데이트 컬럼 목록 조회 실패: {e}")
            raise

    def execute_merge_with_xmin(self, temp_table: str, target_table: str, max_xmin: int, primary_keys: list[str]) -> dict[str, Any]:
        """
        source_xmin을 포함한 MERGE 실행
        
        Args:
            temp_table: 임시 테이블명
            target_table: 타겟 테이블명
            max_xmin: 소스 테이블의 최대 xmin 값
            primary_keys: 기본키 컬럼 목록
            
        Returns:
            MERGE 실행 결과
        """
        try:
            logger.info(f"source_xmin({max_xmin})을 포함한 MERGE 실행: {temp_table} -> {target_table}")
            
            target_hook = self.target_hook
            
            # 1. source_xmin 컬럼이 타겟 테이블에 존재하는지 확인
            self.db_ops.ensure_xmin_column_exists(target_table)
            
            # 2. 기본키 조건 구성
            pk_conditions = " AND ".join([
                f"target.{pk} = source.{pk}" for pk in primary_keys
            ])
            
            # 3. 업데이트할 컬럼 목록 조회 (source_xmin 포함)
            update_columns = self._get_update_columns_for_xmin(target_table)
            
            # 4. INSERT용 컬럼 목록 조회 (source_xmin 포함)
            insert_columns = self._get_all_columns_for_xmin(target_table)
            
            # 5. MERGE SQL 구성
            merge_sql = f"""
                MERGE INTO {target_table} AS target
                USING {temp_table} AS source
                ON {pk_conditions}
                
                WHEN MATCHED THEN
                    UPDATE SET
                        {', '.join([f"{col} = source.{col}" for col in update_columns])},
                        updated_at = CURRENT_TIMESTAMP
                
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({', '.join(insert_columns)})
                    VALUES ({', '.join([f"source.{col}" for col in insert_columns])});
            """
            
            logger.info(f"MERGE SQL 실행: {merge_sql}")
            
            # 6. MERGE 실행
            start_time = time.time()
            target_hook.run(merge_sql)
            end_time = time.time()
            
            # 7. 결과 확인
            source_count_sql = f"SELECT COUNT(*) FROM {temp_table}"
            source_count = target_hook.get_first(source_count_sql)[0]
            
            target_count_sql = f"SELECT COUNT(*) FROM {target_table}"
            target_count = target_hook.get_first(target_count_sql)[0]
            
            # 8. 임시 테이블 정리
            target_hook.run(f"DROP TABLE IF EXISTS {temp_table}")
            
            # 9. 결과 반환
            merge_result = {
                "source_count": source_count,
                "target_count": target_count,
                "max_xmin": max_xmin,
                "execution_time": end_time - start_time,
                "status": "success",
                "message": f"MERGE 완료: {temp_table} -> {target_table}, "
                          f"소스: {source_count}행, 타겟: {target_count}행, "
                          f"최대 xmin: {max_xmin}"
            }
            
            logger.info(merge_result["message"])
            return merge_result
            
        except Exception as e:
            logger.error(f"source_xmin을 포함한 MERGE 실행 실패: {e}")
            # 임시 테이블 정리 시도
            try:
                self.target_hook.run(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass
            
            return {
                "status": "error",
                "message": str(e),
                "source_count": 0,
                "target_count": 0,
                "max_xmin": max_xmin,
                "execution_time": 0
            }

    def _get_all_columns_for_xmin(self, target_table: str) -> list[str]:
        """
        xmin 기반 INSERT를 위한 모든 컬럼 목록 조회 (source_xmin 포함)
        
        Args:
            target_table: 타겟 테이블명
            
        Returns:
            모든 컬럼 목록
        """
        try:
            target_hook = self.target_hook
            
            # 테이블의 모든 컬럼 조회
            if "." in target_table:
                schema_name, table_name = target_table.split(".", 1)
            else:
                schema_name = "raw_data"
                table_name = target_table
            
            columns_sql = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            
            columns = target_hook.get_records(columns_sql, parameters=(schema_name, table_name))
            column_names = [col[0] for col in columns]
            
            # source_xmin 컬럼이 없으면 추가
            if "source_xmin" not in column_names:
                column_names.append("source_xmin")
            
            logger.info(f"xmin INSERT 컬럼 목록: {column_names}")
            return column_names
            
        except Exception as e:
            logger.error(f"INSERT 컬럼 목록 조회 실패: {e}")
            raise

    def cleanup_temp_files(self, file_path: str) -> None:
        """
        임시 파일 정리
        
        Args:
            file_path: 정리할 파일 경로
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"임시 파일 정리 완료: {file_path}")
        except Exception as e:
            logger.warning(f"임시 파일 정리 실패: {file_path}, 오류: {e}")
