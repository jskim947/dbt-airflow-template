"""
Settings Module
DAG 설정과 연결 정보를 관리하는 공통 설정 모듈
- 모든 설정값은 Airflow Variables에서 관리
- 이 파일은 설정을 가져오는 기능만 제공
"""

import os
from typing import Any, Dict, List
import json
from datetime import timedelta

from airflow.models import Variable


class DAGSettings:
    """DAG 설정을 관리하는 클래스 - Airflow Variables 기반"""

    @classmethod
    def get_connection_ids(cls) -> Dict[str, str]:
        """
        기본 연결 ID를 반환 (Airflow Variables에서 가져옴)

        Returns:
            연결 ID 딕셔너리
        """
        try:
            conn_configs = json.loads(Variable.get("connection_configs", deserialize_json=True, default_var={}))
            return conn_configs
        except:
            # fallback: 환경변수에서 기본값
            return {
                "source_postgres": os.getenv("SOURCE_POSTGRES_CONN_ID", "fs2_postgres"),
                "target_postgres": os.getenv("TARGET_POSTGRES_CONN_ID", "postgres_default"),
                "airflow_db": os.getenv("AIRFLOW_DB_CONN_ID", "airflow_db"),
            }

    @classmethod
    def get_dbt_settings(cls) -> Dict[str, Any]:
        """
        dbt 관련 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            dbt 설정 딕셔너리
        """
        try:
            dbt_configs = json.loads(Variable.get("dbt_configs", deserialize_json=True, default_var={}))
            return dbt_configs
        except:
            # fallback: 환경변수에서 기본값
            return {
                "project_path": os.getenv("DBT_PROJECT_PATH", "/opt/airflow/dbt"),
                "profile_name": os.getenv("DBT_PROFILE_NAME", "postgres_data_copy"),
                "target_name": os.getenv("DBT_TARGET_NAME", "dev"),
            }

    @classmethod
    def get_table_configs(cls) -> List[Dict[str, Any]]:
        """
        테이블 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            테이블 설정 목록
        """
        try:
            table_configs = json.loads(Variable.get("table_configs", deserialize_json=True, default_var=[]))
            return table_configs
        except:
            # fallback: 빈 리스트 반환
            return []

    @classmethod
    def get_edi_table_configs(cls) -> List[Dict[str, Any]]:
        """
        EDI 테이블 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            EDI 테이블 설정 목록
        """
        try:
            edi_configs = json.loads(Variable.get("edi_table_configs", deserialize_json=True, default_var=[]))
            return edi_configs
        except:
            # fallback: 빈 리스트 반환
            return []

    @classmethod
    def get_schedule_intervals(cls) -> Dict[str, str]:
        """
        스케줄 간격 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            스케줄 간격 딕셔너리
        """
        try:
            schedule_configs = json.loads(Variable.get("schedule_configs", deserialize_json=True, default_var={}))
            return schedule_configs
        except:
            # fallback: 기본값
            return {
                "main_copy": "0 3,16 * * *",
                "edi_copy": "0 9 * * *",
                "daily": "0 2 * * *",
                "hourly": "0 * * * *",
                "manual": "@once",
            }

    @classmethod
    def get_environment_config(cls) -> Dict[str, Any]:
        """
        환경별 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            환경 설정 딕셔너리
        """
        try:
            env_configs = json.loads(Variable.get("environment_configs", deserialize_json=True, default_var={}))
            current_env = os.getenv("AIRFLOW_ENV", "development").lower()
            return env_configs.get(current_env, env_configs.get("development", {}))
        except:
            # fallback: 환경변수에서 기본값
            env = os.getenv("AIRFLOW_ENV", "development").lower()
            base_config = {
                "debug": True,
                "log_level": "DEBUG",
                "email_notifications": False,
                "retries": 1,
                "batch_size": 1000,
            }
            
            if env == "production":
                base_config.update({
                    "debug": False,
                    "log_level": "WARNING",
                    "email_notifications": True,
                    "retries": 3,
                    "batch_size": 10000,
                })
            elif env == "staging":
                base_config.update({
                    "debug": False,
                    "log_level": "INFO",
                    "email_notifications": True,
                    "retries": 2,
                    "batch_size": 5000,
                })
            
            return base_config



    @classmethod
    def get_chunk_mode_configs(cls) -> Dict[str, Any]:
        """
        청크 방식 데이터 복사 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            청크 방식 설정 딕셔너리
        """
        try:
            chunk_configs = json.loads(Variable.get("chunk_mode_configs", deserialize_json=True, default_var={}))
            return chunk_configs
        except:
            # fallback: 기본값
            return {
                "default_settings": {
                    "chunk_mode": True,
                    "enable_checkpoint": True,
                    "max_retries": 3,
                    "description": "기본 청크 방식 설정"
                }
            }

    @classmethod
    def get_sync_mode_configs(cls) -> Dict[str, Any]:
        """
        동기화 모드 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            동기화 모드 설정 딕셔너리
        """
        try:
            sync_configs = json.loads(Variable.get("sync_mode_configs", deserialize_json=True, default_var={}))
            return sync_configs
        except:
            # fallback: 기본값
            return {
                "full_sync": {
                    "description": "Full table sync - truncate and insert all data",
                    "truncate_before_copy": True,
                    "parallel_workers": 4,
                    "timeout_minutes": 30,
                },
                "incremental_sync": {
                    "description": "Incremental sync - only new/modified records",
                    "truncate_before_copy": False,
                    "parallel_workers": 2,
                    "timeout_minutes": 15,
                }
            }

    @classmethod
    def get_monitoring_config(cls) -> Dict[str, Any]:
        """
        모니터링 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            모니터링 설정 딕셔너리
        """
        try:
            monitoring_configs = json.loads(Variable.get("execution_monitoring_configs", deserialize_json=True, default_var={}))
            return monitoring_configs
        except:
            # fallback: 기본값
            return {
                "monitoring_enabled": True,
                "alert_thresholds": {
                    "execution_time_minutes": 60,
                    "success_rate_percent": 90.0,
                    "error_count": 3,
                    "memory_usage_mb": 2048
                }
            }

    @classmethod
    def get_dag_configs(cls) -> Dict[str, Any]:
        """
        DAG 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            DAG 설정 딕셔너리
        """
        try:
            dag_configs = json.loads(Variable.get("dag_configs", deserialize_json=True, default_var={}))
            return dag_configs
        except:
            # fallback: 기본값
            return {}

    @classmethod
    def validate_config(cls, config_type: str, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        설정의 유효성을 검증하는 공통 메서드

        Args:
            config_type: 설정 타입
            config_data: 검증할 설정 데이터

        Returns:
            검증 결과 딕셔너리
        """
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }
        
        # 기본 검증 로직
        if not config_data:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"{config_type} 설정이 비어있습니다")
            return validation_result
        
        # 설정 타입별 특화 검증
        if config_type == "table_configs":
            validation_result = cls._validate_table_configs(config_data)
        elif config_type == "chunk_mode_configs":
            validation_result = cls._validate_chunk_mode_configs(config_data)
        elif config_type == "sync_mode_configs":
            validation_result = cls._validate_sync_mode_configs(config_data)
        
        return validation_result

    @classmethod
    def _validate_table_configs(cls, table_configs: Dict[str, Any]) -> Dict[str, Any]:
        """테이블 설정 검증"""
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }
        
        for table_name, config in table_configs.items():
            # 필수 필드 확인
            required_fields = ["source", "target", "primary_key", "sync_mode"]
            for field in required_fields:
                if field not in config:
                    validation_result["is_valid"] = False
                    validation_result["errors"].append(f"{table_name}: 필수 필드 누락 - {field}")
            
            # 청크 모드 설정 검증
            if config.get("chunk_mode") and config.get("batch_size", 0) <= 0:
                validation_result["is_valid"] = False
                validation_result["errors"].append(f"{table_name}: 청크 모드 활성화 시 배치 크기는 0보다 커야 함")
        
        return validation_result

    @classmethod
    def _validate_chunk_mode_configs(cls, chunk_configs: Dict[str, Any]) -> Dict[str, Any]:
        """청크 모드 설정 검증"""
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }
        
        # 기본 설정 확인
        if "default_settings" not in chunk_configs:
            validation_result["warnings"].append("기본 청크 모드 설정이 없습니다")
        
        return validation_result

    @classmethod
    def _validate_sync_mode_configs(cls, sync_configs: Dict[str, Any]) -> Dict[str, Any]:
        """동기화 모드 설정 검증"""
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }
        
        # 필수 동기화 모드 확인
        required_modes = ["full_sync", "incremental_sync"]
        for mode in required_modes:
            if mode not in sync_configs:
                validation_result["warnings"].append(f"권장 동기화 모드 누락: {mode}")
        
        return validation_result


class SettingsConnectionManager:
    """데이터베이스 연결을 관리하는 클래스 (deprecated - 새로운 connection_manager.py 사용)"""

    @staticmethod
    def get_source_connection_id() -> str:
        """소스 데이터베이스 연결 ID를 반환"""
        try:
            conn_ids = DAGSettings.get_connection_ids()
            return conn_ids.get("source_postgres", "fs2_postgres")
        except:
            return "fs2_postgres"

    @staticmethod
    def get_target_connection_id() -> str:
        """타겟 데이터베이스 연결 ID를 반환"""
        try:
            conn_ids = DAGSettings.get_connection_ids()
            return conn_ids.get("target_postgres", "postgres_default")
        except:
            return "postgres_default"

    @staticmethod
    def get_airflow_db_connection_id() -> str:
        """Airflow 데이터베이스 연결 ID를 반환"""
        try:
            conn_ids = DAGSettings.get_connection_ids()
            return conn_ids.get("airflow_db", "airflow_db")
        except:
            return "airflow_db"

    @classmethod
    def get_connection_config(cls, conn_id: str) -> Dict[str, Any]:
        """
        특정 연결의 설정을 반환 (Airflow Variables에서 가져옴)

        Args:
            conn_id: 연결 ID

        Returns:
            연결 설정 딕셔너리
        """
        try:
            # Airflow Variables에서 연결 정보 가져오기 시도
            conn_info = Variable.get(f"connection_{conn_id}", deserialize_json=True, default_var={})
            if conn_info:
                return conn_info
        except Exception:
            pass

        # fallback: 환경변수에서 기본값
        env_host = os.getenv(f"{conn_id.upper()}_HOST")
        env_port = os.getenv(f"{conn_id.upper()}_PORT")
        env_db = os.getenv(f"{conn_id.upper()}_DB")
        env_user = os.getenv(f"{conn_id.upper()}_USER")
        env_password = os.getenv(f"{conn_id.upper()}_PASSWORD")
        
        if env_host and env_port and env_db:
            return {
                "host": env_host,
                "port": int(env_port),
                "database": env_db,
                "user": env_user,
                "password": env_password,
                "schema": "public"
            }

        # 최종 fallback: 내부 DB 기본값
        default_connections = {
            "fs2_postgres": {
                "host": os.getenv("SOURCE_POSTGRES_HOST", "source_postgres"),
                "port": int(os.getenv("SOURCE_POSTGRES_PORT", "5432")),
                "database": os.getenv("SOURCE_POSTGRES_DB", "source_db"),
                "schema": "public",
            },
            "postgres_default": {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", "15432")),
                "database": os.getenv("POSTGRES_DB", "airflow"),
                "schema": "raw_data",
            },
            "airflow_db": {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", "15432")),
                "database": os.getenv("POSTGRES_DB", "airflow"),
                "schema": "raw_data",
            },
        }

        return default_connections.get(conn_id, {})


class MonitoringSettings:
    """모니터링 설정을 관리하는 클래스 - Airflow Variables 기반"""

    @staticmethod
    def get_monitoring_config() -> Dict[str, Any]:
        """
        모니터링 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            모니터링 설정 딕셔너리
        """
        try:
            monitoring_configs = DAGSettings.get_monitoring_config()
            return monitoring_configs
        except:
            # fallback: 환경변수에서 기본값
            return {
                "enable_notifications": os.getenv("ENABLE_MONITORING_NOTIFICATIONS", "true").lower() == "true",
                "notification_channels": os.getenv("MONITORING_CHANNELS", "log,console").split(","),
                "performance_thresholds": {
                    "copy_time_seconds": int(os.getenv("COPY_TIME_THRESHOLD_SECONDS", "300")),
                    "memory_usage_mb": int(os.getenv("MEMORY_USAGE_THRESHOLD_MB", "1024")),
                    "error_rate_percent": float(os.getenv("ERROR_RATE_THRESHOLD_PERCENT", "5.0")),
                },
                "retention_days": int(os.getenv("MONITORING_RETENTION_DAYS", "30")),
            }

    @staticmethod
    def get_alert_emails() -> List[str]:
        """알림 이메일 목록을 반환"""
        try:
            monitoring_configs = DAGSettings.get_monitoring_config()
            return monitoring_configs.get("notification_channels", {}).get("email", ["admin@example.com"])
        except:
            # fallback: 환경변수에서 기본값
            emails = os.getenv("ALERT_EMAILS", "admin@example.com")
            return [email.strip() for email in emails.split(",")]


class BatchSettings:
    """배치 처리 설정을 관리하는 클래스 - Airflow Variables 기반"""

    @staticmethod
    def get_batch_config() -> Dict[str, Any]:
        """
        배치 처리 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            배치 설정 딕셔너리
        """
        try:
            env_config = DAGSettings.get_environment_config()
            return {
                "default_batch_size": env_config.get("batch_size", 1000),
                "max_batch_size": int(os.getenv("MAX_BATCH_SIZE", "50000")),
                "min_batch_size": int(os.getenv("MIN_BATCH_SIZE", "100")),
                "batch_timeout_seconds": int(os.getenv("BATCH_TIMEOUT_SECONDS", "300")),
                "parallel_workers": int(os.getenv("PARALLEL_WORKERS", "4")),
                "memory_limit_mb": int(os.getenv("MEMORY_LIMIT_MB", "2048")),
            }
        except:
            # fallback: 환경변수에서 기본값
            return {
                "default_batch_size": int(os.getenv("DEFAULT_BATCH_SIZE", "1000")),
                "max_batch_size": int(os.getenv("MAX_BATCH_SIZE", "50000")),
                "min_batch_size": int(os.getenv("MIN_BATCH_SIZE", "100")),
                "batch_timeout_seconds": int(os.getenv("BATCH_TIMEOUT_SECONDS", "300")),
                "parallel_workers": int(os.getenv("PARALLEL_WORKERS", "4")),
                "memory_limit_mb": int(os.getenv("MEMORY_LIMIT_MB", "2048")),
            }

    @staticmethod
    def get_worker_config() -> Dict[str, Any]:
        """
        워커 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            워커 설정 딕셔너리
        """
        try:
            # Airflow Variables에서 워커 설정 가져오기 시도
            worker_configs = json.loads(Variable.get("worker_configs", deserialize_json=True, default_var={}))
            return worker_configs
        except:
            # fallback: 환경변수에서 기본값
            return {
                "default_workers": int(os.getenv("DEFAULT_WORKERS", "4")),
                "max_workers": int(os.getenv("MAX_WORKERS", "4")),
                "min_workers": int(os.getenv("MIN_WORKERS", "2")),
                "worker_timeout_seconds": int(os.getenv("WORKER_TIMEOUT_SECONDS", "600")),
                "worker_memory_limit_mb": int(os.getenv("WORKER_MEMORY_LIMIT_MB", "1024")),
            }

    @staticmethod
    def get_performance_optimization_config() -> Dict[str, Any]:
        """
        성능 최적화 설정을 반환 (Airflow Variables에서 가져옴)

        Returns:
            성능 최적화 설정 딕셔너리
        """
        try:
            # Airflow Variables에서 성능 설정 가져오기 시도
            perf_configs = json.loads(Variable.get("performance_configs", deserialize_json=True, default_var={}))
            return perf_configs
        except:
            # fallback: 환경변수에서 기본값
            return {
                "enable_session_optimization": os.getenv("ENABLE_SESSION_OPTIMIZATION", "true").lower() == "true",
                "enable_unlogged_staging": os.getenv("ENABLE_UNLOGGED_STAGING", "true").lower() == "true",
                "enable_auto_analyze": os.getenv("ENABLE_AUTO_ANALYZE", "true").lower() == "true",
                "enable_auto_index": os.getenv("ENABLE_AUTO_INDEX", "true").lower() == "true",
                "enable_streaming_pipe": os.getenv("ENABLE_STREAMING_PIPE", "true").lower() == "true",
                "session_parameters": {
                    "synchronous_commit": "off",
                    "statement_timeout": "0",
                    "work_mem": "128MB",
                    "lock_timeout": "300s",
                },
                "batch_size_optimization": {
                    "large_table_threshold": int(os.getenv("LARGE_TABLE_THRESHOLD", "1000000")),
                    "large_table_batch_size": int(os.getenv("LARGE_TABLE_BATCH_SIZE", "50000")),
                    "medium_table_threshold": int(os.getenv("MEDIUM_TABLE_THRESHOLD", "100000")),
                    "medium_table_batch_size": int(os.getenv("MEDIUM_TABLE_BATCH_SIZE", "20000")),
                    "small_table_batch_size": int(os.getenv("SMALL_TABLE_BATCH_SIZE", "10000")),
                },
                "parallel_processing": {
                    "max_concurrent_tables": int(os.getenv("MAX_CONCURRENT_TABLES", "2")),
                    "max_concurrent_chunks": int(os.getenv("MAX_CONCURRENT_CHUNKS", "4")),
                    "pool_name": os.getenv("DB_COPY_POOL_NAME", "postgres_copy_pool"),
                }
            }

    @staticmethod
    def get_table_specific_batch_size(table_name: str) -> int:
        """
        테이블별 배치 크기를 반환 (Airflow Variables에서 가져옴)

        Args:
            table_name: 테이블명

        Returns:
            배치 크기
        """
        try:
            # Airflow Variables에서 테이블별 배치 크기 가져오기 시도
            table_batch_configs = json.loads(Variable.get("table_batch_configs", deserialize_json=True, default_var={}))
            for key, batch_size in table_batch_configs.items():
                if key.lower() in table_name.lower():
                    return batch_size
        except:
            pass
        
        # fallback: 환경변수에서 기본값
        table_batch_sizes = {
            "인포맥스종목마스터": int(os.getenv("INFOMAX_BATCH_SIZE", "10000")),
            "ff_v3_ff_sec_entity": int(os.getenv("FF_ENTITY_BATCH_SIZE", "20000")),
            "edi_690": int(os.getenv("EDI_BATCH_SIZE", "10000")),
        }
        
        for key, batch_size in table_batch_sizes.items():
            if key.lower() in table_name.lower():
                return batch_size
        
        # 최종 fallback: 환경 설정에서 기본값
        try:
            env_config = DAGSettings.get_environment_config()
            return env_config.get("batch_size", 1000)
        except:
            return 1000


# 모든 설정값은 Airflow Variables에서 관리
# 이 파일은 설정을 가져오는 기능만 제공 