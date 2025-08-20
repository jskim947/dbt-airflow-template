"""
Settings Module
DAG 설정과 연결 정보를 관리하는 공통 설정 모듈
"""

import os
from typing import Any, Dict, List

from airflow.models import Variable


class DAGSettings:
    """DAG 설정을 관리하는 클래스"""

    # 기본 DAG 설정
    DEFAULT_DAG_CONFIG = {
        "owner": "data_team",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay_minutes": 5,
        "email": ["admin@example.com"],
        "start_date": "2024-01-01",
        "catchup": False,
        "max_active_runs": 1,
    }

    # 기본 태그
    DEFAULT_TAGS = ["postgres", "data-copy", "etl", "refactored"]

    @classmethod
    def get_connection_ids(cls) -> Dict[str, str]:
        """
        기본 연결 ID를 반환

        Returns:
            연결 ID 딕셔너리
        """
        return {
            "source_postgres": os.getenv("SOURCE_POSTGRES_CONN_ID", "fs2_postgres"),
            "target_postgres": os.getenv("TARGET_POSTGRES_CONN_ID", "postgres_default"),
            "airflow_db": os.getenv("AIRFLOW_DB_CONN_ID", "airflow_db"),
        }

    @classmethod
    def get_dbt_settings(cls) -> Dict[str, Any]:
        """
        dbt 관련 설정을 반환

        Returns:
            dbt 설정 딕셔너리
        """
        return {
            "project_path": os.getenv("DBT_PROJECT_PATH", "/opt/airflow/dbt"),
            "profile_name": os.getenv("DBT_PROFILE_NAME", "postgres_data_copy"),
            "target_name": os.getenv("DBT_TARGET_NAME", "dev"),
        }

    @classmethod
    def get_table_configs(cls) -> List[Dict[str, Any]]:
        """
        기본 테이블 설정을 반환 (청크 방식 지원)

        Returns:
            테이블 설정 목록
        """
        return [
            {
                "source": "fds_팩셋.인포맥스종목마스터",
                "target": "raw_data.인포맥스종목마스터",
                "primary_key": ["인포맥스코드", "팩셋거래소", "gts_exnm", "티커"],
                "sync_mode": "full_sync",
                "batch_size": 10000,  # 청크 방식에 적합한 배치 크기로 조정
                # 청크 방식 설정 (대용량 테이블용)
                "chunk_mode": True,           # 청크 방식 활성화 (권장)
                "enable_checkpoint": True,    # 체크포인트 활성화 (권장)
                "max_retries": 5,            # 최대 재시도 횟수 (대용량 테이블은 더 많이)
                "description": "인포맥스 종목 마스터 - 청크 방식으로 안전하게 처리"
            },
            {
                "source": "fds_copy.ff_v3_ff_sec_entity",
                "target": "raw_data.ff_v3_ff_sec_entity",
                "primary_key": ["fsym_id"],
                "sync_mode": "full_sync",
                "batch_size": 20000,  # 청크 방식에 적합한 배치 크기로 조정
                # 청크 방식 설정 (대용량 테이블용)
                "chunk_mode": True,           # 청크 방식 활성화 (권장)
                "enable_checkpoint": True,    # 체크포인트 활성화 (권장)
                "max_retries": 5,            # 최대 재시도 횟수 (대용량 테이블은 더 많이)
                "description": "FF v3 보안 엔티티 - 청크 방식으로 안전하게 처리"
            },
            {
                "source": "fds_copy.sym_v1_sym_ticker_exchange",
                "target": "raw_data.sym_v1_sym_ticker_exchange",
                "primary_key": ["fsym_id"],
                "sync_mode": "full_sync",
                "batch_size": 10000,  # 청크 방식에 적합한 배치 크기로 조정
                "custom_where": "SPLIT_PART(ticker_exchange, '-', 2) IN ('AMS','BRU','FRA','HKG','HSTC','JAS','JKT','KRX','LIS','LON','NAS','NYS','PAR','ROCO','SES','SHE','SHG','STC','TAI','TKS','TSE')",
                # 청크 방식 설정 (중간 크기 테이블용)
                "chunk_mode": True,           # 청크 방식 활성화
                "enable_checkpoint": True,    # 체크포인트 활성화
                "max_retries": 3,            # 기본 재시도 횟수
                "description": "심볼 티커 거래소 - 청크 방식으로 안전하게 처리"
            },
            {
                "source": "fds_copy.sym_v1_sym_coverage",
                "target": "raw_data.sym_v1_sym_coverage",
                "primary_key": ["fsym_id"],
                "sync_mode": "full_sync",
                "batch_size": 5000,  # 청크 방식에 적합한 배치 크기로 조정
                "custom_where": "universe_type = 'EQ' AND fsym_id like '%-L'",
                # 청크 방식 설정 (중간 크기 테이블용)
                "chunk_mode": True,           # 청크 방식 활성화
                "enable_checkpoint": True,    # 체크포인트 활성화
                "max_retries": 3,            # 기본 재시도 횟수
                "description": "심볼 커버리지 - 청크 방식으로 안전하게 처리"
            },
        ]

    @classmethod
    def get_edi_table_configs(cls) -> List[Dict[str, Any]]:
        """
        EDI 테이블 설정을 반환 (청크 방식 지원)

        Returns:
            EDI 테이블 설정 목록
        """
        return [
            {
                "source": "m23.edi_690",
                "target": "raw_data.edi_690",
                "primary_key": ["eventcd", "eventid", "optionid", "serialid", "scexhid", "sedolid"],
                "sync_mode": "incremental_sync",
                "batch_size": 10000,
                "incremental_field": "changed",
                "incremental_field_type": "yyyymmdd",
                "custom_where": "changed >= '20250812'",
                "where_clause": "changed >= '20250812'",
                # 청크 방식 설정 (EDI 테이블용)
                "chunk_mode": True,           # 청크 방식 활성화 (EDI 데이터 무결성)
                "enable_checkpoint": True,    # 체크포인트 활성화 (EDI 처리 중단 시 복구)
                "max_retries": 3,            # 기본 재시도 횟수
                "description": "EDI 690 이벤트 데이터 - 청크 방식으로 안전하게 처리"
            },
        ]

    @classmethod
    def get_schedule_intervals(cls) -> Dict[str, str]:
        """
        스케줄 간격 설정을 반환

        Returns:
            스케줄 간격 딕셔너리
        """
        return {
            "main_copy": "0 3,16 * * *",  # 매일 오전 3시, 오후 4시
            "edi_copy": "0 9 * * *",       # 매일 9시
            "daily": "0 2 * * *",          # 매일 오전 2시
            "hourly": "0 * * * *",         # 매시간
            "manual": "@once",             # 수동 실행
        }

    @classmethod
    def get_environment_config(cls) -> Dict[str, Any]:
        """
        환경별 설정을 반환

        Returns:
            환경 설정 딕셔너리
        """
        env = os.getenv("AIRFLOW_ENV", "development").lower()
        
        configs = {
            "development": {
                "debug": True,
                "log_level": "DEBUG",
                "email_notifications": False,
                "retries": 1,
                "batch_size": 1000,
            },
            "staging": {
                "debug": False,
                "log_level": "INFO",
                "email_notifications": True,
                "retries": 2,
                "batch_size": 5000,
            },
            "production": {
                "debug": False,
                "log_level": "WARNING",
                "email_notifications": True,
                "retries": 3,
                "batch_size": 10000,
            }
        }
        
        return configs.get(env, configs["development"])

    @classmethod
    def _get_environment_batch_size(cls, table_size_category: str) -> int:
        """
        환경과 테이블 크기에 따른 배치 크기를 반환
        
        Args:
            table_size_category: 테이블 크기 카테고리 (small, medium, large, xlarge)
            
        Returns:
            환경별 최적화된 배치 크기
        """
        env_config = cls.get_environment_config()
        base_batch_size = env_config["batch_size"]
        
        # 테이블 크기별 배치 크기 배율
        size_multipliers = {
            "small": 1.0,      # 기본 배치 크기
            "medium": 2.0,     # 2배
            "large": 5.0,      # 5배
            "xlarge": 10.0     # 10배
        }
        
        multiplier = size_multipliers.get(table_size_category, 1.0)
        return int(base_batch_size * multiplier)

    @classmethod
    def get_chunk_mode_configs(cls) -> Dict[str, Any]:
        """
        청크 방식 데이터 복사 설정을 반환
        
        Returns:
            청크 방식 설정 딕셔너리
        """
        return {
            "default_settings": {
                "chunk_mode": True,           # 기본값: 청크 방식 활성화
                "enable_checkpoint": True,    # 기본값: 체크포인트 활성화
                "max_retries": 3,            # 기본값: 최대 3회 재시도
                "description": "기본 청크 방식 설정 - 모든 테이블에 적용"
            },
            "size_based_recommendations": {
                "small": {  # 10만 행 이하
                    "chunk_mode": False,          # 기존 방식 권장
                    "enable_checkpoint": False,   # 체크포인트 불필요
                    "batch_size": 1000,          # 작은 배치
                    "max_retries": 2,            # 적은 재시도
                    "reason": "소용량 테이블은 기존 방식이 더 빠름"
                },
                "medium": {  # 10만 ~ 100만 행
                    "chunk_mode": True,           # 청크 방식 권장
                    "enable_checkpoint": True,    # 체크포인트 권장
                    "batch_size": 5000,          # 중간 배치
                    "max_retries": 3,            # 기본 재시도
                    "reason": "중간 크기 테이블은 청크 방식으로 안전하게 처리"
                },
                "large": {  # 100만 ~ 1000만 행
                    "chunk_mode": True,           # 청크 방식 필수
                    "enable_checkpoint": True,    # 체크포인트 필수
                    "batch_size": 10000,         # 큰 배치
                    "max_retries": 5,            # 많은 재시도
                    "reason": "대용량 테이블은 청크 방식으로만 안전하게 처리 가능"
                },
                "xlarge": {  # 1000만 행 이상
                    "chunk_mode": True,           # 청크 방식 필수
                    "enable_checkpoint": True,    # 체크포인트 필수
                    "batch_size": 15000,         # 매우 큰 배치
                    "max_retries": 7,            # 매우 많은 재시도
                    "reason": "초대용량 테이블은 청크 방식과 체크포인트가 필수"
                }
            },
            "environment_settings": {
                "development": {
                    "default_chunk_mode": True,
                    "default_enable_checkpoint": True,
                    "default_max_retries": 3,
                    "default_batch_size": 5000,
                    "description": "개발 환경 - 안전한 청크 방식 기본값"
                },
                "staging": {
                    "default_chunk_mode": True,
                    "default_enable_checkpoint": True,
                    "default_max_retries": 4,
                    "default_batch_size": 8000,
                    "description": "스테이징 환경 - 안전한 청크 방식, 중간 배치 크기"
                },
                "production": {
                    "default_chunk_mode": True,
                    "default_enable_checkpoint": True,
                    "default_max_retries": 5,
                    "default_batch_size": 10000,
                    "description": "프로덕션 환경 - 안전한 청크 방식, 최적화된 배치 크기"
                }
            }
        }

    @classmethod
    def validate_chunk_mode_config(cls, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        청크 방식 설정의 유효성을 검증
        
        Args:
            table_config: 테이블 설정
            
        Returns:
            검증 결과 딕셔너리
        """
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }
        
        # 필수 필드 확인
        required_fields = ["source", "target", "primary_key", "sync_mode", "batch_size"]
        for field in required_fields:
            if field not in table_config:
                validation_result["is_valid"] = False
                validation_result["errors"].append(f"필수 필드 누락: {field}")
        
        # 청크 방식 관련 필드 확인
        chunk_fields = ["chunk_mode", "enable_checkpoint", "max_retries"]
        for field in chunk_fields:
            if field not in table_config:
                validation_result["warnings"].append(f"청크 방식 필드 누락: {field} (기본값 사용)")
        
        # 값 유효성 검증
        if table_config.get("chunk_mode") and table_config.get("batch_size", 0) <= 0:
            validation_result["is_valid"] = False
            validation_result["errors"].append("청크 모드 활성화 시 배치 크기는 0보다 커야 함")
        
        if table_config.get("max_retries", 0) < 0:
            validation_result["is_valid"] = False
            validation_result["errors"].append("최대 재시도 횟수는 0 이상이어야 함")
        
        if table_config.get("enable_checkpoint") and not table_config.get("chunk_mode"):
            validation_result["is_valid"] = False
            validation_result["errors"].append("체크포인트는 청크 모드가 활성화된 경우에만 사용 가능")
        
        return validation_result

    @classmethod
    def get_xmin_table_configs(cls) -> List[Dict[str, Any]]:
        """
        xmin 기반 테이블 설정을 반환 (청크 방식 지원)
        
        Returns:
            xmin 기반 테이블 설정 목록
        """
        return [
            {
                "source": "fds.sym_v1_sym_ticker_exchange",
                "target": "raw_data.sym_v1_sym_ticker_exchange",
                "primary_key": ["fsym_id"],
                "sync_mode": "xmin_incremental",
                "batch_size": 5000,
                "xmin_tracking": True,
                "fallback_to_timestamp": True,
                "custom_where": "SPLIT_PART(ticker_exchange, '-', 2) IN ('AMS','BRU','FRA','HKG','HSTC','JAS','JKT','KRX','LIS','LON','NAS','NYS','PAR','ROCO','SES','SHE','SHG','STC','TAI','TKS','TSE')",  # 커스텀 WHERE 조건 (필요시 설정)
                "incremental_field": None,  # 폴백용 타임스탬프 필드
                "incremental_field_type": None,  # 타임스탬프 필드 타입
                # 청크 방식 설정 (xmin 기반 테이블용)
                "chunk_mode": True,           # 청크 방식 활성화 (xmin 처리에 중요)
                "enable_checkpoint": True,    # 체크포인트 활성화 (xmin 무결성 보장)
                "max_retries": 4,            # xmin 처리 실패 시 재시도
                "description": "sym_v1_sym_ticker_exchange 데이터 - xmin 기반 증분 동기화 (청크 방식)"
            }
            # {
            #     "source": "fds_copy.ff_v3_ff_entity",
            #     "target": "raw_data.ff_v3_ff_entity",
            #     "primary_key": ["fsym_id"],
            #     "sync_mode": "xmin_incremental",
            #     "batch_size": 10000,
            #     "xmin_tracking": True,
            #     "fallback_to_timestamp": True,
            #     "custom_where": "fsym_id IS NOT NULL",  # 예시: NULL이 아닌 데이터만
            #     "incremental_field": "last_updated",  # 폴백용 타임스탬프 필드
            #     "incremental_field_type": "timestamp",  # 타임스탬프 필드 타입
            #     # 청크 방식 설정
            #     "chunk_mode": True,
            #     "enable_checkpoint": True,
            #     "max_retries": 4,
            #     "description": "ff_v3_ff_entity 데이터 - xmin 기반 증분 동기화 (청크 방식)"
            # },
            # {
            #     "source": "m23.edi_690",
            #     "target": "raw_data.edi_690",
            #     "primary_key": ["eventcd", "eventid", "optionid", "serialid", "scexhid", "sedolid"],
            #     "sync_mode": "xmin_incremental",
            #     "batch_size": 8000,
            #     "xmin_tracking": True,
            #     "fallback_to_timestamp": True,
            #     "custom_where": "changed >= '20250812'",  # 특정 날짜 이후 데이터만
            #     "incremental_field": "changed",  # 폴백용 타임스탬프 필드
            #     "incremental_field_type": "yyyymmdd",  # 타임스탬프 필드 타입
            #     # 청크 방식 설정
            #     "chunk_mode": True,
            #     "enable_checkpoint": True,
            #     "max_retries": 4,
            #     "description": "EDI 690 이벤트 데이터 - xmin 기반 증분 동기화 (청크 방식)"
            # }
        ]

    @classmethod
    def get_xmin_settings(cls) -> Dict[str, Any]:
        """
        xmin 관련 설정을 반환
        
        Returns:
            xmin 설정 딕셔너리
        """
        return {
            "enable_xmin_tracking": os.getenv("ENABLE_XMIN_TRACKING", "true").lower() == "true",
            "xmin_stability_threshold": int(os.getenv("XMIN_STABILITY_THRESHOLD", "1500000000")),
            "xmin_critical_threshold": int(os.getenv("XMIN_CRITICAL_THRESHOLD", "2000000000")),
            "xmin_check_interval_hours": int(os.getenv("XMIN_CHECK_INTERVAL_HOURS", "24")),
            "enable_replication_check": os.getenv("ENABLE_REPLICATION_CHECK", "true").lower() == "true",
            "fallback_to_timestamp": os.getenv("FALLBACK_TO_TIMESTAMP", "true").lower() == "true",
            "xmin_column_name": "source_xmin",
            "xmin_index_name_prefix": "idx_xmin_",
            "batch_size_multiplier": float(os.getenv("XMIN_BATCH_SIZE_MULTIPLIER", "0.5")),
        }

    @classmethod
    def validate_xmin_config(cls, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        xmin 설정의 유효성을 검증
        
        Args:
            table_config: 테이블 설정
            
        Returns:
            검증 결과 딕셔너리
        """
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }
        
        # 필수 필드 확인
        required_fields = ["source", "target", "primary_key", "sync_mode"]
        for field in required_fields:
            if field not in table_config:
                validation_result["is_valid"] = False
                validation_result["errors"].append(f"필수 필드 누락: {field}")
        
        # xmin_incremental 모드 검증
        if table_config.get("sync_mode") == "xmin_incremental":
            # primary_key가 리스트인지 확인
            if not isinstance(table_config.get("primary_key"), list):
                validation_result["is_valid"] = False
                validation_result["errors"].append("primary_key는 리스트 형태여야 합니다")
            
            # batch_size가 적절한지 확인
            batch_size = table_config.get("batch_size", 5000)
            if batch_size > 10000:
                validation_result["warnings"].append("xmin 처리를 위한 배치 크기가 너무 큽니다. 5000 이하 권장")
            
            # xmin_tracking 설정 확인
            if not table_config.get("xmin_tracking", True):
                validation_result["warnings"].append("xmin_incremental 모드에서 xmin_tracking이 비활성화되어 있습니다")
        
        return validation_result

    @classmethod
    def get_hybrid_table_configs(cls) -> List[Dict[str, Any]]:
        """
        하이브리드 모드 테이블 설정 (xmin + 타임스탬프, 청크 방식 지원)
        
        Returns:
            하이브리드 테이블 설정 목록
        """
        return [
            {
                "source": "m23.edi_690",
                "target": "raw_data.edi_690",
                "primary_key": ["eventcd", "eventid", "optionid", "serialid", "scexhid", "sedolid"],
                "sync_mode": "hybrid_incremental",  # xmin + 타임스탬프
                "batch_size": 5000,
                "xmin_tracking": True,
                "timestamp_tracking": True,
                "incremental_field": "changed",  # 타임스탬프 폴백용
                "incremental_field_type": "yyyymmdd",
                "fallback_strategy": "timestamp_first",  # xmin 실패 시 타임스탬프 우선
                # 청크 방식 설정 (하이브리드 테이블용)
                "chunk_mode": True,           # 청크 방식 활성화 (하이브리드 처리에 중요)
                "enable_checkpoint": True,    # 체크포인트 활성화 (복잡한 처리 보장)
                "max_retries": 4,            # 하이브리드 처리 실패 시 재시도
                "description": "EDI 690 이벤트 데이터 - 하이브리드 증분 동기화 (청크 방식)"
            }
        ] 


class ConnectionManager:
    """데이터베이스 연결을 관리하는 클래스"""

    @staticmethod
    def get_source_connection_id() -> str:
        """소스 데이터베이스 연결 ID를 반환"""
        return DAGSettings.get_connection_ids()["source_postgres"]

    @staticmethod
    def get_target_connection_id() -> str:
        """타겟 데이터베이스 연결 ID를 반환"""
        return DAGSettings.get_connection_ids()["target_postgres"]

    @staticmethod
    def get_airflow_db_connection_id() -> str:
        """Airflow 데이터베이스 연결 ID를 반환"""
        return DAGSettings.get_connection_ids()["airflow_db"]

    @classmethod
    def get_connection_config(cls, conn_id: str) -> Dict[str, Any]:
        """
        특정 연결의 설정을 반환

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

        # 기본 연결 정보 반환
        default_connections = {
            "fs2_postgres": {
                "host": os.getenv("SOURCE_POSTGRES_HOST", "source_postgres"),
                "port": int(os.getenv("SOURCE_POSTGRES_PORT", "5432")),
                "database": os.getenv("SOURCE_POSTGRES_DB", "source_db"),
                "schema": "public",
            },
            "postgres_default": {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "database": os.getenv("POSTGRES_DB", "airflow"),
                "schema": "raw_data",
            },
            "airflow_db": {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "database": os.getenv("POSTGRES_DB", "airflow"),
                "schema": "raw_data",
            },
        }

        return default_connections.get(conn_id, {})


class MonitoringSettings:
    """모니터링 설정을 관리하는 클래스"""

    @staticmethod
    def get_monitoring_config() -> Dict[str, Any]:
        """
        모니터링 설정을 반환

        Returns:
            모니터링 설정 딕셔너리
        """
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
        emails = os.getenv("ALERT_EMAILS", "admin@example.com")
        return [email.strip() for email in emails.split(",")]


class BatchSettings:
    """배치 처리 설정을 관리하는 클래스"""

    @staticmethod
    def get_batch_config() -> Dict[str, Any]:
        """
        배치 처리 설정을 반환

        Returns:
            배치 설정 딕셔너리
        """
        env_config = DAGSettings.get_environment_config()
        
        return {
            "default_batch_size": env_config["batch_size"],
            "max_batch_size": int(os.getenv("MAX_BATCH_SIZE", "50000")),
            "min_batch_size": int(os.getenv("MIN_BATCH_SIZE", "100")),
            "batch_timeout_seconds": int(os.getenv("BATCH_TIMEOUT_SECONDS", "300")),
            "parallel_workers": int(os.getenv("PARALLEL_WORKERS", "4")),
            "memory_limit_mb": int(os.getenv("MEMORY_LIMIT_MB", "2048")),
        }

    @staticmethod
    def get_worker_config() -> Dict[str, Any]:
        """
        워커 설정을 반환 (성능 테스트 후 최적값으로 고정)

        Returns:
            워커 설정 딕셔너리
        """
        return {
            "default_workers": int(os.getenv("DEFAULT_WORKERS", "4")),  # 성능 테스트 결과: 최적값
            "max_workers": int(os.getenv("MAX_WORKERS", "4")),         # 성능 테스트 결과: 최적값
            "min_workers": int(os.getenv("MIN_WORKERS", "2")),         # 성능 테스트 결과: 최적값
            "worker_timeout_seconds": int(os.getenv("WORKER_TIMEOUT_SECONDS", "600")),
            "worker_memory_limit_mb": int(os.getenv("WORKER_MEMORY_LIMIT_MB", "1024")),
        }

    @staticmethod
    def get_performance_optimization_config() -> Dict[str, Any]:
        """
        성능 최적화 설정을 반환

        Returns:
            성능 최적화 설정 딕셔너리
        """
        return {
            "enable_session_optimization": os.getenv("ENABLE_SESSION_OPTIMIZATION", "true").lower() == "true",
            "enable_unlogged_staging": os.getenv("ENABLE_UNLOGGED_STAGING", "true").lower() == "true",
            "enable_auto_analyze": os.getenv("ENABLE_AUTO_ANALYZE", "true").lower() == "true",
            "enable_auto_index": os.getenv("ENABLE_AUTO_INDEX", "true").lower() == "true",
            "enable_streaming_pipe": os.getenv("ENABLE_STREAMING_PIPE", "true").lower() == "true",  # 기본 활성화
            "session_parameters": {
                "synchronous_commit": "off",
                "statement_timeout": "0",
                "work_mem": "128MB",
                "lock_timeout": "300s",
            },
            "batch_size_optimization": {
                "large_table_threshold": int(os.getenv("LARGE_TABLE_THRESHOLD", "1000000")),  # 100만 행 이상
                "large_table_batch_size": int(os.getenv("LARGE_TABLE_BATCH_SIZE", "50000")),  # 5만 행
                "medium_table_threshold": int(os.getenv("MEDIUM_TABLE_THRESHOLD", "100000")),  # 10만 행 이상
                "medium_table_batch_size": int(os.getenv("MEDIUM_TABLE_BATCH_SIZE", "20000")),  # 2만 행
                "small_table_batch_size": int(os.getenv("SMALL_TABLE_BATCH_SIZE", "10000")),   # 1만 행
            },
            "parallel_processing": {
                "max_concurrent_tables": int(os.getenv("MAX_CONCURRENT_TABLES", "2")),  # 동시 실행 테이블 수
                "max_concurrent_chunks": int(os.getenv("MAX_CONCURRENT_CHUNKS", "4")),  # 동시 실행 청크 수
                "pool_name": os.getenv("DB_COPY_POOL_NAME", "postgres_copy_pool"),
            }
        }

    @staticmethod
    def get_table_specific_batch_size(table_name: str) -> int:
        """
        테이블별 배치 크기를 반환

        Args:
            table_name: 테이블명

        Returns:
            배치 크기
        """
        # 테이블별 배치 크기 설정
        table_batch_sizes = {
            "인포맥스종목마스터": 10000,
            "ff_v3_ff_sec_entity": 20000,
            "edi_690": 10000,
        }
        
        # 테이블명에서 키 찾기
        for key, batch_size in table_batch_sizes.items():
            if key.lower() in table_name.lower():
                return batch_size
        
        # 기본값 반환
        return DAGSettings.get_environment_config()["batch_size"] 


# Removed deprecated XminTableConfig class; use DAGSettings methods above instead. 