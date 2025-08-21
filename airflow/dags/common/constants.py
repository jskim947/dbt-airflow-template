#!/usr/bin/env python3
"""
공통 상수 정의 모듈

코드 리뷰에서 제안된 개선사항들을 반영하여
공통으로 사용할 상수들을 정의합니다.
"""

# 청크 방식 기본 설정 상수
class ChunkModeDefaults:
    """청크 방식 기본 설정"""
    DEFAULT_CHUNK_MODE = True
    DEFAULT_ENABLE_CHECKPOINT = True
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_BATCH_SIZE = 10000

# 메모리 사용량 임계값 상수
class MemoryThresholds:
    """메모리 사용량 임계값 정의"""
    WARNING_MB = 500      # 경고 임계값 (500MB)
    CRITICAL_MB = 1000    # 위험 임계값 (1GB)
    EMERGENCY_MB = 1500   # 긴급 임계값 (1.5GB)

# 배치 크기 상수
class BatchSizeDefaults:
    """배치 크기 기본값"""
    SMALL_TABLE = 1000    # 소용량 테이블 (10만 행 이하)
    MEDIUM_TABLE = 5000   # 중간 크기 테이블 (10만~100만 행)
    LARGE_TABLE = 10000   # 대용량 테이블 (100만~1000만 행)
    XLARGE_TABLE = 15000  # 초대용량 테이블 (1000만 행 이상)

# 재시도 횟수 상수
class RetryDefaults:
    """재시도 횟수 기본값"""
    SMALL_TABLE = 2       # 소용량 테이블
    MEDIUM_TABLE = 3      # 중간 크기 테이블
    LARGE_TABLE = 5       # 대용량 테이블
    XLARGE_TABLE = 7      # 초대용량 테이블

# 환경별 설정 상수
class EnvironmentDefaults:
    """환경별 기본 설정"""
    DEVELOPMENT = {
        "debug": True,
        "log_level": "DEBUG",
        "email_notifications": False,
        "retries": 1,
        "batch_size": 1000,
    }
    STAGING = {
        "debug": False,
        "log_level": "INFO",
        "email_notifications": True,
        "retries": 2,
        "batch_size": 5000,
    }
    PRODUCTION = {
        "debug": False,
        "log_level": "WARNING",
        "email_notifications": True,
        "retries": 3,
        "batch_size": 10000,
    }

# 테이블 크기 분류 상수
class TableSizeCategories:
    """테이블 크기 분류 기준"""
    SMALL = 100000        # 10만 행 이하
    MEDIUM = 1000000      # 10만 ~ 100만 행
    LARGE = 10000000      # 100만 ~ 1000만 행
    XLARGE = 100000000    # 1000만 행 이상

# 동기화 모드 상수
class SyncModes:
    """동기화 모드 정의"""
    FULL_SYNC = "full_sync"
    INCREMENTAL_SYNC = "incremental_sync"
    XMIN_INCREMENTS = "xmin_incremental"
    HYBRID_INCREMENTS = "hybrid_incremental"

# 에러 타입 상수
class ErrorTypes:
    """에러 타입 분류"""
    CONNECTION_ERROR = "connection_error"
    DATA_ERROR = "data_error"
    INTERNAL_ERROR = "internal_error"
    TIMEOUT_ERROR = "timeout_error"
    MEMORY_ERROR = "memory_error"

# 로그 레벨 상수
class LogLevels:
    """로그 레벨 정의"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

# 파일 확장자 상수
class FileExtensions:
    """파일 확장자 정의"""
    CSV = ".csv"
    JSON = ".json"
    CHECKPOINT = ".checkpoint"
    BACKUP = ".backup"

# 데이터베이스 상수
class DatabaseDefaults:
    """데이터베이스 기본 설정"""
    DEFAULT_PORT = 15432  # 외부 포트로 변경
    DEFAULT_SCHEMA = "public"
    DEFAULT_TIMEOUT = 30
    DEFAULT_CHUNK_SIZE = 10000

# 성능 임계값 상수
class PerformanceThresholds:
    """성능 임계값 정의"""
    COPY_TIME_SECONDS = 300      # 복사 시간 임계값 (5분)
    MEMORY_USAGE_MB = 1024      # 메모리 사용량 임계값 (1GB)
    ERROR_RATE_PERCENT = 5.0    # 에러율 임계값 (5%)

# 설정 버전 상수
class ConfigVersions:
    """설정 버전 정보"""
    CURRENT_VERSION = "1.0.0"
    MIN_SUPPORTED_VERSION = "1.0.0"
    CONFIG_SCHEMA_VERSION = "1.0"

# 유틸리티 함수
def get_table_size_category(row_count: int) -> str:
    """
    행 수에 따른 테이블 크기 카테고리를 반환
    
    Args:
        row_count: 테이블의 행 수
        
    Returns:
        테이블 크기 카테고리 (small, medium, large, xlarge)
    """
    if row_count <= TableSizeCategories.SMALL:
        return "small"
    elif row_count <= TableSizeCategories.MEDIUM:
        return "medium"
    elif row_count <= TableSizeCategories.LARGE:
        return "large"
    else:
        return "xlarge"

def get_optimal_batch_size(table_size_category: str, environment: str = "production") -> int:
    """
    테이블 크기와 환경에 따른 최적 배치 크기를 반환
    
    Args:
        table_size_category: 테이블 크기 카테고리
        environment: 환경 (development, staging, production)
        
    Returns:
        최적화된 배치 크기
    """
    env_config = EnvironmentDefaults.__dict__.get(environment.upper(), EnvironmentDefaults.PRODUCTION)
    base_batch_size = env_config["batch_size"]
    
    # 테이블 크기별 배치 크기 배율
    size_multipliers = {
        "small": 1.0,
        "medium": 2.0,
        "large": 5.0,
        "xlarge": 10.0
    }
    
    multiplier = size_multipliers.get(table_size_category, 1.0)
    return int(base_batch_size * multiplier)

def get_optimal_retry_count(table_size_category: str) -> int:
    """
    테이블 크기에 따른 최적 재시도 횟수를 반환
    
    Args:
        table_size_category: 테이블 크기 카테고리
        
    Returns:
        최적화된 재시도 횟수
    """
    retry_mapping = {
        "small": RetryDefaults.SMALL_TABLE,
        "medium": RetryDefaults.MEDIUM_TABLE,
        "large": RetryDefaults.LARGE_TABLE,
        "xlarge": RetryDefaults.XLARGE_TABLE
    }
    
    return retry_mapping.get(table_size_category, RetryDefaults.MEDIUM_TABLE) 