#!/usr/bin/env python3
"""
청크 방식 데이터 복사를 위한 테이블 설정 예시

이 파일은 DAG에서 청크 방식 데이터 복사를 사용하기 위한
테이블별 설정 예시를 보여줍니다.
"""

# 청크 방식 데이터 복사를 위한 테이블 설정 예시
CHUNK_MODE_TABLES_CONFIG = {
    "large_table_example": {
        "source": "source_schema.large_table",
        "target": "target_schema.large_table",
        "primary_key": ["id"],
        "sync_mode": "incremental_sync",
        "batch_size": 10000,
        "custom_where": "status = 'active'",
        # 청크 방식 설정 (대용량 테이블용)
        "chunk_mode": True,           # 청크 방식 활성화 (권장)
        "enable_checkpoint": True,    # 체크포인트 활성화 (권장)
        "max_retries": 5,            # 최대 재시도 횟수 (대용량 테이블은 더 많이)
        "description": "대용량 테이블 - 청크 방식으로 안전하게 처리"
    },
    
    "medium_table_example": {
        "source": "source_schema.medium_table",
        "target": "target_schema.medium_table",
        "primary_key": ["id"],
        "sync_mode": "incremental_sync",
        "batch_size": 5000,
        "custom_where": "updated_at >= '2024-01-01'",
        # 청크 방식 설정 (중간 크기 테이블용)
        "chunk_mode": True,           # 청크 방식 활성화
        "enable_checkpoint": True,    # 체크포인트 활성화
        "max_retries": 3,            # 기본 재시도 횟수
        "description": "중간 크기 테이블 - 청크 방식으로 효율적으로 처리"
    },
    
    "small_table_example": {
        "source": "source_schema.small_table",
        "target": "target_schema.small_table",
        "primary_key": ["id"],
        "sync_mode": "full_sync",
        "batch_size": 1000,
        "custom_where": None,
        # 청크 방식 설정 (소용량 테이블용)
        "chunk_mode": False,          # 기존 방식 사용 (빠른 처리)
        "enable_checkpoint": False,   # 체크포인트 비활성화
        "max_retries": 2,            # 적은 재시도 횟수
        "description": "소용량 테이블 - 기존 방식으로 빠르게 처리"
    },
    
    "xmin_table_example": {
        "source": "source_schema.xmin_table",
        "target": "target_schema.xmin_table",
        "primary_key": ["id"],
        "sync_mode": "xmin_incremental",
        "batch_size": 5000,
        "custom_where": "status != 'deleted'",
        # 청크 방식 설정 (xmin 기반 테이블용)
        "chunk_mode": True,           # 청크 방식 활성화 (xmin 처리에 중요)
        "enable_checkpoint": True,    # 체크포인트 활성화 (xmin 무결성 보장)
        "max_retries": 4,            # xmin 처리 실패 시 재시도
        "description": "xmin 기반 테이블 - 청크 방식으로 안전하게 처리"
    },
    
    "edi_table_example": {
        "source": "source_schema.edi_table",
        "target": "target_schema.edi_table",
        "primary_key": ["edi_id"],
        "sync_mode": "incremental_sync",
        "batch_size": 8000,
        "custom_where": "changed >= '20250812'",
        "incremental_field": "changed",
        "incremental_field_type": "yyyymmdd",
        # 청크 방식 설정 (EDI 테이블용)
        "chunk_mode": True,           # 청크 방식 활성화 (EDI 데이터 무결성)
        "enable_checkpoint": True,    # 체크포인트 활성화 (EDI 처리 중단 시 복구)
        "max_retries": 3,            # 기본 재시도 횟수
        "description": "EDI 테이블 - 청크 방식으로 안전하게 처리"
    }
}

# 환경별 설정 예시
ENVIRONMENT_CONFIGS = {
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

# 테이블 크기별 권장 설정
SIZE_BASED_RECOMMENDATIONS = {
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
}

# 사용법 예시
def get_table_config_with_chunk_mode(table_name: str, estimated_rows: int) -> dict:
    """
    테이블 크기에 따른 청크 방식 설정 자동 생성
    
    Args:
        table_name: 테이블명
        estimated_rows: 예상 행 수
        
    Returns:
        청크 방식 설정이 포함된 테이블 설정
    """
    # 테이블 크기 분류
    if estimated_rows <= 100000:
        size_category = "small"
    elif estimated_rows <= 1000000:
        size_category = "medium"
    elif estimated_rows <= 10000000:
        size_category = "large"
    else:
        size_category = "xlarge"
    
    # 권장 설정 가져오기
    recommendations = SIZE_BASED_RECOMMENDATIONS[size_category]
    
    # 기본 설정 생성
    config = {
        "source": f"source_schema.{table_name}",
        "target": f"target_schema.{table_name}",
        "primary_key": ["id"],  # 기본값, 필요시 수정
        "sync_mode": "incremental_sync",  # 기본값, 필요시 수정
        "batch_size": recommendations["batch_size"],
        "custom_where": None,  # 필요시 수정
        # 청크 방식 설정 자동 적용
        "chunk_mode": recommendations["chunk_mode"],
        "enable_checkpoint": recommendations["enable_checkpoint"],
        "max_retries": recommendations["max_retries"],
        "description": f"{table_name} - {recommendations['reason']}"
    }
    
    return config

# 설정 검증 함수
def validate_chunk_mode_config(config: dict) -> list[str]:
    """
    청크 방식 설정의 유효성 검증
    
    Args:
        config: 테이블 설정
        
    Returns:
        검증 오류 메시지 리스트
    """
    errors = []
    
    # 필수 필드 확인
    required_fields = ["source", "target", "primary_key", "sync_mode", "batch_size"]
    for field in required_fields:
        if field not in config:
            errors.append(f"필수 필드 누락: {field}")
    
    # 청크 방식 관련 필드 확인
    chunk_fields = ["chunk_mode", "enable_checkpoint", "max_retries"]
    for field in chunk_fields:
        if field not in config:
            errors.append(f"청크 방식 필드 누락: {field}")
    
    # 값 유효성 검증
    if config.get("chunk_mode") and config.get("batch_size", 0) <= 0:
        errors.append("청크 모드 활성화 시 배치 크기는 0보다 커야 함")
    
    if config.get("max_retries", 0) < 0:
        errors.append("최대 재시도 횟수는 0 이상이어야 함")
    
    if config.get("enable_checkpoint") and not config.get("chunk_mode"):
        errors.append("체크포인트는 청크 모드가 활성화된 경우에만 사용 가능")
    
    return errors

# 사용 예시
if __name__ == "__main__":
    # 테이블 크기별 설정 예시
    print("=== 테이블 크기별 권장 설정 ===")
    for size, config in SIZE_BASED_RECOMMENDATIONS.items():
        print(f"{size.upper()}: {config}")
    
    print("\n=== 자동 설정 생성 예시 ===")
    example_config = get_table_config_with_chunk_mode("example_table", 5000000)
    print(f"500만 행 테이블 설정: {example_config}")
    
    print("\n=== 설정 검증 예시 ===")
    errors = validate_chunk_mode_config(example_config)
    if errors:
        print(f"검증 오류: {errors}")
    else:
        print("설정 검증 통과") 