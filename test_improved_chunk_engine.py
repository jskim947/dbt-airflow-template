#!/usr/bin/env python3
"""
개선된 청크 방식 데이터 복사 엔진 테스트

코드 리뷰에서 제안된 개선사항들이 반영된 후
기능이 정상적으로 동작하는지 테스트합니다.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
import json
import time

# 테스트할 모듈들
from airflow.dags.common.constants import (
    ChunkModeDefaults,
    MemoryThresholds,
    BatchSizeDefaults,
    RetryDefaults,
    get_table_size_category,
    get_optimal_batch_size,
    get_optimal_retry_count
)

class TestImprovedChunkEngine(unittest.TestCase):
    """개선된 청크 엔진 테스트 클래스"""

    def setUp(self):
        """테스트 설정"""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """테스트 정리"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_chunk_mode_defaults(self):
        """청크 방식 기본 설정 상수 테스트"""
        self.assertTrue(ChunkModeDefaults.DEFAULT_CHUNK_MODE)
        self.assertTrue(ChunkModeDefaults.DEFAULT_ENABLE_CHECKPOINT)
        self.assertEqual(ChunkModeDefaults.DEFAULT_MAX_RETRIES, 3)
        self.assertEqual(ChunkModeDefaults.DEFAULT_BATCH_SIZE, 10000)

    def test_memory_thresholds(self):
        """메모리 임계값 상수 테스트"""
        self.assertEqual(MemoryThresholds.WARNING_MB, 500)
        self.assertEqual(MemoryThresholds.CRITICAL_MB, 1000)
        self.assertEqual(MemoryThresholds.EMERGENCY_MB, 1500)
        
        # 임계값 순서 검증
        self.assertLess(MemoryThresholds.WARNING_MB, MemoryThresholds.CRITICAL_MB)
        self.assertLess(MemoryThresholds.CRITICAL_MB, MemoryThresholds.EMERGENCY_MB)

    def test_batch_size_defaults(self):
        """배치 크기 기본값 상수 테스트"""
        self.assertEqual(BatchSizeDefaults.SMALL_TABLE, 1000)
        self.assertEqual(BatchSizeDefaults.MEDIUM_TABLE, 5000)
        self.assertEqual(BatchSizeDefaults.LARGE_TABLE, 10000)
        self.assertEqual(BatchSizeDefaults.XLARGE_TABLE, 15000)
        
        # 배치 크기 순서 검증
        self.assertLess(BatchSizeDefaults.SMALL_TABLE, BatchSizeDefaults.MEDIUM_TABLE)
        self.assertLess(BatchSizeDefaults.MEDIUM_TABLE, BatchSizeDefaults.LARGE_TABLE)
        self.assertLess(BatchSizeDefaults.LARGE_TABLE, BatchSizeDefaults.XLARGE_TABLE)

    def test_retry_defaults(self):
        """재시도 횟수 기본값 상수 테스트"""
        self.assertEqual(RetryDefaults.SMALL_TABLE, 2)
        self.assertEqual(RetryDefaults.MEDIUM_TABLE, 3)
        self.assertEqual(RetryDefaults.LARGE_TABLE, 5)
        self.assertEqual(RetryDefaults.XLARGE_TABLE, 7)
        
        # 재시도 횟수 순서 검증
        self.assertLess(RetryDefaults.SMALL_TABLE, RetryDefaults.MEDIUM_TABLE)
        self.assertLess(RetryDefaults.MEDIUM_TABLE, RetryDefaults.LARGE_TABLE)
        self.assertLess(RetryDefaults.LARGE_TABLE, RetryDefaults.XLARGE_TABLE)

    def test_table_size_category_classification(self):
        """테이블 크기 분류 함수 테스트"""
        # 소용량 테이블
        self.assertEqual(get_table_size_category(50000), "small")
        self.assertEqual(get_table_size_category(100000), "small")
        
        # 중간 크기 테이블
        self.assertEqual(get_table_size_category(100001), "medium")
        self.assertEqual(get_table_size_category(500000), "medium")
        self.assertEqual(get_table_size_category(1000000), "medium")
        
        # 대용량 테이블
        self.assertEqual(get_table_size_category(1000001), "large")
        self.assertEqual(get_table_size_category(5000000), "large")
        self.assertEqual(get_table_size_category(10000000), "large")
        
        # 초대용량 테이블
        self.assertEqual(get_table_size_category(10000001), "xlarge")
        self.assertEqual(get_table_size_category(50000000), "xlarge")

    def test_optimal_batch_size_calculation(self):
        """최적 배치 크기 계산 함수 테스트"""
        # 개발 환경
        dev_batch_size = get_optimal_batch_size("medium", "development")
        self.assertEqual(dev_batch_size, 2000)  # 1000 * 2.0
        
        # 스테이징 환경
        staging_batch_size = get_optimal_batch_size("large", "staging")
        self.assertEqual(staging_batch_size, 25000)  # 5000 * 5.0
        
        # 프로덕션 환경
        prod_batch_size = get_optimal_batch_size("xlarge", "production")
        self.assertEqual(prod_batch_size, 100000)  # 10000 * 10.0
        
        # 기본값 (프로덕션)
        default_batch_size = get_optimal_batch_size("small")
        self.assertEqual(default_batch_size, 10000)  # 10000 * 1.0

    def test_optimal_retry_count_calculation(self):
        """최적 재시도 횟수 계산 함수 테스트"""
        self.assertEqual(get_optimal_retry_count("small"), 2)
        self.assertEqual(get_optimal_retry_count("medium"), 3)
        self.assertEqual(get_optimal_retry_count("large"), 5)
        self.assertEqual(get_optimal_retry_count("xlarge"), 7)
        
        # 알 수 없는 카테고리
        self.assertEqual(get_optimal_retry_count("unknown"), 3)  # 기본값

    def test_constants_consistency(self):
        """상수들 간의 일관성 테스트"""
        # 배치 크기와 재시도 횟수의 상관관계 검증
        # 일반적으로 배치 크기가 클수록 재시도 횟수도 많아야 함
        small_retries = get_optimal_retry_count("small")
        large_retries = get_optimal_retry_count("large")
        self.assertLess(small_retries, large_retries)
        
        # 메모리 임계값과 배치 크기의 상관관계 검증
        # 배치 크기가 클수록 메모리 사용량이 증가하므로 임계값도 높아야 함
        small_batch = BatchSizeDefaults.SMALL_TABLE
        large_batch = BatchSizeDefaults.LARGE_TABLE
        self.assertLess(small_batch, large_batch)

    def test_environment_defaults_structure(self):
        """환경별 기본 설정 구조 테스트"""
        from airflow.dags.common.constants import EnvironmentDefaults
        
        # 모든 환경에 필요한 키가 있는지 확인
        required_keys = ["debug", "log_level", "email_notifications", "retries", "batch_size"]
        
        for env_name in ["DEVELOPMENT", "STAGING", "PRODUCTION"]:
            env_config = getattr(EnvironmentDefaults, env_name)
            for key in required_keys:
                self.assertIn(key, env_config, f"{env_name}에 {key} 키가 없습니다")

    def test_sync_modes_constants(self):
        """동기화 모드 상수 테스트"""
        from airflow.dags.common.constants import SyncModes
        
        self.assertEqual(SyncModes.FULL_SYNC, "full_sync")
        self.assertEqual(SyncModes.INCREMENTAL_SYNC, "incremental_sync")
        self.assertEqual(SyncModes.XMIN_INCREMENTS, "xmin_incremental")
        self.assertEqual(SyncModes.HYBRID_INCREMENTS, "hybrid_incremental")
        
        # 모든 모드가 고유한 값인지 확인
        modes = [
            SyncModes.FULL_SYNC,
            SyncModes.INCREMENTAL_SYNC,
            SyncModes.XMIN_INCREMENTS,
            SyncModes.HYBRID_INCREMENTS
        ]
        self.assertEqual(len(modes), len(set(modes)))

    def test_error_types_constants(self):
        """에러 타입 상수 테스트"""
        from airflow.dags.common.constants import ErrorTypes
        
        self.assertEqual(ErrorTypes.CONNECTION_ERROR, "connection_error")
        self.assertEqual(ErrorTypes.DATA_ERROR, "data_error")
        self.assertEqual(ErrorTypes.INTERNAL_ERROR, "internal_error")
        self.assertEqual(ErrorTypes.TIMEOUT_ERROR, "timeout_error")
        self.assertEqual(ErrorTypes.MEMORY_ERROR, "memory_error")

    def test_file_extensions_constants(self):
        """파일 확장자 상수 테스트"""
        from airflow.dags.common.constants import FileExtensions
        
        self.assertEqual(FileExtensions.CSV, ".csv")
        self.assertEqual(FileExtensions.JSON, ".json")
        self.assertEqual(FileExtensions.CHECKPOINT, ".checkpoint")
        self.assertEqual(FileExtensions.BACKUP, ".backup")

    def test_database_defaults_constants(self):
        """데이터베이스 기본 설정 상수 테스트"""
        from airflow.dags.common.constants import DatabaseDefaults
        
        self.assertEqual(DatabaseDefaults.DEFAULT_PORT, 5432)
        self.assertEqual(DatabaseDefaults.DEFAULT_SCHEMA, "public")
        self.assertEqual(DatabaseDefaults.DEFAULT_TIMEOUT, 30)
        self.assertEqual(DatabaseDefaults.DEFAULT_CHUNK_SIZE, 10000)

    def test_performance_thresholds_constants(self):
        """성능 임계값 상수 테스트"""
        from airflow.dags.common.constants import PerformanceThresholds
        
        self.assertEqual(PerformanceThresholds.COPY_TIME_SECONDS, 300)
        self.assertEqual(PerformanceThresholds.MEMORY_USAGE_MB, 1024)
        self.assertEqual(PerformanceThresholds.ERROR_RATE_PERCENT, 5.0)

    def test_config_versions_constants(self):
        """설정 버전 상수 테스트"""
        from airflow.dags.common.constants import ConfigVersions
        
        self.assertEqual(ConfigVersions.CURRENT_VERSION, "1.0.0")
        self.assertEqual(ConfigVersions.MIN_SUPPORTED_VERSION, "1.0.0")
        self.assertEqual(ConfigVersions.CONFIG_SCHEMA_VERSION, "1.0")

    def test_constants_immutability(self):
        """상수 불변성 테스트"""
        # 상수들이 실제로 상수인지 확인 (런타임에 변경되지 않는지)
        original_warning = MemoryThresholds.WARNING_MB
        
        # 상수 변경 시도 (실제로는 변경되지 않아야 함)
        try:
            MemoryThresholds.WARNING_MB = 999
            # 만약 변경된다면 테스트 실패
            self.assertEqual(MemoryThresholds.WARNING_MB, original_warning)
        except Exception:
            # 상수가 변경 불가능하다면 예외 발생 (정상)
            pass
        
        # 원래 값 확인
        self.assertEqual(MemoryThresholds.WARNING_MB, original_warning)

def run_performance_test():
    """성능 테스트 실행"""
    print("🚀 성능 테스트 시작...")
    
    start_time = time.time()
    
    # 테이블 크기별 최적 배치 크기 계산 성능 테스트
    for i in range(10000):
        get_optimal_batch_size("large", "production")
        get_optimal_retry_count("xlarge")
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"✅ 성능 테스트 완료: {execution_time:.4f}초")
    print(f"   - 10,000회 함수 호출")
    print(f"   - 평균 실행 시간: {execution_time/10000:.6f}초/호출")
    
    return execution_time

def main():
    """메인 함수"""
    print("🧪 개선된 청크 엔진 테스트 시작")
    print("=" * 50)
    
    # 단위 테스트 실행
    print("1. 단위 테스트 실행...")
    unittest.main(argv=[''], exit=False, verbosity=2)
    
    print("\n" + "=" * 50)
    
    # 성능 테스트 실행
    print("2. 성능 테스트 실행...")
    performance_time = run_performance_test()
    
    print("\n" + "=" * 50)
    print("🎉 모든 테스트 완료!")
    
    # 테스트 결과 요약
    if performance_time < 1.0:  # 1초 이내 실행
        print("✅ 성능: 우수")
    elif performance_time < 5.0:  # 5초 이내 실행
        print("✅ 성능: 양호")
    else:
        print("⚠️  성능: 개선 필요")

if __name__ == "__main__":
    main() 