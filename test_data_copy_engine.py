#!/usr/bin/env python3
"""
DataCopyEngine 테스트 스크립트
새로 추가된 워커 설정과 xmin 관련 메서드들을 테스트
"""

import sys
import os
import logging

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_worker_config():
    """워커 설정 테스트"""
    try:
        from airflow.dags.common.settings import BatchSettings
        
        logger.info("=== 워커 설정 테스트 ===")
        
        # 워커 설정 가져오기
        worker_config = BatchSettings.get_worker_config()
        logger.info(f"워커 설정: {worker_config}")
        
        # 환경별 설정 확인
        env = os.getenv("AIRFLOW_ENV", "development")
        logger.info(f"현재 환경: {env}")
        
        # 기본값 확인
        assert "default_workers" in worker_config
        assert "max_workers" in worker_config
        assert "min_workers" in worker_config
        
        logger.info("✅ 워커 설정 테스트 통과")
        return True
        
    except Exception as e:
        logger.error(f"❌ 워커 설정 테스트 실패: {e}")
        return False

def test_batch_settings():
    """배치 설정 테스트"""
    try:
        from airflow.dags.common.settings import BatchSettings
        
        logger.info("=== 배치 설정 테스트 ===")
        
        # 배치 설정 가져오기
        batch_config = BatchSettings.get_batch_config()
        logger.info(f"배치 설정: {batch_config}")
        
        # 테이블별 배치 크기 확인
        table_batch_sizes = {
            "인포맥스종목마스터": BatchSettings.get_table_specific_batch_size("인포맥스종목마스터"),
            "ff_v3_ff_sec_entity": BatchSettings.get_table_specific_batch_size("ff_v3_ff_sec_entity"),
            "edi_690": BatchSettings.get_table_specific_batch_size("edi_690"),
        }
        
        logger.info(f"테이블별 배치 크기: {table_batch_sizes}")
        
        # 기본값 확인
        assert "default_batch_size" in batch_config
        assert "parallel_workers" in batch_config
        
        logger.info("✅ 배치 설정 테스트 통과")
        return True
        
    except Exception as e:
        logger.error(f"❌ 배치 설정 테스트 실패: {e}")
        return False

def test_xmin_settings():
    """xmin 설정 테스트"""
    try:
        from airflow.dags.common.settings import DAGSettings
        
        logger.info("=== xmin 설정 테스트 ===")
        
        # xmin 설정 가져오기
        xmin_settings = DAGSettings.get_xmin_settings()
        logger.info(f"xmin 설정: {xmin_settings}")
        
        # xmin 테이블 설정 가져오기
        xmin_table_configs = DAGSettings.get_xmin_table_configs()
        logger.info(f"xmin 테이블 설정: {xmin_table_configs}")
        
        # 기본값 확인
        assert "enable_xmin_tracking" in xmin_settings
        assert "xmin_column_name" in xmin_settings
        assert "batch_size_multiplier" in xmin_settings
        
        logger.info("✅ xmin 설정 테스트 통과")
        return True
        
    except Exception as e:
        logger.error(f"❌ xmin 설정 테스트 실패: {e}")
        return False

def test_environment_config():
    """환경 설정 테스트"""
    try:
        from airflow.dags.common.settings import DAGSettings
        
        logger.info("=== 환경 설정 테스트 ===")
        
        # 환경 설정 가져오기
        env_config = DAGSettings.get_environment_config()
        logger.info(f"환경 설정: {env_config}")
        
        # 환경별 배치 크기 확인
        env = os.getenv("AIRFLOW_ENV", "development")
        logger.info(f"현재 환경: {env}")
        logger.info(f"환경별 배치 크기: {env_config.get('batch_size', 'N/A')}")
        
        # 기본값 확인
        assert "batch_size" in env_config
        assert "debug" in env_config
        assert "log_level" in env_config
        
        logger.info("✅ 환경 설정 테스트 통과")
        return True
        
    except Exception as e:
        logger.error(f"❌ 환경 설정 테스트 실패: {e}")
        return False

def main():
    """메인 테스트 함수"""
    logger.info("🚀 DataCopyEngine 및 설정 테스트 시작")
    
    test_results = []
    
    # 각 테스트 실행
    test_results.append(("워커 설정", test_worker_config()))
    test_results.append(("배치 설정", test_batch_settings()))
    test_results.append(("xmin 설정", test_xmin_settings()))
    test_results.append(("환경 설정", test_environment_config()))
    
    # 결과 요약
    logger.info("\n=== 테스트 결과 요약 ===")
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ 통과" if result else "❌ 실패"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\n총 {total}개 테스트 중 {passed}개 통과 ({passed/total*100:.1f}%)")
    
    if passed == total:
        logger.info("🎉 모든 테스트 통과!")
        return 0
    else:
        logger.error("💥 일부 테스트 실패")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 