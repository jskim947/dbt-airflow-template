#!/usr/bin/env python3
"""
간단한 설정 테스트 스크립트
Airflow 의존성 없이 기본 설정만 테스트
"""

import os
import sys

def test_environment_variables():
    """환경변수 테스트"""
    print("=== 환경변수 테스트 ===")
    
    # 기본 환경변수 확인
    env_vars = [
        "AIRFLOW_ENV",
        "SOURCE_POSTGRES_CONN_ID", 
        "TARGET_POSTGRES_CONN_ID",
        "POSTGRES_HOST",
        "POSTGRES_PORT"
    ]
    
    for var in env_vars:
        value = os.getenv(var, "설정되지 않음")
        print(f"{var}: {value}")
    
    # 현재 환경
    current_env = os.getenv("AIRFLOW_ENV", "development")
    print(f"\n현재 환경: {current_env}")
    
    return True

def test_worker_recommendations():
    """워커 수 권장사항 테스트"""
    print("\n=== 워커 수 권장사항 ===")
    
    # 환경별 워커 수 권장사항
    env_worker_configs = {
        "development": {
            "default_workers": 2,
            "max_workers": 4,
            "min_workers": 1,
        },
        "staging": {
            "default_workers": 4,
            "max_workers": 6,
            "min_workers": 2,
        },
        "production": {
            "default_workers": 6,
            "max_workers": 8,
            "min_workers": 4,
        }
    }
    
    current_env = os.getenv("AIRFLOW_ENV", "development").lower()
    config = env_worker_configs.get(current_env, env_worker_configs["development"])
    
    print(f"환경: {current_env}")
    print(f"기본 워커 수: {config['default_workers']}")
    print(f"최대 워커 수: {config['max_workers']}")
    print(f"최소 워커 수: {config['min_workers']}")
    
    return True

def test_batch_size_recommendations():
    """배치 크기 권장사항 테스트"""
    print("\n=== 배치 크기 권장사항 ===")
    
    # 환경별 배치 크기 권장사항
    env_batch_configs = {
        "development": {
            "batch_size": 1000,
            "range": "1000-5000"
        },
        "staging": {
            "batch_size": 5000,
            "range": "5000-10000"
        },
        "production": {
            "batch_size": 10000,
            "range": "10000-20000"
        }
    }
    
    current_env = os.getenv("AIRFLOW_ENV", "development").lower()
    config = env_batch_configs.get(current_env, env_batch_configs["development"])
    
    print(f"환경: {current_env}")
    print(f"기본 배치 크기: {config['batch_size']}")
    print(f"권장 범위: {config['range']}")
    
    # 테이블별 배치 크기
    table_batch_sizes = {
        "인포맥스종목마스터": 10000,
        "ff_v3_ff_sec_entity": 20000,
        "edi_690": 10000,
    }
    
    print(f"\n테이블별 배치 크기:")
    for table, batch_size in table_batch_sizes.items():
        print(f"  {table}: {batch_size}")
    
    return True

def main():
    """메인 함수"""
    print("🚀 간단한 설정 테스트 시작\n")
    
    try:
        test_environment_variables()
        test_worker_recommendations()
        test_batch_size_recommendations()
        
        print("\n✅ 모든 테스트 통과!")
        return 0
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 