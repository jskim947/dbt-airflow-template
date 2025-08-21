#!/usr/bin/env python3
"""
워커 수 테스트 스크립트
실행: python worker_test.py
"""

import multiprocessing as mp
import psutil
import time
import os

def main():
    print("=== 시스템 정보 ===")
    
    # CPU 정보
    cpu_physical = mp.cpu_count() // 2  # 물리적 코어 수 추정
    cpu_logical = mp.cpu_count()
    
    print(f"물리적 CPU 코어: {cpu_physical}")
    print(f"논리적 CPU 코어: {cpu_logical}")
    
    # 메모리 정보
    memory = psutil.virtual_memory()
    memory_gb = memory.total / (1024**3)
    memory_available_gb = memory.available / (1024**3)
    
    print(f"총 메모리: {memory_gb:.1f} GB")
    print(f"가용 메모리: {memory_available_gb:.1f} GB")
    
    # 환경변수 확인
    airflow_env = os.getenv("AIRFLOW_ENV", "development")
    print(f"Airflow 환경: {airflow_env}")
    
    # 권장 워커 수 계산
    if memory_available_gb < 2:
        recommended = max(2, cpu_physical // 2)
        reason = "메모리 부족으로 인한 제한"
    elif memory_available_gb < 4:
        recommended = max(2, cpu_physical - 1)
        reason = "메모리 제한으로 인한 제한"
    else:
        recommended = cpu_physical
        reason = "CPU 코어 수 기반"
    
    print(f"\n=== 권장 워커 수 ===")
    print(f"권장 워커 수: {recommended}")
    print(f"이유: {reason}")
    
    # 환경별 워커 수 권장사항
    env_recommendations = {
        "development": min(recommended, 4),
        "staging": min(recommended, 6),
        "production": recommended
    }
    
    print(f"\n=== 환경별 권장 워커 수 ===")
    for env, workers in env_recommendations.items():
        print(f"{env}: {workers} 워커")
    
    # 배치 크기 권장사항 (기존 설정 유지)
    print(f"\n=== 배치 크기 권장사항 ===")
    print("현재 설정된 배치 크기 유지 권장:")
    print("- 개발환경: 1000-5000")
    print("- 스테이징: 5000-10000") 
    print("- 프로덕션: 10000-20000")
    print("- 테이블별로 개별 설정 가능")

if __name__ == "__main__":
    main() 