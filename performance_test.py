#!/usr/bin/env python3
"""
워커 수별 성능 테스트 스크립트
최적의 워커 수를 찾기 위한 성능 측정
"""

import time
import multiprocessing as mp
import psutil
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def simulate_workload(worker_id: int, duration: int = 5) -> dict:
    """
    워커별 작업 시뮬레이션
    
    Args:
        worker_id: 워커 ID
        duration: 작업 지속 시간 (초)
        
    Returns:
        작업 결과 딕셔너리
    """
    start_time = time.time()
    
    # CPU 집약적 작업 시뮬레이션
    result = 0
    for i in range(1000000):
        result += i * i
    
    # 메모리 사용량 측정
    process = psutil.Process()
    memory_info = process.memory_info()
    
    end_time = time.time()
    actual_duration = end_time - start_time
    
    return {
        "worker_id": worker_id,
        "start_time": start_time,
        "end_time": end_time,
        "duration": actual_duration,
        "memory_mb": memory_info.rss / (1024 * 1024),
        "cpu_percent": process.cpu_percent(),
        "result": result
    }

def test_worker_performance(num_workers: int, num_tasks: int = 10) -> dict:
    """
    특정 워커 수로 성능 테스트
    
    Args:
        num_workers: 테스트할 워커 수
        num_tasks: 실행할 작업 수
        
    Returns:
        성능 테스트 결과
    """
    start_time = time.time()
    
    logger.info(f"워커 {num_workers}개로 {num_tasks}개 작업 테스트 시작")
    
    # 시스템 리소스 모니터링 시작
    initial_cpu = psutil.cpu_percent(interval=1)
    initial_memory = psutil.virtual_memory()
    
    results = []
    total_memory = 0
    max_memory = 0
    
    try:
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # 작업 제출
            future_to_task = {
                executor.submit(simulate_workload, i % num_workers, 3): i 
                for i in range(num_tasks)
            }
            
            # 결과 수집
            for future in as_completed(future_to_task):
                result = future.result()
                results.append(result)
                total_memory += result["memory_mb"]
                max_memory = max(max_memory, result["memory_mb"])
                
                logger.info(f"작업 {result['worker_id']} 완료: {result['duration']:.2f}초, "
                          f"메모리: {result['memory_mb']:.1f}MB")
    
    except Exception as e:
        logger.error(f"워커 {num_workers}개 테스트 실패: {e}")
        return {
            "num_workers": num_workers,
            "status": "failed",
            "error": str(e)
        }
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    # 성능 지표 계산
    avg_task_duration = sum(r["duration"] for r in results) / len(results) if results else 0
    throughput = len(results) / total_duration if total_duration > 0 else 0
    
    # 시스템 리소스 사용량
    final_cpu = psutil.cpu_percent(interval=1)
    final_memory = psutil.virtual_memory()
    
    performance_result = {
        "num_workers": num_workers,
        "status": "success",
        "total_duration": total_duration,
        "avg_task_duration": avg_task_duration,
        "throughput": throughput,
        "total_memory_used": total_memory,
        "max_memory_per_worker": max_memory,
        "avg_memory_per_worker": total_memory / len(results) if results else 0,
        "cpu_usage_change": final_cpu - initial_cpu,
        "memory_usage_change": (final_memory.used - initial_memory.used) / (1024 * 1024),
        "num_tasks_completed": len(results)
    }
    
    logger.info(f"워커 {num_workers}개 테스트 완료: "
               f"총 시간: {total_duration:.2f}초, "
               f"처리량: {throughput:.2f} 작업/초")
    
    return performance_result

def find_optimal_workers() -> dict:
    """
    최적의 워커 수 찾기
    
    Returns:
        최적 워커 수와 성능 데이터
    """
    logger.info("=== 워커 수별 성능 테스트 시작 ===")
    
    # 시스템 정보 확인
    cpu_count = mp.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024**3)
    
    logger.info(f"시스템 정보: CPU {cpu_count}개, 메모리 {memory_gb:.1f}GB")
    
    # 테스트할 워커 수 범위
    if cpu_count <= 4:
        worker_range = range(1, min(cpu_count + 1, 5))
    elif cpu_count <= 8:
        worker_range = range(1, min(cpu_count + 1, 9))
    else:
        worker_range = range(1, min(cpu_count + 1, 13))
    
    logger.info(f"테스트할 워커 수 범위: {list(worker_range)}")
    
    # 각 워커 수별로 성능 테스트
    performance_results = []
    
    for num_workers in worker_range:
        logger.info(f"\n--- {num_workers}개 워커 테스트 ---")
        result = test_worker_performance(num_workers, num_tasks=20)
        performance_results.append(result)
        
        # 잠시 대기 (시스템 안정화)
        time.sleep(2)
    
    # 성능 분석
    successful_results = [r for r in performance_results if r["status"] == "success"]
    
    if not successful_results:
        logger.error("모든 테스트가 실패했습니다.")
        return {"optimal_workers": 1, "reason": "모든 테스트 실패"}
    
    # 처리량 기준으로 최적 워커 수 찾기
    best_throughput = max(successful_results, key=lambda x: x["throughput"])
    optimal_workers = best_throughput["num_workers"]
    
    # 메모리 사용량 고려
    memory_efficient = min(successful_results, key=lambda x: x["avg_memory_per_worker"])
    
    # 최종 권장사항
    recommendation = {
        "optimal_workers": optimal_workers,
        "reason": f"최고 처리량: {best_throughput['throughput']:.2f} 작업/초",
        "memory_efficient_workers": memory_efficient["num_workers"],
        "performance_data": successful_results,
        "system_info": {
            "cpu_count": cpu_count,
            "memory_gb": memory_gb,
            "tested_workers": list(worker_range)
        }
    }
    
    return recommendation

def main():
    """메인 함수"""
    logger.info("🚀 워커 수별 성능 테스트 시작")
    
    try:
        # 최적 워커 수 찾기
        recommendation = find_optimal_workers()
        
        # 결과 출력
        logger.info("\n=== 성능 테스트 결과 ===")
        logger.info(f"권장 워커 수: {recommendation['optimal_workers']}")
        logger.info(f"이유: {recommendation['reason']}")
        logger.info(f"메모리 효율적 워커 수: {recommendation['memory_efficient_workers']}")
        
        # 상세 성능 데이터
        logger.info("\n=== 상세 성능 데이터 ===")
        for result in recommendation["performance_data"]:
            logger.info(f"워커 {result['num_workers']}개: "
                       f"처리량 {result['throughput']:.2f} 작업/초, "
                       f"평균 메모리 {result['avg_memory_per_worker']:.1f}MB")
        
        # 환경변수 설정 가이드
        logger.info("\n=== 권장 환경변수 설정 ===")
        logger.info(f"export DEFAULT_WORKERS={recommendation['optimal_workers']}")
        logger.info(f"export MAX_WORKERS={min(recommendation['optimal_workers'] + 2, recommendation['system_info']['cpu_count'])}")
        logger.info(f"export MIN_WORKERS={max(1, recommendation['optimal_workers'] - 2)}")
        
        return 0
        
    except Exception as e:
        logger.error(f"성능 테스트 실패: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 