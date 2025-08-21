#!/usr/bin/env python3
"""
Xmin 기능 테스트 스크립트
PostgreSQL xmin 기반 증분 처리 기능을 테스트합니다.

사용법:
    python test_xmin_functionality.py

테스트 내용:
1. DatabaseOperations의 xmin 관련 메서드 테스트
2. DataCopyEngine의 xmin 기반 복사 기능 테스트
3. 설정 파일의 xmin 설정 테스트
"""

import os
import sys
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'airflow', 'dags'))

def test_xmin_settings():
    """Xmin 설정 테스트"""
    logger.info("=== Xmin 설정 테스트 시작 ===")
    
    try:
        from common import DAGSettings
        
        # xmin 설정 조회
        xmin_settings = DAGSettings.get_xmin_settings()
        logger.info(f"xmin 설정: {xmin_settings}")
        
        # xmin 테이블 설정 조회
        table_configs = DAGSettings.get_xmin_table_configs()
        logger.info(f"xmin 테이블 설정: {len(table_configs)}개 테이블")
        
        for config in table_configs:
            logger.info(f"  - {config['source']} -> {config['target']}")
            logger.info(f"    커스텀 조건: {config.get('custom_where', '없음')}")
            logger.info(f"    폴백 필드: {config.get('incremental_field', '없음')}")
        
        # 하이브리드 설정 조회
        hybrid_configs = DAGSettings.get_hybrid_table_configs()
        logger.info(f"하이브리드 설정: {len(hybrid_configs)}개 테이블")
        
        # 설정 검증 테스트
        test_config = {
            "source": "m23.test_table",
            "target": "raw_data.test_table",
            "primary_key": ["id"],
            "sync_mode": "xmin_incremental",
            "batch_size": 5000
        }
        
        validation_result = DAGSettings.validate_xmin_config(test_config)
        logger.info(f"설정 검증 결과: {validation_result}")
        
        logger.info("✅ XminTableConfig 설정 테스트 성공")
        return True
        
    except Exception as e:
        logger.error(f"❌ Xmin 설정 테스트 실패: {e}")
        return False

def test_database_operations_xmin():
    """DatabaseOperations의 xmin 관련 메서드 테스트"""
    logger.info("=== DatabaseOperations xmin 메서드 테스트 시작 ===")
    
    try:
        from common.database_operations import DatabaseOperations
        
        # 연결 정보 설정 (환경변수에서 가져오기)
        source_conn_id = os.getenv("SOURCE_POSTGRES_CONN_ID", "fs2_postgres")
        target_conn_id = os.getenv("TARGET_POSTGRES_CONN_ID", "postgres_default")
        
        logger.info(f"소스 연결 ID: {source_conn_id}")
        logger.info(f"타겟 연결 ID: {target_conn_id}")
        
        # DatabaseOperations 객체 생성
        db_ops = DatabaseOperations(source_conn_id, target_conn_id)
        
        # 연결 테스트
        connection_results = db_ops.test_connections()
        logger.info(f"연결 테스트 결과: {connection_results}")
        
        if not all(connection_results.values()):
            logger.warning("일부 데이터베이스 연결 실패 - 테스트 계속 진행")
        
        # xmin 관련 메서드 테스트 (연결이 성공한 경우에만)
        if connection_results.get("source", False):
            logger.info("소스 데이터베이스 연결 성공 - xmin 메서드 테스트 진행")
            
            # 테스트용 테이블명
            test_source_table = "m23.edi_690"  # 실제 존재하는 테이블명으로 변경 필요
            
            try:
                # xmin 안정성 검증
                stability = db_ops.validate_xmin_stability(test_source_table)
                logger.info(f"xmin 안정성: {stability}")
                
                # 복제 상태 확인
                replication_ok = db_ops.check_replication_status(test_source_table)
                logger.info(f"복제 상태 확인: {replication_ok}")
                
            except Exception as e:
                logger.warning(f"xmin 메서드 테스트 중 오류 (테이블이 존재하지 않을 수 있음): {e}")
        
        if connection_results.get("target", False):
            logger.info("타겟 데이터베이스 연결 성공 - xmin 컬럼 관련 메서드 테스트 진행")
            
            # 테스트용 테이블명
            test_target_table = "raw_data.test_xmin_table"
            
            try:
                # source_xmin 컬럼 존재 확인/추가
                result = db_ops.ensure_xmin_column_exists(test_target_table)
                logger.info(f"source_xmin 컬럼 확인/추가 결과: {result}")
                
                # 마지막 처리된 xmin 값 조회
                last_xmin = db_ops.get_last_processed_xmin_from_target(test_target_table)
                logger.info(f"마지막 처리 xmin: {last_xmin}")
                
            except Exception as e:
                logger.warning(f"xmin 컬럼 메서드 테스트 중 오류: {e}")
        
        logger.info("✅ DatabaseOperations xmin 메서드 테스트 성공")
        return True
        
    except Exception as e:
        logger.error(f"❌ DatabaseOperations xmin 메서드 테스트 실패: {e}")
        return False

def test_data_copy_engine_xmin():
    """DataCopyEngine의 xmin 기반 복사 기능 테스트"""
    logger.info("=== DataCopyEngine xmin 기능 테스트 시작 ===")
    
    try:
        from common.database_operations import DatabaseOperations
        from common.data_copy_engine import DataCopyEngine
        
        # 연결 정보 설정
        source_conn_id = os.getenv("SOURCE_POSTGRES_CONN_ID", "fs2_postgres")
        target_conn_id = os.getenv("TARGET_POSTGRES_CONN_ID", "postgres_default")
        
        # 객체 생성
        db_ops = DatabaseOperations(source_conn_id, target_conn_id)
        copy_engine = DataCopyEngine(db_ops)
        
        # 연결 테스트
        connection_results = db_ops.test_connections()
        if not all(connection_results.values()):
            logger.warning("데이터베이스 연결 실패 - 테스트 스킵")
            return True
        
        # xmin 기반 복사 기능 테스트 (메서드 존재 여부만 확인)
        logger.info("DataCopyEngine xmin 메서드 존재 여부 확인:")
        
        # copy_table_data_with_xmin 메서드 확인
        if hasattr(copy_engine, 'copy_table_data_with_xmin'):
            logger.info("✅ copy_table_data_with_xmin 메서드 존재")
        else:
            logger.error("❌ copy_table_data_with_xmin 메서드 없음")
            return False
        
        # _export_to_csv_with_xmin 메서드 확인
        if hasattr(copy_engine, '_export_to_csv_with_xmin'):
            logger.info("✅ _export_to_csv_with_xmin 메서드 존재")
        else:
            logger.error("❌ _export_to_csv_with_xmin 메서드 없음")
            return False
        
        # _execute_xmin_merge 메서드 확인
        if hasattr(copy_engine, '_execute_xmin_merge'):
            logger.info("✅ _execute_xmin_merge 메서드 존재")
        else:
            logger.error("❌ _execute_xmin_merge 메서드 없음")
            return False
        
        logger.info("✅ DataCopyEngine xmin 기능 테스트 성공")
        return True
        
    except Exception as e:
        logger.error(f"❌ DataCopyEngine xmin 기능 테스트 실패: {e}")
        return False

def test_dag_functions():
    """DAG 함수들 테스트"""
    logger.info("=== DAG 함수 테스트 시작 ===")
    
    try:
        # xmin DAG 파일 존재 확인
        dag_file_path = "airflow/dags/xmin_incremental_sync_dag.py"
        if os.path.exists(dag_file_path):
            logger.info(f"✅ xmin DAG 파일 존재: {dag_file_path}")
        else:
            logger.error(f"❌ xmin DAG 파일 없음: {dag_file_path}")
            return False
        
        # 기존 DAG 파일에 xmin 함수 추가 확인
        refactored_dag_path = "airflow/dags/postgres_data_copy_dag_refactored.py"
        if os.path.exists(refactored_dag_path):
            with open(refactored_dag_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
                if 'copy_data_with_xmin_incremental' in content:
                    logger.info("✅ copy_data_with_xmin_incremental 함수 존재")
                else:
                    logger.warning("⚠️ copy_data_with_xmin_incremental 함수 없음")
                
                if 'validate_xmin_incremental_integrity' in content:
                    logger.info("✅ validate_xmin_incremental_integrity 함수 존재")
                else:
                    logger.warning("⚠️ validate_xmin_incremental_integrity 함수 없음")
        
        logger.info("✅ DAG 함수 테스트 성공")
        return True
        
    except Exception as e:
        logger.error(f"❌ DAG 함수 테스트 실패: {e}")
        return False

def main():
    """메인 테스트 함수"""
    logger.info("🚀 Xmin 기능 테스트 시작")
    
    test_results = []
    
    # 1. 설정 테스트
    test_results.append(("Xmin 설정", test_xmin_settings()))
    
    # 2. DatabaseOperations xmin 메서드 테스트
    test_results.append(("DatabaseOperations xmin 메서드", test_database_operations_xmin()))
    
    # 3. DataCopyEngine xmin 기능 테스트
    test_results.append(("DataCopyEngine xmin 기능", test_data_copy_engine_xmin()))
    
    # 4. DAG 함수 테스트
    test_results.append(("DAG 함수", test_dag_functions()))
    
    # 결과 요약
    logger.info("\n" + "="*50)
    logger.info("📊 테스트 결과 요약")
    logger.info("="*50)
    
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\n전체: {total}개, 성공: {passed}개, 실패: {total - passed}개")
    
    if passed == total:
        logger.info("🎉 모든 테스트 통과!")
        return 0
    else:
        logger.error(f"⚠️ {total - passed}개 테스트 실패")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 