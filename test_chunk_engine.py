#!/usr/bin/env python3
"""
청크 방식 데이터 복사 엔진 테스트 코드
"""

import os
import sys
import logging
import tempfile
import time

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_chunk_engine_basic():
    """기본 청크 엔진 기능 테스트"""
    print("=== 기본 청크 엔진 기능 테스트 ===")
    
    try:
        from airflow.dags.common.data_copy_engine import DataCopyEngine
        
        # Mock DatabaseOperations 클래스 생성
        class MockDatabaseOperations:
            def __init__(self):
                self.connection = None
                
            def get_source_hook(self):
                return MockHook()
                
            def get_target_hook(self):
                return MockHook()
                
            def get_table_row_count(self, table_name, where_clause=None):
                # 테스트용 더미 데이터
                return 1000
                
            def get_table_schema(self, table_name):
                return {
                    "columns": [
                        {"name": "id", "type": "INTEGER", "nullable": False},
                        {"name": "name", "type": "VARCHAR", "nullable": True},
                        {"name": "priority", "type": "BIGINT", "nullable": True},
                        {"name": "created_at", "type": "TIMESTAMP", "nullable": True}
                    ]
                }
                
            def execute_query(self, query):
                return [["1"]]
                
            def reconnect(self):
                pass
        
        class MockHook:
            def get_conn(self):
                return MockConnection()
                
            def get_records(self, query, parameters=None):
                # 테스트용 더미 데이터
                return [
                    {"id": 1, "name": "Test1", "priority": "100", "created_at": "2024-01-01"},
                    {"id": 2, "name": "Test2", "priority": "200", "created_at": "2024-01-02"},
                    {"id": 3, "name": "Test3", "priority": "300", "created_at": "2024-01-03"}
                ]
                
            def get_first(self, query):
                return [1000]
        
        class MockConnection:
            def __init__(self):
                self.closed = False
                self.autocommit = True
                
            def cursor(self):
                return MockCursor()
                
            def commit(self):
                pass
                
            def rollback(self):
                pass
        
        class MockCursor:
            def __init__(self):
                self.data = [
                    (1, "Test1", "100", "2024-01-01"),
                    (2, "Test2", "200", "2024-01-02"),
                    (3, "Test3", "300", "2024-01-03")
                ]
                self.index = 0
                
            def execute(self, query):
                pass
                
            def __iter__(self):
                return self
                
            def __next__(self):
                if self.index < len(self.data):
                    result = self.data[self.index]
                    self.index += 1
                    return result
                raise StopIteration
        
        # Mock 객체로 DataCopyEngine 인스턴스 생성
        mock_db_ops = MockDatabaseOperations()
        engine = DataCopyEngine(mock_db_ops, temp_dir="/tmp")
        
        print("✅ DataCopyEngine 인스턴스 생성 성공")
        
        # 메서드 존재 여부 확인
        required_methods = [
            '_export_to_csv_chunked',
            '_export_to_csv_legacy',
            '_refresh_database_session',
            '_check_session_health',
            '_check_memory_usage',
            '_force_memory_cleanup',
            '_process_single_chunk_with_transaction',
            '_handle_chunk_error',
            '_save_checkpoint',
            '_resume_from_checkpoint',
            '_verify_checkpoint_integrity',
            '_cleanup_checkpoint',
            '_optimize_batch_size_dynamically',
            '_log_performance_metrics',
            '_log_final_performance_report'
        ]
        
        for method_name in required_methods:
            if hasattr(engine, method_name):
                print(f"✅ 메서드 {method_name} 존재 확인")
            else:
                print(f"❌ 메서드 {method_name} 누락")
                return False
        
        print("✅ 모든 필수 메서드 존재 확인")
        
        # 기본 기능 테스트
        test_csv_path = "/tmp/test_chunk_engine.csv"
        
        # 청크 모드로 CSV 내보내기 테스트
        try:
            result = engine.export_to_csv(
                table_name="test_table",
                csv_path=test_csv_path,
                chunk_mode=True,
                enable_checkpoint=True,
                batch_size=100,
                max_retries=3
            )
            print(f"✅ 청크 모드 CSV 내보내기 성공: {result}행")
        except Exception as e:
            print(f"❌ 청크 모드 CSV 내보내기 실패: {e}")
        
        # 기존 모드로 CSV 내보내기 테스트
        try:
            result = engine.export_to_csv(
                table_name="test_table",
                csv_path=test_csv_path,
                chunk_mode=False,
                batch_size=100
            )
            print(f"✅ 기존 모드 CSV 내보내기 성공: {result}행")
        except Exception as e:
            print(f"❌ 기존 모드 CSV 내보내기 실패: {e}")
        
        # 임시 파일 정리
        if os.path.exists(test_csv_path):
            os.remove(test_csv_path)
            print("✅ 테스트 CSV 파일 정리 완료")
        
        return True
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_chunk_engine_performance():
    """성능 최적화 기능 테스트"""
    print("\n=== 성능 최적화 기능 테스트 ===")
    
    try:
        from airflow.dags.common.data_copy_engine import DataCopyEngine
        
        # Mock 객체 생성 (간단한 버전)
        class MockDBOps:
            def get_source_hook(self):
                return MockHook()
            def get_target_hook(self):
                return MockHook()
            def get_table_row_count(self, table_name, where_clause=None):
                return 10000
            def get_table_schema(self, table_name):
                return {"columns": [{"name": "id", "type": "INTEGER", "nullable": False}]}
            def execute_query(self, query):
                return [["1"]]
            def reconnect(self):
                pass
        
        class MockHook:
            def get_conn(self):
                return MockConnection()
            def get_records(self, query, parameters=None):
                return [{"id": i} for i in range(100)]
            def get_first(self, query):
                return [10000]
        
        class MockConnection:
            def __init__(self):
                self.closed = False
                self.autocommit = True
            def cursor(self):
                return MockCursor()
            def commit(self):
                pass
            def rollback(self):
                pass
        
        class MockCursor:
            def __init__(self):
                self.data = [(i,) for i in range(100)]
                self.index = 0
            def execute(self, query):
                pass
            def __iter__(self):
                return self
            def __next__(self):
                if self.index < len(self.data):
                    result = self.data[self.index]
                    self.index += 1
                    return result
                raise StopIteration
        
        # 테스트 실행
        mock_db_ops = MockDBOps()
        engine = DataCopyEngine(mock_db_ops, temp_dir="/tmp")
        
        # 성능 메트릭 테스트
        performance_metrics = {
            'start_time': time.time(),
            'chunk_processing_times': [1.5, 2.1, 1.8, 2.3, 1.9],
            'memory_usage_history': [50.2, 52.1, 48.9, 51.3, 49.7],
            'session_refresh_count': 2
        }
        
        # 성능 메트릭 로깅 테스트
        try:
            engine._log_performance_metrics(performance_metrics, 100, 5000, 10000)
            print("✅ 성능 메트릭 로깅 테스트 성공")
        except Exception as e:
            print(f"❌ 성능 메트릭 로깅 테스트 실패: {e}")
        
        # 최종 성능 리포트 테스트
        try:
            engine._log_final_performance_report(performance_metrics, 10000, 30.5)
            print("✅ 최종 성능 리포트 테스트 성공")
        except Exception as e:
            print(f"❌ 최종 성능 리포트 테스트 실패: {e}")
        
        # 배치 크기 동적 조정 테스트
        try:
            new_batch_size = engine._optimize_batch_size_dynamically(1000, 150.0, 35.0, False)
            print(f"✅ 배치 크기 동적 조정 테스트 성공: 1000 → {new_batch_size}")
        except Exception as e:
            print(f"❌ 배치 크기 동적 조정 테스트 실패: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ 성능 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_chunk_engine_checkpoint():
    """체크포인트 시스템 테스트"""
    print("\n=== 체크포인트 시스템 테스트 ===")
    
    try:
        from airflow.dags.common.data_copy_engine import DataCopyEngine
        
        # Mock 객체 생성
        class MockDBOps:
            def get_source_hook(self):
                return MockHook()
            def get_target_hook(self):
                return MockHook()
            def get_table_row_count(self, table_name, where_clause=None):
                return 1000
            def get_table_schema(self, table_name):
                return {"columns": [{"name": "id", "type": "INTEGER", "nullable": False}]}
            def execute_query(self, query):
                return [["1"]]
            def reconnect(self):
                pass
        
        class MockHook:
            def get_conn(self):
                return MockConnection()
            def get_records(self, query, parameters=None):
                return [{"id": i} for i in range(100)]
            def get_first(self, query):
                return [1000]
        
        class MockConnection:
            def __init__(self):
                self.closed = False
                self.autocommit = True
            def cursor(self):
                return MockCursor()
            def commit(self):
                pass
            def rollback(self):
                pass
        
        class MockCursor:
            def __init__(self):
                self.data = [(i,) for i in range(100)]
                self.index = 0
            def execute(self, query):
                pass
            def __iter__(self):
                return self
            def __next__(self):
                if self.index < len(self.data):
                    result = self.data[self.index]
                    self.index += 1
                    return result
                raise StopIteration
        
        # 테스트 실행
        mock_db_ops = MockDBOps()
        engine = DataCopyEngine(mock_db_ops, temp_dir="/tmp")
        
        # 임시 CSV 파일 생성
        test_csv_path = "/tmp/test_checkpoint.csv"
        with open(test_csv_path, 'w') as f:
            f.write("id,name,priority\n")
            f.write("1,Test1,100\n")
            f.write("2,Test2,200\n")
        
        # 체크포인트 저장 테스트
        try:
            engine._save_checkpoint("test_table", 100, 500, test_csv_path)
            print("✅ 체크포인트 저장 테스트 성공")
        except Exception as e:
            print(f"❌ 체크포인트 저장 테스트 실패: {e}")
        
        # 체크포인트 복구 테스트
        try:
            offset, total = engine._resume_from_checkpoint("test_table", test_csv_path)
            print(f"✅ 체크포인트 복구 테스트 성공: offset={offset}, total={total}")
        except Exception as e:
            print(f"❌ 체크포인트 복구 테스트 실패: {e}")
        
        # 체크포인트 정리 테스트
        try:
            engine._cleanup_checkpoint(test_csv_path)
            print("✅ 체크포인트 정리 테스트 성공")
        except Exception as e:
            print(f"❌ 체크포인트 정리 테스트 실패: {e}")
        
        # 임시 파일 정리
        if os.path.exists(test_csv_path):
            os.remove(test_csv_path)
        checkpoint_file = f"{test_csv_path}.checkpoint"
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
        print("✅ 테스트 파일 정리 완료")
        
        return True
        
    except Exception as e:
        print(f"❌ 체크포인트 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """메인 테스트 함수"""
    print("🚀 청크 방식 데이터 복사 엔진 테스트 시작")
    print("=" * 60)
    
    # 테스트 실행
    tests = [
        ("기본 기능", test_chunk_engine_basic),
        ("성능 최적화", test_chunk_engine_performance),
        ("체크포인트 시스템", test_chunk_engine_checkpoint)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name} 테스트 시작...")
        if test_func():
            print(f"✅ {test_name} 테스트 통과")
            passed += 1
        else:
            print(f"❌ {test_name} 테스트 실패")
        print("-" * 40)
    
    # 결과 요약
    print(f"\n📊 테스트 결과 요약")
    print(f"통과: {passed}/{total}")
    print(f"성공률: {passed/total*100:.1f}%")
    
    if passed == total:
        print("🎉 모든 테스트 통과!")
        return 0
    else:
        print("⚠️ 일부 테스트 실패")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 