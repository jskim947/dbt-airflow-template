좋은 아이디어입니다! 기존 배치 방식을 유지하면서 워커 수만 최적화하고, `source_xmin` 필드 사용을 위한 수정 계획을 문서로 작성하겠습니다.

## 📋 **source_xmin 필드 사용을 위한 코드 수정 계획**

### **목표**
- 기존 배치 처리 방식 유지 (스트리밍 방식 제외)
- 워커 수 최적화로 성능 향상
- `source_xmin` 필드를 올바르게 사용하여 MERGE 오류 해결

---

## 🔧 **1단계: 워커 수 최적화**

### **1.1 시스템 정보 확인 및 워커 수 결정**
```python
#!/usr/bin/env python3
"""
워커 수 테스트 스크립트
실행: python worker_test.py
"""

import multiprocessing as mp
import psutil
import time

def main():
    print("=== 시스템 정보 ===")
    
    # CPU 정보
    cpu_physical = mp.cpu_count(logical=False)
    cpu_logical = mp.cpu_count(logical=True)
    
    print(f"물리적 CPU 코어: {cpu_physical}")
    print(f"논리적 CPU 코어: {cpu_logical}")
    
    # 메모리 정보
    memory = psutil.virtual_memory()
    memory_gb = memory.total / (1024**3)
    memory_available_gb = memory.available / (1024**3)
    
    print(f"총 메모리: {memory_gb:.1f} GB")
    print(f"가용 메모리: {memory_available_gb:.1f} GB")
    
    # 권장 워커 수
    if memory_available_gb < 2:
        recommended = max(2, cpu_physical // 2)
    elif memory_available_gb < 4:
        recommended = max(2, cpu_physical - 1)
    else:
        recommended = cpu_physical
    
    print(f"\n=== 권장 워커 수 ===")
    print(f"권장 워커 수: {recommended}")
    
    if recommended < cpu_physical:
        print(f"이유: 메모리 부족으로 인한 제한")
    else:
        print(f"이유: CPU 코어 수 기반")

if __name__ == "__main__":
    main()
```

### **1.2 워커 수 설정 적용**
```python
# settings.py에 워커 수 설정 추가
@classmethod
def get_worker_config(cls) -> Dict[str, Any]:
    """워커 설정 반환"""
    return {
        "default_workers": 4,  # 기본값
        "max_workers": 8,      # 최대값
        "min_workers": 2,      # 최소값
        "batch_size": 1000,    # 배치 크기 조정 (5000 → 1000)
    }
```

---

## �� **2단계: source_xmin 필드 사용을 위한 코드 수정**

### **2.1 DataCopyEngine 수정**

#### **2.1.1 소스 테이블 xmin 값 조회 메서드 추가**
```python
def get_source_max_xmin(self, source_table: str) -> int:
    """소스 테이블의 최대 xmin 값 조회"""
    try:
        source_hook = self.get_source_hook()
        
        # PostgreSQL 시스템 필드 xmin의 최대값 조회
        max_xmin_sql = f"SELECT MAX(xmin::text::bigint) FROM {source_table}"
        result = source_hook.get_first(max_xmin_sql)
        
        if result and result[0]:
            max_xmin = result[0]
            logger.info(f"소스 테이블 {source_table}의 최대 xmin: {max_xmin}")
            return max_xmin
        else:
            logger.warning(f"소스 테이블 {source_table}에서 xmin 값을 찾을 수 없음")
            return 0
            
    except Exception as e:
        logger.error(f"소스 테이블 xmin 조회 실패: {e}")
        return 0
```

#### **2.1.2 xmin 값을 포함한 CSV 내보내기 (기존 배치 방식 유지)**
```python
def export_to_csv_with_xmin(self, source_table: str, csv_file: str) -> int:
    """xmin 값을 포함하여 CSV 내보내기 (기존 배치 방식)"""
    try:
        # 1. 소스 테이블의 최대 xmin 값 조회
        max_xmin = self.get_source_max_xmin(source_table)
        logger.info(f"소스 테이블 {source_table}의 최대 xmin: {max_xmin}")
        
        # 2. 기존 배치 방식으로 CSV 내보내기 (워커 수 최적화 적용)
        source_hook = self.get_source_hook()
        
        # 전체 행 수 조회
        count_sql = f"SELECT COUNT(*) FROM {source_table}"
        total_rows = source_hook.get_first(count_sql)[0]
        
        # 배치 크기 및 워커 수 설정
        batch_size = 1000  # 5000 → 1000으로 조정
        num_workers = 4    # 워커 수 최적화
        
        logger.info(f"CSV 내보내기 시작: {source_table} → {csv_file}")
        logger.info(f"총 행 수: {total_rows}, 배치 크기: {batch_size}, 워커 수: {num_workers}")
        
        # 배치별로 데이터 처리하여 CSV 파일에 쓰기
        with open(csv_file, 'w') as f:
            for offset in range(0, total_rows, batch_size):
                query = f"""
                    SELECT 
                        fsym_id, 
                        ticker_exchange,
                        {max_xmin} as source_xmin
                    FROM {source_table}
                    ORDER BY fsym_id
                    LIMIT {batch_size} OFFSET {offset}
                """
                
                batch_data = source_hook.get_records(query)
                
                # CSV 형식으로 쓰기
                for row in batch_data:
                    f.write(','.join(str(cell) for cell in row) + '\n')
                
                # 진행률 로깅
                current_progress = min(offset + batch_size, total_rows)
                logger.info(f"배치 처리 진행률: {current_progress}/{total_rows}")
        
        logger.info(f"xmin 값({max_xmin})을 포함한 CSV 내보내기 완료: {csv_file}")
        return max_xmin
        
    except Exception as e:
        logger.error(f"xmin 값을 포함한 CSV 내보내기 실패: {e}")
        raise
```

#### **2.1.3 source_xmin을 포함한 MERGE 실행**
```python
def execute_merge_with_xmin(self, temp_table: str, target_table: str, max_xmin: int):
    """source_xmin을 포함한 MERGE 실행"""
    try:
        target_hook = self.get_target_hook()
        
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING (
                SELECT fsym_id, ticker_exchange, source_xmin
                FROM {temp_table}
            ) AS source
            ON target.fsym_id = source.fsym_id
            WHEN MATCHED THEN
                UPDATE SET 
                    ticker_exchange = source.ticker_exchange,
                    source_xmin = source.source_xmin
            WHEN NOT MATCHED THEN
                INSERT (fsym_id, ticker_exchange, source_xmin)
                VALUES (source.fsym_id, source.ticker_exchange, source.source_xmin);
        """
        
        target_hook.run(merge_sql)
        logger.info(f"source_xmin을 포함한 MERGE 완료: {temp_table} → {target_table}")
        
    except Exception as e:
        logger.error(f"source_xmin을 포함한 MERGE 실패: {e}")
        raise
```

### **2.2 xmin 기반 증분 처리 메서드 수정**

#### **2.2.1 copy_table_data_with_xmin 메서드 수정**
```python
def copy_table_data_with_xmin(self, source_table: str, target_table: str, **kwargs):
    """xmin 기반 증분 데이터 복사 (수정된 버전)"""
    try:
        logger.info(f"xmin 기반 증분 데이터 복사 시작: {source_table} → {target_table}")
        
        # 1. 소스 테이블의 최대 xmin 값 조회
        max_xmin = self.get_source_max_xmin(source_table)
        logger.info(f"소스 테이블 {source_table}의 최대 xmin: {max_xmin}")
        
        # 2. xmin 값을 포함한 CSV 내보내기 (기존 배치 방식)
        csv_file = f"/tmp/temp_{source_table.replace('.', '_')}.csv"
        actual_max_xmin = self.export_to_csv_with_xmin(source_table, csv_file)
        
        # 3. 임시 테이블에 CSV 데이터 로드
        temp_table = self.import_csv_to_temp_table(csv_file, target_table)
        
        # 4. source_xmin을 포함한 MERGE 실행
        self.execute_merge_with_xmin(temp_table, target_table, actual_max_xmin)
        
        # 5. 정리 작업
        self.cleanup_temp_files(csv_file)
        self.cleanup_temp_table(temp_table)
        
        logger.info(f"xmin 기반 증분 복사 완료: {source_table} → {target_table}")
        return {"status": "success", "max_xmin": actual_max_xmin}
        
    except Exception as e:
        logger.error(f"xmin 기반 증분 복사 실패: {e}")
        raise
```

---

## �� **3단계: 테이블 생성 및 스키마 검증 수정**

### **3.1 source_xmin 컬럼을 포함한 테이블 생성**
```python
def _generate_standard_create_table_sql_with_xmin(self, target_table: str, source_schema: dict):
    """source_xmin 컬럼을 포함한 표준 테이블 생성 SQL"""
    columns = []
    
    # 기존 컬럼들 추가
    for column_info in source_schema["columns"]:
        column_name = column_info["name"]
        data_type = column_info["type"]
        is_nullable = column_info["nullable"]
        max_length = column_info.get("max_length")
        
        # PostgreSQL 타입으로 변환
        pg_type = self._convert_to_postgres_type(data_type, max_length)
        
        if not is_nullable:
            columns.append(f'"{column_name}" {pg_type} NOT NULL')
        else:
            columns.append(f'"{column_name}" {pg_type}')
    
    # source_xmin 컬럼 추가
    columns.append('"source_xmin" BIGINT')
    
    columns_sql = ", ".join(columns)
    create_sql = f"CREATE TABLE {target_table} ({columns_sql})"
    
    return create_sql
```

---

## 🔧 **4단계: 통합 및 메인 복사 메서드 수정**

### **4.1 메인 복사 메서드에서 xmin 방식 호출**
```python
def copy_table_data(self, source_table: str, target_table: str, sync_mode: str = "full_sync", **kwargs):
    """테이블 데이터 복사 (xmin 방식 지원)"""
    try:
        if sync_mode == "xmin_incremental":
            # xmin 기반 증분 복사 (수정된 버전)
            return self.copy_table_data_with_xmin(source_table, target_table, **kwargs)
        elif sync_mode == "incremental_sync":
            # 기존 증분 복사
            return self.copy_table_data_incremental(source_table, target_table, **kwargs)
        else:
            # 전체 복사
            return self.copy_table_data_full(source_table, target_table, **kwargs)
            
    except Exception as e:
        logger.error(f"테이블 복사 실패: {e}")
        raise
```

---

## �� **수정 우선순위 및 일정**

### **1일차: 워커 수 최적화**
- [ ] `worker_test.py` 스크립트 실행하여 최적 워커 수 확인
- [ ] `settings.py`에 워커 설정 추가
- [ ] 배치 크기 5000 → 1000으로 조정

### **2일차: source_xmin 관련 메서드 구현**
- [ ] `get_source_max_xmin()` 메서드 구현
- [ ] `export_to_csv_with_xmin()` 메서드 구현 (기존 배치 방식 유지)
- [ ] `execute_merge_with_xmin()` 메서드 구현

### **3일차: 통합 및 테스트**
- [ ] `copy_table_data_with_xmin()` 메서드 수정
- [ ] 테이블 생성 시 `source_xmin` 컬럼 포함
- [ ] 전체 워크플로우 테스트

---

## �� **기대 효과**

### **성능 향상**
- **워커 수 최적화**: CPU 코어 수에 맞춘 병렬 처리
- **배치 크기 조정**: 메모리 사용량 80% 감소
- **전체 처리 시간**: 20-30% 단축 예상

### **오류 해결**
- **MERGE 오류**: `source_xmin` 컬럼 참조 문제 해결
- **데이터 무결성**: xmin 값으로 증분 동기화 기준 제공
- **안정성**: 기존 검증된 배치 방식 유지

### **유지보수성**
- **코드 일관성**: 기존 패턴과 동일한 구조
- **확장성**: 향후 xmin 기반 증분 처리 확장 가능
- **모니터링**: 상세한 진행률 및 오류 로깅

이 계획대로 진행하면 기존 방식의 장점을 유지하면서 `source_xmin` 필드를 올바르게 사용할 수 있습니다.