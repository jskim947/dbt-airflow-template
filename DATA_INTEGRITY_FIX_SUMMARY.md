# 데이터 무결성 검증 문제 해결 요약

## 🔍 문제 분석

### ❌ **1차 문제: 임시 테이블 접근 실패**
```
ERROR - Failed to get row count for table temp_raw_data_sym_v1_sym_ticker_exchange_1755737439 from source DB: 
오류: "temp_raw_data_sym_v1_sym_ticker_exchange_1755737439" 이름의 릴레이션(relation)이 없습니다
```

**원인**: 
- 임시 테이블이 타겟 DB(`postgres_default`)에 생성됨
- 소스 DB(`fs2_postgres`)에서 접근 시도
- 스키마 불일치로 인한 접근 실패

### ❌ **2차 문제: 데이터 무결성 검증 실패**
```
데이터 무결성 검증 실패: Source: 9569200 rows, Target: 5968802 rows, Missing: 3229433, Extra: 0
```

**원인**: 
- WHERE 조건이 적용된 데이터만 복사 (5,968,802행)
- 무결성 검증은 전체 테이블 비교 (9,569,200행)
- **WHERE 조건이 무결성 검증에 반영되지 않음**

## 📊 문제 상세 분석

### 1. **데이터 복사 과정**
```
소스 전체: 9,569,200행
WHERE 조건 적용: 5,968,802행 (특정 거래소만)
타겟 복사: 5,968,802행 ✅
무결성 검증: 전체 vs 부분 비교 ❌
```

### 2. **WHERE 조건**
```sql
SPLIT_PART(ticker_exchange, '-', 2) IN (
    'AMS','BRU','FRA','HKG','HSTC','JAS','JKT','KRX','LIS','LON',
    'NAS','NYS','PAR','ROCO','SES','SHE','SHG','STC','TAI','TKS','TSE'
)
```

## ✅ 해결 방안

### 1. **임시 테이블 접근 문제 해결**
- **스테이징 테이블 방식 구현 완료** (이전 작업)
- 타겟 DB에 스테이징 테이블 생성
- psql COPY로 직접 복사
- 스키마 불일치 문제 해결

### 2. **데이터 무결성 검증 수정**
- `validate_data_integrity` 메서드에 `where_clause` 파라미터 추가
- WHERE 조건이 있는 경우와 없는 경우를 구분하여 처리
- 조건에 맞는 데이터만 비교하도록 수정

## 🔧 구현된 수정 사항

### 1. **DatabaseOperations 클래스 수정**

#### `validate_data_integrity` 메서드 개선
```python
def validate_data_integrity(
    self, 
    source_table: str, 
    target_table: str, 
    primary_keys: list[str], 
    where_clause: str = None  # ✅ 새로 추가된 파라미터
) -> dict[str, Any]:
```

#### WHERE 조건 처리 로직
```python
if where_clause:
    # WHERE 조건이 적용된 경우: 조건에 맞는 데이터만 비교
    logger.info(f"WHERE 조건이 적용된 무결성 검증: {where_clause}")
    
    # 소스 테이블 행 수 (WHERE 조건 적용)
    source_count = self.get_table_row_count(
        source_table, 
        where_clause=where_clause, 
        use_target_db=False
    )
    
    # 기본키 기반 데이터 일치 여부 확인 (WHERE 조건 적용)
    source_pk_sql = f"SELECT {pk_columns} FROM {source_table} WHERE {where_clause} ORDER BY {pk_columns}"
    
else:
    # WHERE 조건이 없는 경우: 전체 테이블 비교 (기존 로직)
    logger.info("전체 테이블 무결성 검증")
```

### 2. **DAG 파일 수정**

#### 모든 `validate_data_integrity` 호출 수정
```python
# 수정 전
validation_result = db_ops.validate_data_integrity(
    table_config["source"], 
    table_config["target"], 
    table_config["primary_key"]
)

# 수정 후
where_clause = table_config.get("custom_where")
validation_result = db_ops.validate_data_integrity(
    table_config["source"], 
    table_config["target"], 
    table_config["primary_key"],
    where_clause=where_clause  # ✅ WHERE 조건 전달
)
```

#### 수정된 DAG 함수들
1. `copy_data_with_custom_sql()` - 기본 데이터 복사
2. `copy_data_with_xmin_incremental()` - xmin 기반 증분 복사  
3. `validate_xmin_incremental_integrity()` - xmin 무결성 검증

## 📋 수정된 파일 목록

### 1. **airflow/dags/common/database_operations.py**
- `validate_data_integrity` 메서드에 `where_clause` 파라미터 추가
- WHERE 조건 적용 시와 미적용 시 로직 분리
- 상세한 로깅 및 에러 메시지 개선

### 2. **airflow/dags/postgres_data_copy_dag_refactored.py**
- 모든 `validate_data_integrity` 호출에 WHERE 조건 전달
- 3개 함수에서 무결성 검증 로직 수정

## 🎯 예상 결과

### 1. **데이터 무결성 검증 성공**
```
WHERE 조건 적용 - Source: 5968802 rows, Target: 5968802 rows
✅ 무결성 검증 성공
```

### 2. **정확한 데이터 비교**
- WHERE 조건이 적용된 데이터만 비교
- 불필요한 "Missing" 데이터 경고 제거
- 실제 데이터 불일치만 감지

### 3. **로깅 개선**
- WHERE 조건 적용 여부 명확히 표시
- 검증 대상 데이터 범위 명시
- 디버깅 및 모니터링 향상

## 🔄 다음 단계

### 1. **테스트 및 검증**
- [ ] 수정된 코드로 DAG 실행 테스트
- [ ] WHERE 조건 적용 시 무결성 검증 확인
- [ ] 로그 메시지 및 경고 확인

### 2. **추가 개선 사항**
- [ ] 다른 DAG 파일들도 동일하게 수정
- [ ] 무결성 검증 성능 최적화
- [ ] 에러 핸들링 강화

### 3. **문서화**
- [ ] API 문서 업데이트
- [ ] 사용법 가이드 작성
- [ ] 문제 해결 가이드 추가

## 🎉 결론

이번 수정을 통해 **데이터 무결성 검증의 정확성**을 크게 향상시켰습니다. WHERE 조건이 적용된 데이터 복사의 경우, 무결성 검증도 동일한 조건으로 비교하여 불필요한 경고를 제거하고 실제 데이터 불일치만을 감지할 수 있게 되었습니다.

**주요 개선 사항:**
1. ✅ WHERE 조건 적용 시 정확한 무결성 검증
2. ✅ 불필요한 "Missing" 데이터 경고 제거  
3. ✅ 상세한 로깅 및 에러 메시지
4. ✅ 기존 코드와의 호환성 유지

이제 WHERE 조건이 적용된 데이터 복사에서도 정확한 무결성 검증이 가능합니다! 🚀 