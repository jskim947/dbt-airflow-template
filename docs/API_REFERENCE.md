# 📚 **API Reference - xmin 기반 증분 동기화**

## 📋 **개요**

이 문서는 PostgreSQL의 `xmin` 시스템 필드를 활용한 증분 동기화 시스템의 API를 설명합니다. 모든 메서드는 `DataCopyEngine` 클래스에 구현되어 있으며, 워커 설정 기반 최적화와 `source_xmin` 필드 통합을 지원합니다.

---

## 🔧 **DataCopyEngine 클래스**

### **클래스 개요**
```python
class DataCopyEngine:
    """데이터 복사 엔진 클래스
    
    PostgreSQL의 xmin 시스템 필드를 활용한 증분 동기화를 지원합니다.
    워커 설정 기반 배치 크기 최적화와 source_xmin 필드 통합을 제공합니다.
    """
```

### **초기화**
```python
def __init__(self, db_ops: DatabaseOperations, temp_dir: str = "/tmp"):
    """DataCopyEngine 초기화
    
    Args:
        db_ops: DatabaseOperations 인스턴스
        temp_dir: 임시 파일 저장 디렉토리 (기본값: "/tmp")
    
    Attributes:
        worker_config: 워커 설정 딕셔너리
        source_hook: 소스 데이터베이스 연결
        target_hook: 타겟 데이터베이스 연결
    """
```

---

## 📊 **xmin 관련 메서드**

### **1. get_source_max_xmin()**

소스 테이블의 최대 xmin 값을 조회합니다.

```python
def get_source_max_xmin(self, source_table: str) -> int:
    """소스 테이블의 최대 xmin 값 조회
    
    Args:
        source_table: 소스 테이블명 (예: "fds.sym_v1_sym_ticker_exchange")
    
    Returns:
        최대 xmin 값 (실패 시 0)
    
    Raises:
        Exception: 데이터베이스 연결 또는 쿼리 실행 실패 시
    
    Example:
        >>> copy_engine = DataCopyEngine(db_ops)
        >>> max_xmin = copy_engine.get_source_max_xmin("fds.sym_v1_sym_ticker_exchange")
        >>> print(f"최대 xmin: {max_xmin}")
        최대 xmin: 1234567890
    """
```

**동작 방식**:
1. PostgreSQL 시스템 필드 `xmin`의 최대값 조회
2. `xmin::text::bigint` 변환으로 안전한 타입 캐스팅
3. 에러 발생 시 0 반환으로 안전성 확보

**SQL 쿼리**:
```sql
SELECT MAX(xmin::text::bigint) FROM {source_table}
```

---

### **2. export_to_csv_with_xmin()**

xmin 값을 포함하여 CSV 파일로 데이터를 내보냅니다.

```python
def export_to_csv_with_xmin(self, source_table: str, csv_file: str, batch_size: int = None) -> int:
    """xmin 값을 포함하여 CSV 내보내기 (워커 설정 적용)
    
    Args:
        source_table: 소스 테이블명
        csv_file: CSV 파일 경로
        batch_size: 배치 크기 (None이면 워커 설정 기반으로 결정)
    
    Returns:
        내보낸 행 수
    
    Raises:
        Exception: CSV 내보내기 실패 시
    
    Example:
        >>> result = copy_engine.export_to_csv_with_xmin(
        ...     source_table="fds.sym_v1_sym_ticker_exchange",
        ...     csv_file="/tmp/data.csv",
        ...     batch_size=None  # 자동 최적화
        ... )
        >>> print(f"내보낸 행 수: {result}")
        내보낸 행 수: 10000
    """
```

**배치 크기 자동 최적화**:
- **워커 2개 이하**: 5,000행
- **워커 4개 이하**: 10,000행  
- **워커 4개 초과**: 15,000행

**CSV 구조**:
```csv
fsym_id,ticker_exchange,source_xmin
12345,KRX,1234567890
12346,KRX,1234567890
...
```

**진행률 로깅**:
```
INFO - 배치 처리 진행률: 10000/50000 (10000행 처리됨)
INFO - 배치 처리 진행률: 20000/50000 (20000행 처리됨)
...
```

---

### **3. execute_merge_with_xmin()**

source_xmin을 포함한 MERGE 작업을 실행합니다.

```python
def execute_merge_with_xmin(self, temp_table: str, target_table: str, max_xmin: int, primary_keys: list[str]) -> dict[str, Any]:
    """source_xmin을 포함한 MERGE 실행
    
    Args:
        temp_table: 임시 테이블명
        target_table: 타겟 테이블명
        max_xmin: 소스 테이블의 최대 xmin 값
        primary_keys: 기본키 컬럼 목록
    
    Returns:
        MERGE 실행 결과 딕셔너리
        
        {
            "source_count": int,      # 소스 테이블 행 수
            "target_count": int,      # 타겟 테이블 행 수
            "max_xmin": int,          # 최대 xmin 값
            "execution_time": float,  # 실행 시간 (초)
            "status": str,            # 실행 상태 ("success" 또는 "error")
            "message": str            # 결과 메시지
        }
    
    Raises:
        Exception: MERGE 실행 실패 시
    
    Example:
        >>> result = copy_engine.execute_merge_with_xmin(
        ...     temp_table="temp_sym_v1_sym_ticker_exchange",
        ...     target_table="raw_data.sym_v1_sym_ticker_exchange",
        ...     max_xmin=1234567890,
        ...     primary_keys=["fsym_id"]
        ... )
        >>> print(f"MERGE 결과: {result['message']}")
        MERGE 완료: temp_sym_v1_sym_ticker_exchange -> raw_data.sym_v1_sym_ticker_exchange, 
        소스: 10000행, 타겟: 50000행, 최대 xmin: 1234567890
    """
```

**MERGE SQL 구조**:
```sql
MERGE INTO {target_table} AS target
USING {temp_table} AS source
ON {primary_key_conditions}

WHEN MATCHED THEN
    UPDATE SET
        {update_columns},
        updated_at = CURRENT_TIMESTAMP

WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_columns})
    VALUES ({source_values});
```

**자동 처리**:
1. `source_xmin` 컬럼 존재 확인 및 자동 생성
2. 인덱스 자동 생성 (`idx_{table_name}_source_xmin`)
3. 임시 테이블 자동 정리

---

### **4. copy_table_data_with_xmin()**

xmin 기반 증분 데이터 복사의 전체 워크플로우를 실행합니다.

```python
def copy_table_data_with_xmin(
    self,
    source_table: str,
    target_table: str,
    primary_keys: list[str],
    sync_mode: str = "xmin_incremental",
    batch_size: int = 5000,
    custom_where: str | None = None,
) -> dict[str, Any]:
    """xmin 기반 증분 데이터 복사 (실제 xmin 값 저장)
    
    Args:
        source_table: 소스 테이블명
        target_table: 타겟 테이블명
        primary_keys: 기본키 컬럼 목록
        sync_mode: 동기화 모드 (기본값: "xmin_incremental")
        batch_size: 배치 크기 (기본값: 5000)
        custom_where: 커스텀 WHERE 조건 (선택사항)
    
    Returns:
        복사 결과 딕셔너리
        
        {
            "status": str,                    # 실행 상태
            "source_table": str,              # 소스 테이블명
            "target_table": str,              # 타겟 테이블명
            "sync_mode": str,                 # 동기화 모드
            "xmin_stability": str,            # xmin 안정성 상태
            "last_processed_xmin": int,       # 마지막 처리된 xmin
            "latest_source_xmin": int,        # 최신 소스 xmin
            "merge_result": dict,             # MERGE 결과
            "batch_size": int,                # 사용된 배치 크기
            "execution_time": float           # 실행 시간
        }
    
    Raises:
        Exception: 복사 과정에서 오류 발생 시
    
    Example:
        >>> result = copy_engine.copy_table_data_with_xmin(
        ...     source_table="fds.sym_v1_sym_ticker_exchange",
        ...     target_table="raw_data.sym_v1_sym_ticker_exchange",
        ...     primary_keys=["fsym_id"],
        ...     batch_size=10000
        ... )
        >>> print(f"동기화 상태: {result['status']}")
        동기화 상태: success
    """
```

**워크플로우 단계**:
1. **xmin 안정성 검증**: 순환 위험 체크
2. **복제 상태 확인**: 복제본 환경 체크
3. **타겟 테이블 준비**: `source_xmin` 컬럼 확인/생성
4. **증분 조건 생성**: xmin 기반 WHERE 절 구성
5. **데이터 추출**: CSV 파일로 내보내기 (xmin 포함)
6. **임시 테이블 로드**: CSV 데이터를 임시 테이블에 로드
7. **MERGE 실행**: `source_xmin` 포함하여 데이터 동기화
8. **정리 작업**: 임시 파일 및 테이블 정리

---

### **5. validate_xmin_incremental_integrity()**

xmin 기반 증분 동기화의 무결성을 검증합니다.

```python
def validate_xmin_incremental_integrity(self, source_table: str, target_table: str) -> dict[str, Any]:
    """xmin 기반 증분 동기화의 무결성을 검증
    
    Args:
        source_table: 소스 테이블명
        target_table: 타겟 테이블명
    
    Returns:
        무결성 검증 결과 딕셔너리
        
        {
            "source_table": str,              # 소스 테이블명
            "target_table": str,              # 타겟 테이블명
            "source_max_xmin": int,           # 소스 최대 xmin
            "target_last_xmin": int,          # 타겟 마지막 xmin
            "is_synchronized": bool,          # 동기화 상태
            "sync_gap": int,                  # 동기화 간격
            "source_count": int,              # 소스 테이블 행 수
            "target_count": int,              # 타겟 테이블 행 수
            "xmin_distribution": dict,        # xmin 분포 정보
            "integrity_score": float,         # 무결성 점수 (0.0 ~ 1.0)
            "status": str                     # 검증 상태
        }
    
    Raises:
        Exception: 검증 과정에서 오류 발생 시
    
    Example:
        >>> integrity = copy_engine.validate_xmin_incremental_integrity(
        ...     source_table="fds.sym_v1_sym_ticker_exchange",
        ...     target_table="raw_data.sym_v1_sym_ticker_exchange"
        ... )
        >>> print(f"무결성 점수: {integrity['integrity_score']:.2f}")
        무결성 점수: 0.95
        >>> print(f"동기화 상태: {'동기화됨' if integrity['is_synchronized'] else '동기화 필요'}")
        동기화 상태: 동기화됨
    """
```

**검증 항목**:
- **동기화 상태**: `target_last_xmin >= source_max_xmin`
- **동기화 간격**: `source_max_xmin - target_last_xmin`
- **데이터 일치성**: 소스/타겟 테이블 행 수 비교
- **xmin 분포**: 최소/최대/평균/표준편차 분석

**무결성 점수 계산**:
- **기본 점수**: 1.0
- **동기화 지연**: -0.3 (동기화되지 않은 경우)
- **간격 과다**: -0.2 (100만 이상), -0.1 (10만 이상)
- **데이터 불일치**: -0.2 × (1 - 일치율)

---

## 🔧 **워커 설정 관련**

### **워커 설정 구조**
```python
worker_config = {
    "default_workers": 4,              # 기본 워커 수
    "max_workers": 4,                  # 최대 워커 수
    "min_workers": 2,                  # 최소 워커 수
    "worker_timeout_seconds": 600,     # 워커 타임아웃 (초)
    "worker_memory_limit_mb": 1024,    # 워커 메모리 제한 (MB)
}
```

### **워커 수별 배치 크기 자동 조정**
```python
# export_to_csv_with_xmin에서 자동 적용
if batch_size is None:
    default_workers = self.worker_config.get("default_workers", 4)
    if default_workers <= 2:
        batch_size = 5000      # 메모리 효율성 우선
    elif default_workers <= 4:
        batch_size = 10000     # 균형 모드
    else:
        batch_size = 15000     # 성능 우선
```

---

## 📊 **에러 처리 및 로깅**

### **에러 처리 전략**
1. **데이터베이스 연결 실패**: 기본값 사용 및 경고 로그
2. **xmin 조회 실패**: 0 반환 및 경고 로그
3. **MERGE 실행 실패**: 임시 테이블 정리 후 에러 반환
4. **파일 I/O 실패**: 상세한 에러 메시지 및 스택 트레이스

### **로깅 레벨**
- **INFO**: 정상적인 진행 상황 및 성능 지표
- **WARNING**: 성능 저하 가능성 및 권장사항
- **ERROR**: 실행 실패 및 복구 방안
- **CRITICAL**: 시스템 장애 및 즉시 조치 필요

### **로그 메시지 예시**
```
INFO - 워커 설정 로드 완료: {'default_workers': 4, 'max_workers': 4, 'min_workers': 2}
INFO - 소스 테이블 fds.sym_v1_sym_ticker_exchange의 최대 xmin: 1234567890
INFO - 워커 수(4) 기반 배치 크기 자동 설정: 10000
INFO - 배치 처리 진행률: 10000/50000 (10000행 처리됨)
INFO - source_xmin(1234567890)을 포함한 CSV 내보내기 완료: /tmp/data.csv, 총 50000행
INFO - source_xmin(1234567890)을 포함한 MERGE 실행: temp_table -> target_table
INFO - MERGE 완료: temp_table -> target_table, 소스: 50000행, 타겟: 100000행, 최대 xmin: 1234567890
```

---

## 🚨 **제한사항 및 주의사항**

### **PostgreSQL 전용 기능**
- `xmin` 시스템 필드는 PostgreSQL에서만 사용 가능
- 다른 데이터베이스에서는 타임스탬프 기반으로 자동 전환

### **복제본 환경 제한**
- 복제본에서는 xmin 기반 처리 불가
- 자동으로 타임스탬프 기반 증분 동기화로 전환

### **xmin 순환 위험**
- xmin 범위가 20억 이상이면 전체 동기화 모드로 전환
- 안전성을 위한 자동 폴백 메커니즘

### **메모리 사용량**
- 워커 수 증가에 따른 메모리 사용량 증가
- 배치 크기 증가에 따른 메모리 사용량 증가
- 시스템 리소스 모니터링 필요

---

## 🔮 **향후 확장 계획**

### **단기 (1-2주)**
- [ ] 하이브리드 모드 (xmin + 타임스탬프)
- [ ] 폴백 전략 개선
- [ ] 실시간 성능 모니터링

### **중기 (1-2개월)**
- [ ] 동적 워커 수 조정
- [ ] 배치 크기 자동 최적화
- [ ] 분산 처리 지원

### **장기 (3-6개월)**
- [ ] 스트리밍 처리 모드
- [ ] 머신러닝 기반 최적화
- [ ] 클라우드 네이티브 지원

---

## 📞 **지원 및 문의**

### **문제 해결**
- **로그 확인**: 상세한 로그로 문제 진단
- **성능 테스트**: `performance_test.py`로 성능 문제 진단
- **설정 검증**: `simple_test.py`로 설정 문제 진단

### **추가 지원**
- **코드 리뷰**: 구현된 메서드들의 상세한 코드 리뷰
- **성능 튜닝**: 특정 환경에 맞는 성능 최적화
- **기능 확장**: 새로운 요구사항에 따른 기능 추가

---

**문서 버전**: 1.0.0  
**마지막 업데이트**: 2025-08-19  
**구현 상태**: 완료 ✅ 