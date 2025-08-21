# PostgreSQL Xmin 기반 증분 동기화 기능

## 📋 개요

이 프로젝트는 PostgreSQL의 시스템 필드 `xmin`을 활용한 정확한 증분 데이터 동기화 기능을 제공합니다. `xmin`은 PostgreSQL에서 각 행의 삽입 트랜잭션 ID를 나타내는 시스템 컬럼으로, 이를 활용하면 비즈니스 컬럼 변경에 영향받지 않는 정확한 증분 처리가 가능합니다.

## 🚀 주요 특징

### 1. **정확한 증분 처리**
- PostgreSQL 시스템 필드 `xmin`을 활용한 트랜잭션 기반 증분 처리
- 비즈니스 컬럼 변경에 영향받지 않는 안정적인 동기화
- 자동 안정성 검증 및 폴백 전략

### 2. **자동 관리**
- `source_xmin` 컬럼 자동 생성 및 관리
- 인덱스 자동 생성으로 성능 최적화
- 복제 환경 자동 감지 및 적응

### 3. **고성능 처리**
- 배치 단위 데이터 처리
- CSV 기반 효율적인 데이터 전송
- MERGE 작업으로 UPSERT 처리

### 4. **모니터링 및 검증**
- 상세한 진행 상황 추적
- 데이터 무결성 자동 검증
- xmin 기반 동기화 상태 모니터링

## 🏗️ 아키텍처

### 핵심 컴포넌트

```
┌─────────────────────────────────────────────────────────────┐
│                    Xmin Incremental Sync                    │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────┐               │
│  │   DAG Layer     │    │  Common Layer   │               │
│  │                 │    │                 │               │
│  │ • xmin_incremental_sync_dag.py         │               │
│  │ • postgres_data_copy_dag_refactored.py │               │
│  └─────────────────┘    └─────────────────┘               │
│           │                       │                        │
│           ▼                       ▼                        │
│  ┌─────────────────┐    ┌─────────────────┐               │
│  │  DataCopyEngine │    │DatabaseOperations│               │
│  │                 │    │                 │               │
│  │ • copy_table_data_with_xmin()          │               │
│  │ • _export_to_csv_with_xmin()           │               │
│  │ • _execute_xmin_merge()                │               │
│  └─────────────────┘    └─────────────────┘               │
│           │                       │                        │
│           ▼                       ▼                        │
│  ┌─────────────────┐    ┌─────────────────┐               │
│  │   PostgreSQL    │    │   PostgreSQL    │               │
│  │   (Source)      │    │   (Target)      │               │
│  │                 │    │                 │               │
│  │ • xmin 컬럼     │    │ • source_xmin   │               │
│  │ • 트랜잭션 ID   │    │ • 증분 처리     │               │
│  └─────────────────┘    └─────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

### 데이터 흐름

1. **소스 테이블에서 xmin 값 조회**
2. **타겟 테이블에 source_xmin 컬럼 확인/추가**
3. **마지막 처리된 xmin 값 조회**
4. **xmin 기반 증분 조건 생성**
5. **CSV 파일로 데이터 추출 (xmin 포함)**
6. **임시 테이블에 데이터 로드**
7. **MERGE 작업 실행 (source_xmin 컬럼 포함)**
8. **데이터 무결성 검증**

## 📁 파일 구조

```
airflow/dags/
├── xmin_incremental_sync_dag.py          # xmin 전용 DAG
├── postgres_data_copy_dag_refactored.py  # 기존 DAG (xmin 함수 추가)
└── common/
    ├── database_operations.py            # xmin 관련 메서드 추가
    ├── data_copy_engine.py               # xmin 기반 복사 기능
    └── settings.py                       # xmin 설정 관리

test_xmin_functionality.py                # xmin 기능 테스트
README_XMIN_FEATURE.md                    # 이 문서
```

## ⚙️ 설정

### 환경변수

```bash
# xmin 기능 활성화
ENABLE_XMIN_TRACKING=true

# xmin 안정성 임계값
XMIN_STABILITY_THRESHOLD=1500000000      # 15억 (경고)
XMIN_CRITICAL_THRESHOLD=2000000000       # 20억 (위험)

# xmin 체크 간격
XMIN_CHECK_INTERVAL_HOURS=24

# 복제 상태 확인
ENABLE_REPLICATION_CHECK=true

# 타임스탬프 폴백
FALLBACK_TO_TIMESTAMP=true

# 배치 크기 조정
XMIN_BATCH_SIZE_MULTIPLIER=0.5           # 기본 배치 크기의 50%
```

### 테이블 설정

```python
# xmin 기반 증분 동기화 설정
{
    "source": "m23.edi_690",
    "target": "raw_data.edi_690",
    "primary_key": ["eventcd", "eventid", "optionid", "serialid", "scexhid", "sedolid"],
    "sync_mode": "xmin_incremental",
    "batch_size": 5000,                   # xmin 처리는 기본값 5000
    "xmin_tracking": True,
    "fallback_to_timestamp": True,        # xmin 실패 시 타임스탬프로 폴백
    "description": "EDI 690 이벤트 데이터 - xmin 기반 증분 동기화"
}
```

## 🔧 사용법

### 1. 기본 사용법

```python
from common.database_operations import DatabaseOperations
from common.data_copy_engine import DataCopyEngine

# 객체 생성
db_ops = DatabaseOperations(source_conn_id, target_conn_id)
copy_engine = DataCopyEngine(db_ops)

# xmin 기반 증분 데이터 복사
result = copy_engine.copy_table_data_with_xmin(
    source_table="m23.edi_690",
    target_table="raw_data.edi_690",
    primary_keys=["eventcd", "eventid", "optionid", "serialid", "scexhid", "sedolid"],
    sync_mode="xmin_incremental",
    batch_size=5000
)
```

### 2. DAG에서 사용

```python
# xmin 전용 DAG 실행
dag_id = "xmin_incremental_sync"

# 또는 기존 DAG에서 xmin 함수 사용
def copy_data_with_xmin_incremental(table_config: dict, **context):
    # xmin 기반 증분 복사 로직
    pass
```

### 3. 설정 검증

```python
from common.settings import XminTableConfig

# 설정 검증
validation_result = XminTableConfig.validate_xmin_config(table_config)
if not validation_result["is_valid"]:
    for error in validation_result["errors"]:
        print(f"오류: {error}")
```

## 📊 모니터링

### 체크포인트

- **xmin 안정성**: 소스 테이블의 xmin 값 안정성 상태
- **복제 상태**: 데이터베이스 복제 상태 확인
- **source_xmin 컬럼**: 타겟 테이블의 source_xmin 컬럼 상태
- **xmin 범위**: 처리된 xmin 값의 범위
- **동기화 상태**: xmin 기반 동기화 완료 여부

### 성능 메트릭

- **처리 시간**: xmin 기반 복사 작업 소요 시간
- **처리 레코드 수**: 증분 처리된 레코드 수
- **배치 크기**: 최적화된 배치 크기
- **메모리 사용량**: CSV 처리 및 임시 테이블 사용량

## 🧪 테스트

### 테스트 실행

```bash
# xmin 기능 테스트
python test_xmin_functionality.py

# 특정 테스트만 실행
python -c "
from test_xmin_functionality import test_xmin_settings
test_xmin_settings()
"
```

### 테스트 내용

1. **XminTableConfig 설정 테스트**
   - xmin 설정 조회
   - 환경별 테이블 설정
   - 설정 검증

2. **DatabaseOperations xmin 메서드 테스트**
   - xmin 안정성 검증
   - 복제 상태 확인
   - source_xmin 컬럼 관리

3. **DataCopyEngine xmin 기능 테스트**
   - xmin 기반 복사 메서드 존재 확인
   - CSV 추출 및 MERGE 기능

4. **DAG 함수 테스트**
   - xmin DAG 파일 존재 확인
   - 기존 DAG에 xmin 함수 추가 확인

## ⚠️ 주의사항

### 1. **xmin 값 순환**
- PostgreSQL 트랜잭션 ID는 약 21억 개 후 순환
- 자동 VACUUM으로 인한 xmin 값 불안정성 가능
- 정기적인 전체 동기화로 데이터 일관성 보장

### 2. **복제 환경**
- 읽기 전용 복제본에서는 xmin 기반 처리 비활성화
- 복제 지연으로 인한 xmin 값 불일치 가능
- 복제 상태 자동 감지 및 폴백 전략

### 3. **성능 고려사항**
- source_xmin 컬럼에 인덱스 생성으로 조회 성능 향상
- 대용량 테이블의 경우 배치 크기 조정 필요
- CSV 추출 시 메모리 사용량 모니터링

### 4. **데이터 무결성**
- 기본키 기반 데이터 일치 여부 확인
- xmin 기반 증분 동기화 상태 검증
- 실패 시 적절한 에러 핸들링 및 복구

## 🔄 폴백 전략

### 1. **xmin 실패 시 타임스탬프 폴백**

```python
# xmin 안정성 검증 실패 시
if xmin_stability == "force_full_sync":
    # 전체 동기화 모드로 전환
    return copy_engine.copy_table_data(
        source_table=source_table,
        target_table=target_table,
        primary_keys=primary_keys,
        sync_mode="full_sync"
    )

# 복제본 환경에서 xmin 처리 불가 시
if not replication_ok:
    # 타임스탬프 기반으로 전환
    return copy_engine.copy_table_data(
        source_table=source_table,
        target_table=target_table,
        primary_keys=primary_keys,
        sync_mode="incremental_sync"
    )
```

### 2. **하이브리드 모드**

```python
# xmin + 타임스탬프 하이브리드 설정
{
    "sync_mode": "hybrid_incremental",
    "xmin_tracking": True,
    "timestamp_tracking": True,
    "incremental_field": "changed",
    "fallback_strategy": "timestamp_first"
}
```

## 📈 성능 최적화

### 1. **배치 크기 최적화**

```python
# xmin 처리를 위한 배치 크기 조정
batch_size = table_config.get("batch_size", 5000)  # 기본값 5000

# 환경변수로 배치 크기 조정
batch_multiplier = float(os.getenv("XMIN_BATCH_SIZE_MULTIPLIER", "0.5"))
adjusted_batch_size = int(batch_size * batch_multiplier)
```

### 2. **인덱스 최적화**

```sql
-- 부분 인덱스로 성능 향상
CREATE INDEX CONCURRENTLY idx_table_source_xmin 
ON target_table (source_xmin) 
WHERE source_xmin IS NOT NULL;

-- 복합 인덱스 (필요시)
CREATE INDEX idx_table_xmin_pk 
ON target_table (source_xmin, primary_key1, primary_key2);
```

### 3. **CSV 압축**

```python
# 압축된 CSV 추출로 파일 크기 감소
export_cmd = [
    "psql", ...,
    "-c", f"\\copy ({select_sql}) TO '{csv_path}.gz' WITH CSV HEADER"
]
```

## 🚨 문제 해결

### 1. **xmin 컬럼 생성 실패**

```python
# 로그 확인
logger.error(f"source_xmin 컬럼 추가 실패: {e}")

# 수동 해결
ALTER TABLE target_table ADD COLUMN source_xmin BIGINT;
CREATE INDEX idx_target_table_source_xmin ON target_table (source_xmin);
```

### 2. **CSV 추출 실패**

```python
# psql 명령어 확인
logger.error(f"CSV 추출 실패: {result.stderr}")

# 권한 확인
GRANT SELECT ON source_table TO user;
GRANT USAGE ON SCHEMA source_schema TO user;
```

### 3. **MERGE 작업 실패**

```python
# 기본키 조건 확인
logger.error(f"xmin 기반 MERGE 실패: {e}")

# 컬럼 존재 여부 확인
SELECT column_name FROM information_schema.columns 
WHERE table_name = 'target_table' AND column_name = 'source_xmin';
```

## 🔮 향후 계획

### 1. **단기 계획 (1-2개월)**
- [ ] xmin 기능 안정성 테스트 및 검증
- [ ] 성능 최적화 및 튜닝
- [ ] 모니터링 대시보드 구축

### 2. **중기 계획 (3-6개월)**
- [ ] 하이브리드 모드 완성
- [ ] 자동 폴백 전략 고도화
- [ ] 실시간 동기화 기능 추가

### 3. **장기 계획 (6개월 이상)**
- [ ] 다른 데이터베이스 시스템 지원
- [ ] 분산 처리 및 확장성 개선
- [ ] AI 기반 최적화 전략

## 📚 참고 자료

### PostgreSQL 문서
- [System Columns](https://www.postgresql.org/docs/current/ddl-system-columns.html)
- [Transaction IDs](https://www.postgresql.org/docs/current/datatype-oid.html)
- [VACUUM](https://www.postgresql.org/docs/current/sql-vacuum.html)

### 관련 기술
- [Change Data Capture (CDC)](https://en.wikipedia.org/wiki/Change_data_capture)
- [Incremental ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load)
- [PostgreSQL Replication](https://www.postgresql.org/docs/current/warm-standby.html)

## 🤝 기여하기

### 개발 환경 설정

```bash
# 의존성 설치
pip install -r requirements.txt

# 코드 포맷팅
black airflow/dags/
ruff check airflow/dags/

# 테스트 실행
python test_xmin_functionality.py
```

### 코드 리뷰 체크리스트

- [ ] 타입 힌트 추가
- [ ] docstring 작성
- [ ] 에러 핸들링 구현
- [ ] 로깅 추가
- [ ] 테스트 코드 작성

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

---

**문의사항이나 버그 리포트가 있으시면 이슈를 생성해 주세요.**

**개발팀: data_team@example.com** 