# 스테이징 테이블 마이그레이션 1차 구현 완료 요약

## 구현 완료 현황

### ✅ 1차 구현 완료 항목 (1-2주)

#### 1. DataCopyEngine 클래스에 스테이징 테이블 관련 메서드 추가

##### 1.1 핵심 메서드들
- **`create_staging_table()`**: 타겟 DB에 스테이징 테이블 생성
- **`copy_data_to_staging()`**: 소스 → 스테이징 데이터 복사
- **`merge_from_staging()`**: 스테이징 → 최종 테이블 MERGE
- **`cleanup_staging_table()`**: 스테이징 테이블 정리
- **`copy_with_staging_table()`**: 전체 프로세스 관리

##### 1.2 보조 메서드들
- **`_generate_staging_table_sql()`**: 스테이징 테이블 생성 SQL 생성
- **`_copy_with_psql_copy()`**: psql COPY 명령어를 사용한 데이터 복사
- **`_execute_merge()`**: MERGE SQL 실행
- **`_get_source_count()`**: 소스 테이블 데이터 개수 조회
- **`_get_staging_count()`**: 스테이징 테이블 데이터 개수 조회
- **`_get_target_count()`**: 타겟 테이블 데이터 개수 조회

##### 1.3 증분 동기화 지원 메서드들
- **`_build_incremental_where_condition()`**: 증분 동기화 WHERE 조건 생성
- **`_get_latest_incremental_value()`**: 최신 증분 값 조회
- **`_table_exists()`**: 테이블 존재 여부 확인

#### 2. 기존 메서드 수정

##### 2.1 `copy_data_with_custom_sql()` 메서드 수정
- 기존 CSV 기반 방식에서 스테이징 테이블 방식으로 변경
- 증분 동기화 조건 자동 생성 지원
- 스테이징 테이블을 통한 안전한 데이터 복사

#### 3. 새로운 워크플로우 구현

```
1. 소스 테이블 스키마 조회
2. 타겟 DB에 스테이징 테이블 생성 (UNLOGGED)
3. 소스 → 스테이징 데이터 복사 (psql COPY)
4. 스테이징 → 최종 테이블 MERGE (트랜잭션 보장)
5. 스테이징 테이블 정리 (항상 실행 보장)
6. 데이터 무결성 검증
```

## 주요 개선사항

### 1. 스키마 불일치 문제 해결
- **기존**: 임시 테이블이 타겟 DB에 생성되지만 소스 DB에서 접근 시도
- **개선**: 스테이징 테이블을 타겟 DB에 생성하고 psql COPY로 직접 복사

### 2. 데이터베이스 연결 분리 문제 해결
- **기존**: 소스 DB와 타겟 DB 간 세션 불일치
- **개선**: 각 단계별로 적절한 DB 연결 사용

### 3. 데이터 무결성 향상
- **기존**: CSV 파일을 통한 중간 단계로 인한 데이터 손실 가능성
- **개선**: psql COPY를 통한 직접 복사로 데이터 무결성 보장

### 4. 성능 최적화
- **UNLOGGED 테이블**: WAL 로깅 비활성화로 성능 향상
- **psql COPY**: 네이티브 PostgreSQL 복사 명령어 사용
- **트랜잭션 보장**: MERGE 작업의 원자성 보장

## 구현된 코드 구조

### 1. 메서드 계층 구조
```
DataCopyEngine
├── create_staging_table()           # 스테이징 테이블 생성
│   └── _generate_staging_table_sql() # SQL 생성
├── copy_data_to_staging()           # 데이터 복사
│   ├── _copy_with_psql_copy()      # psql COPY 실행
│   ├── _get_source_count()         # 소스 데이터 개수
│   └── _get_staging_count()        # 스테이징 데이터 개수
├── merge_from_staging()             # MERGE 작업
│   ├── _execute_merge()            # MERGE SQL 실행
│   └── _get_target_count()         # 타겟 데이터 개수
├── cleanup_staging_table()          # 정리
└── copy_with_staging_table()        # 전체 프로세스 관리
```

### 2. 에러 핸들링 및 복구
- 각 단계별 상세한 에러 로깅
- `finally` 블록을 통한 스테이징 테이블 정리 보장
- 트랜잭션 실패 시 자동 롤백

### 3. 증분 동기화 지원
- 다양한 데이터 타입별 WHERE 조건 자동 생성
- 타겟 테이블 존재 여부 확인
- 최신 값 기반 증분 조건 구성

## 테스트 준비

### 1. 테스트 스크립트 생성
- `test_staging_table_engine.py`: 새로운 메서드들 테스트
- 메서드 구조 및 로직 검증
- 에러 핸들링 테스트

### 2. 테스트 시나리오
- 스테이징 테이블 생성/삭제
- 데이터 복사 프로세스
- MERGE 작업 검증
- 증분 동기화 조건 생성

## 다음 단계 (2차 구현)

### 🔄 2차 구현 예정 항목 (3-4주)

#### 1. 에러 핸들링 및 복구 메커니즘 강화
- [ ] 상세한 에러 분류 및 처리
- [ ] 재시도 메커니즘 구현
- [ ] 부분 실패 시 복구 전략

#### 2. 성능 최적화
- [ ] 배치 크기 최적화
- [ ] 병렬 처리 구현
- [ ] 메모리 사용량 모니터링

#### 3. 모니터링 및 로깅 개선
- [ ] 구조화된 로깅 (JSON 형식)
- [ ] 성능 메트릭 수집
- [ ] 진행 상황 추적

#### 4. 단위 테스트 작성
- [ ] 핵심 로직 단위 테스트
- [ ] 모킹을 사용한 외부 의존성 격리
- [ ] 테스트 커버리지 80% 이상

## 사용법 예시

### 1. 기본 사용법
```python
# DataCopyEngine 인스턴스 생성
engine = DataCopyEngine(db_ops)

# 스테이징 테이블 방식으로 데이터 복사
result = engine.copy_with_staging_table(
    source_table="source_db.table_name",
    target_table="target_db.table_name",
    primary_keys=["id"],
    where_condition="created_at > '2024-01-01'"
)
```

### 2. 증분 동기화 사용법
```python
# 증분 동기화를 위한 WHERE 조건 자동 생성
where_condition = engine._build_incremental_where_condition(
    incremental_field="created_at",
    incremental_field_type="timestamp",
    target_table="target_db.table_name"
)
```

## 주의사항

### 1. 시스템 요구사항
- `psql` 명령어가 시스템에 설치되어 있어야 함
- PostgreSQL 클라이언트 라이브러리 필요
- 적절한 데이터베이스 권한 설정

### 2. 성능 고려사항
- 대용량 데이터의 경우 메모리 사용량 모니터링 필요
- 네트워크 대역폭 고려
- 타겟 DB의 디스크 공간 확인

### 3. 보안 고려사항
- 데이터베이스 비밀번호가 환경변수로 노출되지 않도록 주의
- 네트워크 보안 설정 확인
- 최소 권한 원칙 적용

## 결론

1차 구현을 통해 스테이징 테이블 방식의 핵심 기능이 완성되었습니다. 기존의 스키마 불일치 문제와 데이터베이스 연결 분리 문제를 해결하고, 더 안정적이고 효율적인 데이터 복사 프로세스를 구현했습니다.

다음 단계로는 에러 핸들링 강화, 성능 최적화, 그리고 포괄적인 테스트 작성이 필요합니다. 이를 통해 프로덕션 환경에서 안정적으로 사용할 수 있는 수준으로 발전시킬 수 있을 것입니다. 