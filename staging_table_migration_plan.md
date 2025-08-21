# 스테이징 테이블 생성 방식 프로세스 수정 계획

## 1. 프로젝트 개요

### 1.1 목적
- 기존 임시 테이블 방식의 스키마 불일치 문제 해결
- 더 안정적이고 효율적인 데이터 복사 프로세스 구현
- 데이터 무결성 향상 및 에러 발생률 감소

### 1.2 배경
- 현재 임시 테이블이 타겟 DB에 생성되지만 소스 DB에서 접근하려고 시도하는 문제
- 스트리밍 파이프는 성공하지만 MERGE 작업에서 임시 테이블을 찾을 수 없는 오류 발생
- 데이터 무결성 검증 실패로 인한 반복적인 태스크 실패

### 1.3 범위
- `DataCopyEngine` 클래스의 스트리밍 파이프 로직 수정
- 임시 테이블 생성 및 관리 방식 변경
- MERGE 작업 프로세스 개선
- 에러 핸들링 및 복구 메커니즘 강화

## 2. 현재 문제점 분석

### 2.1 주요 문제
```
ERROR - MERGE 작업 실패: temp_raw_data_sym_v1_sym_coverage_1755730082 -> raw_data.sym_v1_sym_coverage, 
오류: "temp_raw_data_sym_v1_sym_coverage_1755730082" 이름의 릴레이션(relation)이 없습니다
```

### 2.2 근본 원인
1. **스키마 불일치**: 임시 테이블이 `raw_data` 스키마에 생성되지만 소스 DB에서 접근 시도
2. **데이터베이스 연결 분리**: 소스 DB(`fs2_postgres`)와 타겟 DB(`postgres_default`) 분리
3. **세션 관리 문제**: 임시 테이블 생성과 사용 간 세션 불일치
4. **권한 문제**: 소스 DB에서 타겟 DB의 임시 테이블 접근 권한 부족

### 2.3 영향받는 컴포넌트
- `data_copy_engine.py`: 스트리밍 파이프 및 MERGE 로직
- `database_operations.py`: 테이블 생성 및 관리
- `monitoring.py`: 에러 추적 및 로깅

## 3. 해결 방안: 스테이징 테이블 방식

### 3.1 핵심 개념
- **스테이징 테이블**: 타겟 DB에 실제 테이블로 생성되는 임시 테이블
- **단계별 처리**: 소스 → 스테이징 → 최종 테이블 순서로 데이터 이동
- **트랜잭션 보장**: 각 단계별 트랜잭션 관리로 데이터 일관성 확보

### 3.2 새로운 워크플로우
```
1. 소스 테이블 스키마 조회
2. 타겟 DB에 스테이징 테이블 생성
3. 소스 → 스테이징 데이터 복사 (psql COPY)
4. 스테이징 → 최종 테이블 MERGE
5. 스테이징 테이블 정리
6. 데이터 무결성 검증
```

## 4. 상세 구현 계획

### 4.1 1단계: 스테이징 테이블 생성 로직 수정

#### 4.1.1 `DataCopyEngine` 클래스 수정
```python
def create_staging_table(self, target_table: str, source_schema: dict) -> str:
    """
    타겟 DB에 스테이징 테이블 생성
    
    Args:
        target_table: 최종 타겟 테이블명 (예: raw_data.sym_v1_sym_coverage)
        source_schema: 소스 테이블 스키마 정보
    
    Returns:
        생성된 스테이징 테이블명
    """
    # 고유한 스테이징 테이블명 생성
    staging_table_name = f"staging_{target_table.split('.')[-1]}_{int(time.time())}"
    full_staging_name = f"{target_table.split('.')[0]}.{staging_table_name}"
    
    # 스테이징 테이블 생성 SQL 생성
    create_sql = self._generate_staging_table_sql(full_staging_name, source_schema)
    
    # 타겟 DB에 스테이징 테이블 생성
    with self.target_db.get_connection() as conn:
        conn.execute(create_sql)
        self.logger.info(f"스테이징 테이블 생성 완료: {full_staging_name}")
    
    return full_staging_name
```

#### 4.1.2 스테이징 테이블명 규칙
```python
# 명명 규칙: staging_{원본테이블명}_{타임스탬프}
staging_table_name = f"staging_{table_name}_{int(time.time())}"

# 예시:
# staging_sym_v1_sym_coverage_1755730082
# staging_sym_v1_sym_ticker_exchange_1755729458
```

### 4.2 2단계: 데이터 복사 로직 수정

#### 4.2.1 스트리밍 파이프 수정
```python
def copy_data_to_staging(self, source_table: str, staging_table: str, where_condition: str = None) -> dict:
    """
    소스 테이블에서 스테이징 테이블로 데이터 복사
    
    Args:
        source_table: 소스 테이블명
        staging_table: 스테이징 테이블명
        where_condition: WHERE 조건
    
    Returns:
        복사 결과 정보
    """
    try:
        # 1. 소스 데이터 카운트 조회
        source_count = self._get_source_count(source_table, where_condition)
        
        # 2. 스테이징 테이블로 직접 복사 (psql COPY)
        copy_result = self._copy_with_psql_copy(source_table, staging_table, where_condition)
        
        # 3. 복사된 데이터 검증
        staging_count = self._get_staging_count(staging_table)
        
        return {
            'status': 'success',
            'source_count': source_count,
            'staging_count': staging_count,
            'copy_time': copy_result['execution_time']
        }
        
    except Exception as e:
        self.logger.error(f"스테이징 테이블로 데이터 복사 실패: {str(e)}")
        raise
```

#### 4.2.2 psql COPY 명령어 구현
```python
def _copy_with_psql_copy(self, source_table: str, staging_table: str, where_condition: str = None) -> dict:
    """
    psql COPY 명령어를 사용한 데이터 복사
    """
    start_time = time.time()
    
    # WHERE 조건이 있는 경우 서브쿼리로 처리
    if where_condition:
        source_query = f"SELECT * FROM {source_table} WHERE {where_condition}"
    else:
        source_query = f"SELECT * FROM {source_table}"
    
    # COPY 명령어 실행
    copy_command = f"""
    psql -h {self.source_db.host} -U {self.source_db.user} -d {self.source_db.database} \
    -c "\\copy ({source_query}) TO STDOUT CSV HEADER" | \
    psql -h {self.target_db.host} -U {self.target_db.user} -d {self.target_db.database} \
    -c "\\copy {staging_table} FROM STDIN CSV HEADER"
    """
    
    # 명령어 실행
    result = subprocess.run(copy_command, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"COPY 명령어 실행 실패: {result.stderr}")
    
    execution_time = time.time() - start_time
    
    return {
        'status': 'success',
        'execution_time': execution_time,
        'stdout': result.stdout,
        'stderr': result.stderr
    }
```

### 4.3 3단계: MERGE 작업 로직 수정

#### 4.3.1 스테이징에서 최종 테이블로 MERGE
```python
def merge_from_staging(self, target_table: str, staging_table: str, primary_keys: list) -> dict:
    """
    스테이징 테이블에서 최종 테이블로 데이터 MERGE
    
    Args:
        target_table: 최종 타겟 테이블명
        staging_table: 스테이징 테이블명
        primary_keys: 기본키 리스트
    
    Returns:
        MERGE 결과 정보
    """
    try:
        # 1. MERGE 전 데이터 검증
        staging_count = self._get_staging_count(staging_table)
        target_before_count = self._get_target_count(target_table)
        
        # 2. MERGE 작업 실행
        merge_result = self._execute_merge(target_table, staging_table, primary_keys)
        
        # 3. MERGE 후 데이터 검증
        target_after_count = self._get_target_count(target_table)
        
        return {
            'status': 'success',
            'staging_count': staging_count,
            'target_before_count': target_before_count,
            'target_after_count': target_after_count,
            'merged_rows': target_after_count - target_before_count,
            'merge_time': merge_result['execution_time']
        }
        
    except Exception as e:
        self.logger.error(f"MERGE 작업 실패: {str(e)}")
        raise
```

#### 4.3.2 MERGE SQL 실행
```python
def _execute_merge(self, target_table: str, staging_table: str, primary_keys: list) -> dict:
    """
    MERGE SQL 실행
    """
    start_time = time.time()
    
    # 기본키 기반 MERGE SQL 생성
    pk_columns = ', '.join(primary_keys)
    pk_conditions = ' AND '.join([f"t.{pk} = s.{pk}" for pk in primary_keys])
    
    merge_sql = f"""
    BEGIN;
    
    -- 기존 데이터 삭제 (기본키 기준)
    DELETE FROM {target_table} t
    WHERE EXISTS (
        SELECT 1 FROM {staging_table} s
        WHERE {pk_conditions}
    );
    
    -- 새 데이터 삽입
    INSERT INTO {target_table}
    SELECT * FROM {staging_table};
    
    COMMIT;
    """
    
    # MERGE SQL 실행
    with self.target_db.get_connection() as conn:
        conn.execute(merge_sql)
    
    execution_time = time.time() - start_time
    
    return {
        'status': 'success',
        'execution_time': execution_time
    }
```

### 4.4 4단계: 스테이징 테이블 정리

#### 4.4.1 정리 로직 구현
```python
def cleanup_staging_table(self, staging_table: str) -> bool:
    """
    스테이징 테이블 정리
    
    Args:
        staging_table: 정리할 스테이징 테이블명
    
    Returns:
        정리 성공 여부
    """
    try:
        cleanup_sql = f"DROP TABLE IF EXISTS {staging_table}"
        
        with self.target_db.get_connection() as conn:
            conn.execute(cleanup_sql)
        
        self.logger.info(f"스테이징 테이블 정리 완료: {staging_table}")
        return True
        
    except Exception as e:
        self.logger.error(f"스테이징 테이블 정리 실패: {str(e)}")
        return False
```

#### 4.4.2 정리 보장 메커니즘
```python
def copy_with_staging_table(self, source_table: str, target_table: str, where_condition: str = None):
    """
    스테이징 테이블을 사용한 전체 데이터 복사 프로세스
    """
    staging_table = None
    
    try:
        # 1. 스테이징 테이블 생성
        staging_table = self.create_staging_table(target_table, self.source_schema)
        
        # 2. 데이터 복사 (소스 → 스테이징)
        copy_result = self.copy_data_to_staging(source_table, staging_table, where_condition)
        
        # 3. MERGE 작업 (스테이징 → 최종)
        merge_result = self.merge_from_staging(target_table, staging_table, self.primary_keys)
        
        # 4. 결과 반환
        return {
            'status': 'success',
            'copy_result': copy_result,
            'merge_result': merge_result
        }
        
    except Exception as e:
        self.logger.error(f"스테이징 테이블 방식 데이터 복사 실패: {str(e)}")
        raise
        
    finally:
        # 5. 스테이징 테이블 정리 (항상 실행 보장)
        if staging_table:
            self.cleanup_staging_table(staging_table)
```

## 5. 구현 우선순위 및 일정

### 5.1 1차 구현 (1-2주)
- [ ] `DataCopyEngine` 클래스의 스테이징 테이블 생성 로직 구현
- [ ] 데이터 복사 로직 수정 (psql COPY 방식)
- [ ] 기본적인 MERGE 작업 로직 구현
- [ ] 스테이징 테이블 정리 로직 구현

### 5.2 2차 구현 (3-4주)
- [ ] 에러 핸들링 및 복구 메커니즘 강화
- [ ] 성능 최적화 (배치 크기, 병렬 처리 등)
- [ ] 모니터링 및 로깅 개선
- [ ] 단위 테스트 작성

### 5.3 3차 구현 (5-6주)
- [ ] 통합 테스트 및 검증
- [ ] 성능 테스트 및 튜닝
- [ ] 문서화 및 운영 가이드 작성
- [ ] 프로덕션 배포 준비