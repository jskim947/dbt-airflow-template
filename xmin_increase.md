맞습니다! xmin을 실제로 증분 처리에 사용하려면 **타겟 테이블에도 xmin 값을 저장해야 합니다**. 

## xmin 기반 증분 처리의 올바른 구현 방식

### 1. 핵심 아이디어

#### 1.1 **xmin 값을 타겟에 저장**
```sql
-- 타겟 테이블에 xmin 컬럼 추가
ALTER TABLE raw_data.edi_690 
ADD COLUMN source_xmin BIGINT;

-- 소스의 xmin 값을 타겟에 복사
INSERT INTO raw_data.edi_690 (..., source_xmin)
SELECT ..., xmin as source_xmin 
FROM m23.edi_690;
```

#### 1.2 **xmin 기반 증분 조건**
```sql
-- 마지막으로 처리된 xmin 값보다 큰 레코드만 조회
SELECT * FROM m23.edi_690 
WHERE xmin > (
    SELECT COALESCE(MAX(source_xmin), 0) 
    FROM raw_data.edi_690
);
```

### 2. 실제 구현 계획

#### 2.1 **DatabaseOperations 클래스에 xmin 저장 기능 추가**

```python:airflow/dags/common/database_operations.py
def ensure_xmin_column_exists(
    self, 
    target_table: str
) -> str:
    """
    타겟 테이블에 source_xmin 컬럼이 존재하는지 확인하고 없으면 추가
    
    Args:
        target_table: 타겟 테이블명
        
    Returns:
        결과 메시지
    """
    try:
        target_hook = self.get_target_hook()
        
        # source_xmin 컬럼 존재 여부 확인
        column_exists_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = '{target_table.split('.')[-1]}'
                AND table_schema = '{target_table.split('.')[0]}'
                AND column_name = 'source_xmin'
            )
        """
        
        column_exists = target_hook.get_first(column_exists_sql)[0]
        
        if not column_exists:
            # source_xmin 컬럼 추가
            add_column_sql = f"""
                ALTER TABLE {target_table} 
                ADD COLUMN source_xmin BIGINT
            """
            target_hook.run(add_column_sql)
            
            # 인덱스 생성 (성능 향상)
            create_index_sql = f"""
                CREATE INDEX idx_{target_table.replace('.', '_')}_source_xmin 
                ON {target_table} (source_xmin)
            """
            target_hook.run(create_index_sql)
            
            logger.info(f"source_xmin 컬럼 추가 완료: {target_table}")
            return f"source_xmin 컬럼 추가 완료: {target_table}"
        else:
            logger.info(f"source_xmin 컬럼이 이미 존재: {target_table}")
            return f"source_xmin 컬럼이 이미 존재: {target_table}"
            
    except Exception as e:
        error_msg = f"source_xmin 컬럼 추가 실패: {e}"
        logger.error(error_msg)
        raise Exception(error_msg)

def get_last_processed_xmin_from_target(
    self, 
    target_table: str
) -> int:
    """
    타겟 테이블에서 마지막으로 처리된 xmin 값 조회
    
    Args:
        target_table: 타겟 테이블명
        
    Returns:
        마지막으로 처리된 xmin 값 (없으면 0)
    """
    try:
        target_hook = self.get_target_hook()
        
        # source_xmin 컬럼이 있는지 확인
        column_exists_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = '{target_table.split('.')[-1]}'
                AND table_schema = '{target_table.split('.')[0]}'
                AND column_default IS NULL
                AND column_name = 'source_xmin'
            )
        """
        
        column_exists = target_hook.get_first(column_exists_sql)[0]
        
        if not column_exists:
            logger.warning(f"source_xmin 컬럼이 존재하지 않음: {target_table}")
            return 0
        
        # 마지막 처리된 xmin 값 조회
        last_xmin_sql = f"""
            SELECT COALESCE(MAX(source_xmin), 0) 
            FROM {target_table}
        """
        
        result = target_hook.get_first(last_xmin_sql)
        last_xmin = result[0] if result and result[0] else 0
        
        logger.info(f"마지막 처리 xmin 조회: {target_table} -> {last_xmin}")
        return last_xmin
        
    except Exception as e:
        logger.error(f"마지막 처리 xmin 조회 실패: {e}")
        return 0

def build_xmin_incremental_condition(
    self,
    source_table: str,
    target_table: str,
    last_xmin: int = 0
) -> tuple[str, int]:
    """
    xmin 기반 증분 조건 생성
    
    Args:
        source_table: 소스 테이블명
        target_table: 타겟 테이블명
        last_xmin: 마지막으로 처리된 xmin 값
        
    Returns:
        (증분 조건 SQL, 최신 xmin 값) 튜플
    """
    try:
        source_hook = self.get_source_hook()
        
        if last_xmin == 0:
            # 첫 실행 시 전체 데이터 처리
            condition = "1=1"
            latest_xmin = 0
        else:
            # xmin 기반 증분 조건
            condition = f"xmin > {last_xmin}"
            
            # 최신 xmin 값 조회
            latest_xmin_sql = f"SELECT MAX(xmin) FROM {source_table}"
            latest_xmin_result = source_hook.get_first(latest_xmin_sql)
            latest_xmin = latest_xmin_result[0] if latest_xmin_result[0] else last_xmin
        
        logger.info(f"xmin 기반 증분 조건 생성: {condition}, 최신 xmin: {latest_xmin}")
        return condition, latest_xmin
        
    except Exception as e:
        logger.error(f"xmin 기반 증분 조건 생성 실패: {e}")
        raise
```

#### 2.2 **DataCopyEngine에 xmin 기반 복사 기능 구현**

```python:airflow/dags/common/data_copy_engine.py
def copy_table_data_with_xmin(
    self,
    source_table: str,
    target_table: str,
    primary_keys: list[str],
    sync_mode: str = "xmin_incremental",
    batch_size: int = 10000,
    custom_where: str | None = None,
) -> dict[str, Any]:
    """
    xmin 기반 증분 데이터 복사 (실제 xmin 값 저장)
    
    Args:
        source_table: 소스 테이블명
        target_table: 타겟 테이블명
        primary_keys: 기본키 컬럼 목록
        sync_mode: 동기화 모드
        batch_size: 배치 크기
        custom_where: 커스텀 WHERE 조건
        
    Returns:
        복사 결과 딕셔너리
    """
    try:
        logger.info(f"xmin 기반 증분 데이터 복사 시작: {source_table} -> {target_table}")
        
        # 1단계: 타겟 테이블에 source_xmin 컬럼 확인/추가
        self.db_ops.ensure_xmin_column_exists(target_table)
        
        # 2단계: 마지막으로 처리된 xmin 값 조회
        last_xmin = self.db_ops.get_last_processed_xmin_from_target(target_table)
        
        # 3단계: xmin 기반 증분 조건 생성
        incremental_condition, latest_xmin = self.db_ops.build_xmin_incremental_condition(
            source_table, target_table, last_xmin
        )
        
        # 4단계: 커스텀 WHERE 조건과 결합
        if custom_where:
            if incremental_condition == "1=1":
                where_clause = custom_where
            else:
                where_clause = f"({incremental_condition}) AND ({custom_where})"
        else:
            where_clause = incremental_condition
        
        # 5단계: xmin 포함 증분 데이터 조회
        select_sql = f"""
            SELECT *, xmin as source_xmin
            FROM {source_table}
            WHERE {where_clause}
            ORDER BY xmin
        """
        
        logger.info(f"xmin 기반 증분 데이터 조회 SQL: {select_sql}")
        
        # 6단계: CSV 파일로 데이터 추출 (xmin 포함)
        csv_path = self._export_to_csv_with_xmin(source_table, select_sql, batch_size)
        
        # 7단계: 임시 테이블 생성 및 데이터 로드
        temp_table = f"temp_{target_table.replace('.', '_')}"
        self._import_csv_in_session_with_xmin(temp_table, csv_path, batch_size)
        
        # 8단계: MERGE 작업 실행 (xmin 값 포함)
        merge_result = self._execute_xmin_merge(
            source_table, target_table, temp_table, primary_keys
        )
        
        # 9단계: 임시 파일 정리
        self.cleanup_temp_files(csv_path)
        
        result = {
            "status": "success",
            "source_table": source_table,
            "target_table": target_table,
            "sync_mode": sync_mode,
            "last_xmin": last_xmin,
            "latest_xmin": latest_xmin,
            "records_processed": merge_result.get("total_processed", 0),
            "merge_result": merge_result
        }
        
        logger.info(f"xmin 기반 증분 데이터 복사 완료: {result}")
        return result
        
    except Exception as e:
        error_msg = f"xmin 기반 증분 데이터 복사 실패: {e}"
        logger.error(error_msg)
        raise Exception(error_msg)

def _export_to_csv_with_xmin(
    self,
    source_table: str,
    select_sql: str,
    batch_size: int
) -> str:
    """
    xmin 포함 데이터를 CSV로 추출
    """
    try:
        csv_path = f"/tmp/{source_table.replace('.', '_')}_{int(time.time())}.csv"
        
        # psql 명령어로 데이터 추출 (xmin 포함)
        export_cmd = [
            "psql",
            "-h", self.source_host,
            "-p", str(self.source_port),
            "-U", self.source_user,
            "-d", self.source_database,
            "-c", f"\\copy ({select_sql}) TO '{csv_path}' WITH CSV HEADER"
        ]
        
        env = os.environ.copy()
        if self.source_password:
            env["PGPASSWORD"] = self.source_password
        
        result = subprocess.run(
            export_cmd, 
            env=env, 
            capture_output=True, 
            text=True
        )
        
        if result.returncode != 0:
            raise Exception(f"CSV 추출 실패: {result.stderr}")
        
        logger.info(f"xmin 포함 데이터 CSV 추출 완료: {csv_path}")
        return csv_path
        
    except Exception as e:
        logger.error(f"xmin 포함 데이터 CSV 추출 실패: {e}")
        raise

def _execute_xmin_merge(
    self,
    source_table: str,
    target_table: str,
    temp_table: str,
    primary_keys: list[str]
) -> dict[str, Any]:
    """
    xmin 기반 MERGE 작업 실행 (source_xmin 컬럼 포함)
    """
    try:
        target_hook = self.get_target_hook()
        
        # 기본키 조건 구성
        pk_conditions = " AND ".join([
            f"target.{pk} = source.{pk}" for pk in primary_keys
        ])
        
        # 업데이트할 컬럼 목록 (source_xmin 포함)
        update_columns = self._get_update_columns(target_table)
        if "source_xmin" not in update_columns:
            update_columns.append("source_xmin")
        
        # MERGE SQL 실행
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_table} AS source
            ON {pk_conditions}
            
            WHEN MATCHED THEN
                UPDATE SET
                    {', '.join([f"{col} = source.{col}" for col in update_columns])},
                    updated_at = CURRENT_TIMESTAMP
            
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({', '.join(self._get_all_columns(target_table))})
                VALUES ({', '.join([f"source.{col}" for col in self._get_all_columns(target_table)])});
        """
        
        logger.info(f"xmin 기반 MERGE SQL 실행: {merge_sql}")
        
        # MERGE 실행
        target_hook.run(merge_sql)
        
        # 결과 확인
        result_sql = f"SELECT COUNT(*) FROM {temp_table}"
        total_processed = target_hook.get_first(result_sql)[0]
        
        # 임시 테이블 삭제
        target_hook.run(f"DROP TABLE IF EXISTS {temp_table}")
        
        merge_result = {
            "total_processed": total_processed,
            "merge_sql": merge_sql
        }
        
        logger.info(f"xmin 기반 MERGE 완료: {merge_result}")
        return merge_result
        
    except Exception as e:
        logger.error(f"xmin 기반 MERGE 실패: {e}")
        raise
```

### 3. 실제 데이터 흐름

#### 3.1 **소스 테이블 (m23.edi_690)**
```sql
-- 소스 데이터 (xmin은 PostgreSQL이 자동 생성)
SELECT eventcd, eventid, xmin FROM m23.edi_690 LIMIT 3;

 eventcd | eventid | xmin  
---------+---------+-------
 ABC     | 123     | 12345
 DEF     | 456     | 12346
 GHI     | 789     | 12347
```

#### 3.2 **타겟 테이블 (raw_data.edi_690)**
```sql
-- 타겟 테이블에 source_xmin 컬럼 추가
ALTER TABLE raw_data.edi_690 ADD COLUMN source_xmin BIGINT;

-- 복사된 데이터 (xmin 값이 저장됨)
SELECT eventcd, eventid, source_xmin FROM raw_data.edi_690 LIMIT 3;

 eventcd | eventid | source_xmin  
---------+---------+-------------
 ABC     | 123     | 12345
 DEF     | 456     | 12346
 GHI     | 789     | 12347
```

#### 3.3 **증분 처리**
```sql
-- 다음 실행 시: 마지막 xmin(12347)보다 큰 레코드만 조회
SELECT * FROM m23.edi_690 
WHERE xmin > (
    SELECT COALESCE(MAX(source_xmin), 0) 
    FROM raw_data.edi_690
);

-- 결과: xmin > 12347인 새로운 레코드들만
```

### 4. 결론

**맞습니다! xmin을 실제 증분 처리에 사용하려면:**

1. ✅ **타겟 테이블에 `source_xmin` 컬럼 추가**
2. ✅ **소스의 `xmin` 값을 타겟에 복사**
3. ✅ **타겟의 `source_xmin` 값을 기준으로 증분 조건 생성**

이렇게 하면 **실제로 xmin 기반 증분 처리가 가능**하며, 소스와 타겟의 xmin 값이 일치하게 됩니다.

---

## 🚀 실제 구현 계획 (수정안)

### 5. 구현 단계별 계획

#### 5.1 **1단계: 기본 인프라 구축 (1-2주)**

##### 5.1.1 DatabaseOperations 클래스 확장
- `ensure_xmin_column_exists()` 메서드 구현
- `get_last_processed_xmin_from_target()` 메서드 구현  
- `build_xmin_incremental_condition()` 메서드 구현

##### 5.1.2 필요한 import 추가
```python
import subprocess  # CSV 추출을 위한 psql 명령어 실행
```

##### 5.1.3 테스트 환경에서 기본 기능 검증
- source_xmin 컬럼 자동 생성 테스트
- xmin 값 조회 및 증분 조건 생성 테스트

#### 5.2 **2단계: DataCopyEngine 확장 (2-3주)**

##### 5.2.1 핵심 메서드 구현
- `copy_table_data_with_xmin()` 메서드 구현
- `_export_to_csv_with_xmin()` 메서드 구현
- `_execute_xmin_merge()` 메서드 구현

##### 5.2.2 기존 메서드와의 통합
- `_get_update_columns()` 메서드에 source_xmin 컬럼 추가 로직
- `_get_all_columns()` 메서드에서 source_xmin 컬럼 포함

##### 5.2.3 CSV 추출 및 임시 테이블 로드
- psql 명령어를 통한 xmin 포함 데이터 추출
- 임시 테이블에 source_xmin 컬럼 포함하여 데이터 로드

#### 5.3 **3단계: DAG 통합 및 테스트 (3-4주)**

##### 5.3.1 새로운 DAG 함수 추가
```python:airflow/dags/postgres_data_copy_dag.py
def copy_data_with_xmin_incremental(table_config: dict, **context):
    """xmin 기반 증분 데이터 복사 DAG 함수"""
    
def validate_xmin_incremental_integrity(table_config: dict, **context):
    """xmin 기반 증분 동기화 무결성 검증"""
```

##### 5.3.2 설정 파일 업데이트
```python:airflow/dags/common/settings.py
@classmethod
def get_xmin_table_configs(cls) -> List[Dict[str, Any]]:
    """xmin 기반 증분 동기화 테이블 설정"""
```

##### 5.3.3 통합 테스트
- 전체 워크플로우 테스트
- 다양한 테이블에서 xmin 기반 증분 처리 검증
- 성능 테스트 및 최적화

#### 5.4 **4단계: 운영 환경 적용 (4주 이후)**

##### 5.4.1 단계적 적용
- 개발/테스트 환경에서 충분한 검증 후 운영 환경 적용
- 기존 증분 처리와 병행 운영으로 안정성 확보

##### 5.4.2 모니터링 및 알림
- xmin 기반 증분 처리 성공/실패 모니터링
- source_xmin 컬럼 값 이상 시 알림 설정

### 6. 구현 시 주의사항

#### 6.1 **기존 코드와의 호환성**
- 기존 증분 처리 로직은 그대로 유지
- xmin 기반 처리는 새로운 sync_mode로 구현
- 점진적 전환 가능하도록 설계

#### 6.2 **성능 고려사항**
- source_xmin 컬럼에 인덱스 생성으로 조회 성능 향상
- 대용량 테이블의 경우 배치 크기 조정
- CSV 추출 시 메모리 사용량 모니터링

#### 6.3 **오류 처리 및 복구**
- source_xmin 컬럼 생성 실패 시 적절한 오류 메시지
- CSV 추출 실패 시 재시도 메커니즘
- MERGE 작업 실패 시 롤백 전략

### 7. 예상 효과 및 장점

#### 7.1 **기술적 장점**
- **정확한 증분 처리**: 트랜잭션 ID 기반으로 변경사항 정확 추적
- **자동 관리**: 시스템 필드로 운영 부담 최소화
- **스키마 독립성**: 비즈니스 컬럼 변경에 영향받지 않음

#### 7.2 **운영적 장점**
- **실시간 동기화**: 트랜잭션 커밋 시점에 변경사항 감지
- **데이터 무결성**: 트랜잭션 기반으로 데이터 일관성 보장
- **확장성**: 새로운 테이블 추가 시 설정만으로 증분 처리 가능

### 8. 리스크 및 대응 방안

#### 8.1 **주요 리스크**
- **트랜잭션 ID 순환**: PostgreSQL 14에서 약 21억 개 후 순환
- **VACUUM 영향**: 자동 VACUUM으로 인한 xmin 값 불안정성
- **복제 환경**: 읽기 전용 복제본에서 xmin 값 차이

#### 8.2 **대응 방안**
- **정기적인 VACUUM**: 자동 VACUUM 설정으로 xmin 값 안정성 확보
- **모니터링**: xmin 값 변화를 지속적으로 모니터링
- **백업 전략**: 정기적인 전체 동기화로 데이터 일관성 보장

### 9. 결론 및 권장사항

#### 9.1 **구현 가능성**
PostgreSQL xmin 시스템 필드를 활용한 증분 처리는 **충분히 구현 가능**하며, 다음과 같은 이점을 제공합니다:

1. **정확한 변경사항 추적**: 트랜잭션 기반의 정확한 증분 처리
2. **자동 관리**: 시스템 필드로 운영 부담 최소화
3. **고성능**: 인덱싱된 시스템 컬럼으로 빠른 조회

#### 9.2 **권장 구현 순서**
1. **즉시 시작**: 1단계 기본 인프라 구축
2. **단계적 구현**: 각 단계별 충분한 테스트 후 다음 단계 진행
3. **운영 적용**: 개발/테스트 환경에서 안정성 확인 후 운영 적용

#### 9.3 **성공 요인**
- **충분한 테스트**: 각 단계별 철저한 테스트 및 검증
- **점진적 전환**: 기존 시스템과 병행 운영으로 안정성 확보
- **지속적 모니터링**: xmin 값 변화 및 시스템 상태 지속 모니터링

이 계획을 통해 PostgreSQL xmin 시스템 필드의 장점을 최대한 활용하면서도 안정적인 증분 처리 시스템을 구축할 수 있습니다.

---

## ⚠️ 기존 코드 적용 시 발생 가능한 문제점 검토

### 10. 기존 코드와의 호환성 문제

#### 10.1 **sync_mode 충돌 문제**

##### 10.1.1 현재 sync_mode 구조
```python
# 기존 코드에서 사용하는 sync_mode
TABLES_CONFIG = [
    {
        "sync_mode": "full_sync",        # 전체 동기화
        "sync_mode": "incremental_sync", # 기존 증분 동기화
        "sync_mode": "cdc_sync",         # 변경 데이터 캡처
    }
]

# xmin 기반 증분 처리를 위한 새로운 sync_mode 필요
"sync_mode": "xmin_incremental"  # 새로운 모드 추가
```

##### 10.1.2 문제점
- **기존 로직과 충돌**: `_build_where_clause()` 메서드에서 `sync_mode == "incremental_sync"` 조건으로 기존 증분 처리
- **중복 처리**: xmin 기반 처리와 기존 증분 처리가 동시에 실행될 수 있음
- **설정 혼란**: 개발자가 어떤 증분 방식을 사용할지 혼동할 수 있음

##### 10.1.3 해결 방안
```python
def _build_where_clause(self, ...):
    # 기존 증분 처리와 xmin 기반 처리 구분
    if sync_mode == "incremental_sync":
        # 기존 타임스탬프 기반 증분 처리
        incremental_condition = self._build_incremental_condition(...)
    elif sync_mode == "xmin_incremental":
        # xmin 기반 증분 처리
        incremental_condition = self._build_xmin_incremental_condition(...)
    else:
        # full_sync 등 다른 모드
        pass
```

#### 10.2 **기존 incremental_field와의 충돌**

##### 10.2.1 현재 구조
```python
# 기존 설정
{
    "incremental_field": "changed",           # 비즈니스 필드
    "incremental_field_type": "yyyymmdd",    # 필드 타입
    "sync_mode": "incremental_sync"
}

# xmin 기반 설정 (새로운 방식)
{
    "sync_mode": "xmin_incremental",         # xmin 사용
    # incremental_field 불필요 (xmin이 자동으로 증분 기준)
}
```

##### 10.2.2 문제점
- **설정 중복**: `incremental_field`와 xmin이 동시에 설정되면 어떤 것을 우선할지 불명확
- **기존 로직 영향**: `get_last_update_time()` 함수가 `incremental_field`를 필수로 요구
- **DAG 태스크 충돌**: 기존 증분 처리 태스크와 xmin 기반 태스크가 동시 실행될 수 있음

##### 10.2.3 해결 방안
```python
def get_last_update_time(table_config: dict, **context) -> Any:
    # xmin 기반 처리일 때는 다른 로직 사용
    if table_config.get("sync_mode") == "xmin_incremental":
        return self._get_last_xmin_value(table_config)
    
    # 기존 로직 유지
    if table_config.get("sync_mode") == "incremental_sync":
        # 기존 incremental_field 기반 로직
        pass
```

#### 10.3 **MERGE SQL 템플릿 충돌**

##### 10.3.1 현재 MERGE SQL 구조
```sql
-- airflow/dags/sql/incremental_merge.sql
MERGE INTO TARGET_TABLE AS target
USING TEMP_TABLE AS source
ON PRIMARY_KEY_CONDITION

WHEN MATCHED THEN
    UPDATE SET
        UPDATE_COLUMNS,
        INCREMENTAL_FIELD = source.INCREMENTAL_FIELD,  -- 기존 증분 필드
        updated_at = CURRENT_TIMESTAMP
```

##### 10.3.2 문제점
- **컬럼 불일치**: `INCREMENTAL_FIELD`가 xmin 기반 처리에서는 `source_xmin`으로 변경 필요
- **SQL 템플릿 분기**: xmin 기반과 기존 증분 처리를 위한 별도 SQL 템플릿 필요
- **파라미터 치환**: `INCREMENTAL_FIELD` 파라미터가 xmin 처리에서는 의미 없음

##### 10.3.3 해결 방안
```sql
-- 새로운 xmin_merge.sql 템플릿 생성
MERGE INTO TARGET_TABLE AS target
USING TEMP_TABLE AS source
ON PRIMARY_KEY_CONDITION

WHEN MATCHED THEN
    UPDATE SET
        UPDATE_COLUMNS,
        source_xmin = source.source_xmin,  -- xmin 값 업데이트
        updated_at = CURRENT_TIMESTAMP
```

#### 10.4 **컬럼 관리 로직 충돌**

##### 10.4.1 현재 컬럼 관리
```python
# airflow/dags/postgres_data_copy_dag.py
def prepare_merge_parameters(table_config: dict, **context) -> dict[str, Any]:
    # 증분 필드 제외 로직
    exclude_columns = ["created_at"]
    
    if table_config.get("sync_mode") == "incremental_sync" and table_config.get("incremental_field"):
        exclude_columns.append(table_config["incremental_field"])
    
    update_columns = [col for col in all_columns if col not in exclude_columns]
```

##### 10.4.2 문제점
- **컬럼 제외 로직**: xmin 기반 처리에서는 `source_xmin` 컬럼을 제외하면 안 됨
- **업데이트 컬럼 목록**: `source_xmin` 컬럼이 업데이트 대상에 포함되어야 함
- **INSERT 컬럼 목록**: `source_xmin` 컬럼이 INSERT 대상에 포함되어야 함

##### 10.4.3 해결 방안
```python
def prepare_merge_parameters(table_config: dict, **context) -> dict[str, Any]:
    exclude_columns = ["created_at"]
    
    if table_config.get("sync_mode") == "incremental_sync" and table_config.get("incremental_field"):
        exclude_columns.append(table_config["incremental_field"])
    elif table_config.get("sync_mode") == "xmin_incremental":
        # xmin 기반 처리에서는 source_xmin 컬럼을 제외하지 않음
        pass
    
    # source_xmin 컬럼이 있으면 업데이트 대상에 포함
    update_columns = [col for col in all_columns if col not in exclude_columns]
    if table_config.get("sync_mode") == "xmin_incremental":
        if "source_xmin" not in update_columns:
            update_columns.append("source_xmin")
```

### 11. 성능 및 리소스 문제

#### 11.1 **메모리 사용량 증가**

##### 11.1.1 문제점
- **추가 컬럼**: `source_xmin` 컬럼으로 인한 메모리 사용량 증가
- **CSV 파일 크기**: xmin 값 포함으로 CSV 파일 크기 증가
- **임시 테이블**: source_xmin 컬럼이 포함된 임시 테이블로 메모리 사용량 증가

##### 11.1.2 해결 방안
```python
# 배치 크기 조정
def copy_table_data_with_xmin(self, ..., batch_size: int = 5000):  # 기존 10000에서 감소
    # 메모리 사용량을 고려한 배치 크기 조정
    
# CSV 압축 옵션 추가
def _export_to_csv_with_xmin(self, ...):
    # 압축된 CSV 추출로 파일 크기 감소
    export_cmd = [
        "psql", ...,
        "-c", f"\\copy ({select_sql}) TO '{csv_path}.gz' WITH CSV HEADER"
    ]
```

#### 11.2 **데이터베이스 성능 영향**

##### 11.2.1 문제점
- **인덱스 추가**: `source_xmin` 컬럼에 인덱스 생성으로 INSERT/UPDATE 성능 저하
- **MERGE 성능**: source_xmin 컬럼 업데이트로 MERGE 작업 시간 증가
- **쿼리 복잡성**: xmin 기반 증분 조건으로 쿼리 복잡성 증가

##### 11.2.2 해결 방안
```python
# 부분 인덱스 사용으로 성능 최적화
def ensure_xmin_column_exists(self, target_table: str):
    # 전체 테이블이 아닌 NULL이 아닌 값에만 인덱스 생성
    create_index_sql = f"""
        CREATE INDEX CONCURRENTLY idx_{target_table.replace('.', '_')}_source_xmin 
        ON {target_table} (source_xmin) 
        WHERE source_xmin IS NOT NULL
    """
    
# 배치 MERGE로 성능 향상
def _execute_xmin_merge(self, ...):
    # 대용량 테이블의 경우 배치 단위로 MERGE 실행
    batch_size = 5000
    for i in range(0, total_count, batch_size):
        batch_merge_sql = f"{merge_sql} LIMIT {batch_size} OFFSET {i}"
```

### 12. 데이터 무결성 문제

#### 12.1 **xmin 값 순환 문제**

##### 12.1.1 문제점
- **PostgreSQL 제한**: 트랜잭션 ID가 약 21억 개 후 순환
- **데이터 손실**: 순환 후 이전 xmin 값과 새로운 xmin 값이 같아져 증분 처리 실패
- **VACUUM 영향**: 자동 VACUUM으로 인한 xmin 값 불안정성

##### 12.1.2 해결 방안
```python
def validate_xmin_stability(self, table_name: str):
    # xmin 값 순환 위험도 체크
    xmin_range = max_xmin - min_xmin
    
    if xmin_range > 2000000000:  # 20억 이상
        logger.critical(f"xmin 순환 위험: {table_name}")
        # 전체 동기화 모드로 전환
        return "force_full_sync"
    elif xmin_range > 1500000000:  # 15억 이상
        logger.warning(f"xmin 순환 경고: {table_name}")
        return "warning"
    
    return "stable"

def copy_table_data_with_xmin(self, ...):
    # xmin 안정성 검증
    stability = self.db_ops.validate_xmin_stability(source_table)
    
    if stability == "force_full_sync":
        logger.warning("xmin 순환 위험으로 전체 동기화 모드로 전환")
        return self.copy_table_data(..., sync_mode="full_sync")
```

#### 12.2 **복제 환경 문제**

##### 12.2.1 문제점
- **읽기 전용 복제본**: 소스와 타겟의 xmin 값이 다를 수 있음
- **지연된 복제**: 복제 지연으로 인한 xmin 값 불일치
- **장애 복구**: 복제본 장애 시 xmin 기반 증분 처리 실패

##### 12.2.2 해결 방안
```python
def check_replication_status(self, source_table: str):
    # 복제 상태 확인
    replication_lag_sql = """
        SELECT 
            CASE 
                WHEN pg_is_in_recovery() THEN 'replica'
                ELSE 'primary'
            END as db_role,
            pg_last_wal_receive_lsn() as receive_lsn,
            pg_last_wal_replay_lsn() as replay_lsn
    """
    
    # 복제본인 경우 xmin 기반 처리 비활성화
    if db_role == 'replica':
        logger.warning("복제본 환경에서 xmin 기반 처리 비활성화")
        return False
    
    return True
```

### 13. 운영 및 유지보수 문제

#### 13.1 **기존 DAG 태스크와의 충돌**

##### 13.1.1 문제점
- **태스크 중복**: 기존 증분 처리 태스크와 xmin 기반 태스크가 동시 실행
- **의존성 혼란**: DAG 의존성 그래프가 복잡해짐
- **모니터링 어려움**: 어떤 방식으로 처리되었는지 추적 어려움

##### 13.1.2 해결 방안
```python
# DAG 태스크 분리
def create_xmin_dag():
    # xmin 기반 처리를 위한 별도 DAG 생성
    with DAG('xmin_incremental_sync', ...) as dag:
        xmin_copy_task = PythonOperator(
            task_id='copy_with_xmin',
            python_callable=copy_data_with_xmin_incremental,
            ...
        )
        
        # 기존 DAG와 분리하여 독립적으로 실행

# 또는 조건부 실행
def conditional_copy_task(table_config: dict, **context):
    if table_config.get("sync_mode") == "xmin_incremental":
        return copy_data_with_xmin_incremental(table_config, **context)
    else:
        return copy_data_with_dynamic_sql(table_config, **context)
```

#### 13.2 **설정 관리 복잡성**

##### 13.2.1 문제점
- **설정 파일 증가**: xmin 기반 설정과 기존 설정이 혼재
- **환경별 설정**: 개발/스테이징/운영 환경별 설정 관리 복잡성
- **버전 관리**: 기존 설정과 xmin 설정의 버전 호환성

##### 13.2.2 해결 방안
```python
# 설정 파일 구조화
class XminTableConfig:
    """xmin 기반 테이블 설정 전용 클래스"""
    
    def __init__(self, source: str, target: str, primary_keys: list[str]):
        self.source = source
        self.target = target
        self.primary_keys = primary_keys
        self.sync_mode = "xmin_incremental"  # 고정값
        self.xmin_tracking = True
        self.fallback_to_timestamp = True  # xmin 실패 시 타임스탬프로 폴백

# 환경별 설정 분리
@classmethod
def get_xmin_table_configs(cls, environment: str = "production") -> List[Dict[str, Any]]:
    configs = {
        "development": [...],
        "staging": [...],
        "production": [...]
    }
    return configs.get(environment, configs["development"])
```

### 14. 권장 해결 전략

#### 14.1 **단계적 도입 전략**
1. **1단계**: 기존 코드와 독립적인 xmin 모듈 개발
2. **2단계**: 기존 DAG에 xmin 옵션 추가 (선택적 사용)
3. **3단계**: 점진적으로 xmin 기반 처리로 전환
4. **4단계**: 기존 증분 처리 로직 점진적 제거

#### 14.2 **호환성 보장 방안**
1. **기본값 유지**: 기존 설정의 기본값은 그대로 유지
2. **하위 호환성**: 기존 sync_mode가 계속 작동하도록 보장
3. **설정 검증**: xmin 설정과 기존 설정이 충돌하지 않도록 검증

#### 14.3 **모니터링 및 알림**
1. **xmin 상태 모니터링**: xmin 값 변화 및 안정성 지속 모니터링
2. **성능 메트릭**: xmin 기반 처리와 기존 처리의 성능 비교
3. **오류 추적**: xmin 관련 오류 발생 시 즉시 알림

이러한 문제점들을 사전에 파악하고 적절한 해결 방안을 마련함으로써, 기존 시스템의 안정성을 유지하면서 xmin 기능을 성공적으로 도입할 수 있습니다.