### 서버 간 PostgreSQL 데이터 복사 성능 개선 가이드

본 문서는 `postgres_multi_table_copy_refactored` DAG과 공통 엔진(`common/DataCopyEngine`)을 사용하는 서버 간(Postgres→Postgres) 데이터 복사 작업의 처리 시간을 단축하기 위한 실용적인 개선 방안을 정리합니다. 대규모 테이블, 잦은 동기화, 네트워크 왕복 비용이 큰 환경을 우선 고려했습니다.

---

## 현재 방식 요약 및 잠재 병목

- **현재 추출/적재 방식**: Airflow 태스크에서 `psql`의 `\copy`를 사용해 소스에서 CSV로 내보낸 후, 타겟으로 다시 `\copy`로 적재하고 이후 MERGE/검증 수행

```879:946:airflow/dags/postgres_data_copy_dag.py
        # 1단계: 소스에서 데이터 추출하여 CSV로 내보내기 (psql \copy 사용)
        export_cmd = f"psql ... -c \"\\copy ({select_sql}) TO '{csv_filename}' WITH CSV HEADER\""
        ...
        # 2단계: CSV에서 타겟으로 가져오기 (psql \copy 사용)
        import_cmd = f"psql ... -c \"\\copy {temp_table_full} FROM '{csv_filename}' WITH CSV HEADER\""
```

- **병목 지점**
  - **디스크 I/O**: 중간 CSV 파일 생성/읽기 오버헤드
  - **네트워크 I/O**: 원격 DB ↔ Airflow 워커 ↔ 타겟 DB의 왕복 및 텍스트(CSV) 포맷 오버헤드
  - **단일 스레드 처리**: 테이블 단위 순차 처리, 단일 스트림 적재
  - **MERGE 비용**: 타겟 기본키/조인 키 인덱스 부재 또는 비효율, 대량 DELETE/UPSERT 시 WAL/락 경합

---

## 즉시 적용 가능한 빠른 개선(Quick Wins)

- **CSV → 스트리밍 파이프 전환(중간 파일 제거)**
  - 중간 파일 없이 STDOUT→STDIN 파이프라인으로 전송해 디스크 I/O를 제거합니다.
  - 예시:

```bash
PGPASSWORD=$SRC_PASS psql -h $SRC_HOST -p $SRC_PORT -U $SRC_USER -d $SRC_DB \
  -c "\\copy (SELECT ...) TO STDOUT WITH CSV HEADER" | \
PGPASSWORD=$DST_PASS psql -h $DST_HOST -p $DST_PORT -U $DST_USER -d $DST_DB \
  -c "\\copy target_schema.target_table FROM STDIN WITH CSV HEADER"
```

  - 구현 방법: `DataCopyEngine`에서 `subprocess` 기반 `psql` 호출을 파이프라인으로 결합하거나, 파이썬 드라이버의 `copy_expert`(STDOUT/STDIN)로 교체

- **바이너리 COPY 사용 검토**
  - 텍스트 CSV 대비 CPU/파싱 오버헤드가 낮습니다. 가능 시 `COPY ... WITH (FORMAT binary)`로 전환

- **세션 파라미터 튜닝(적재 세션 한정)**
  - 타겟 세션에서 다음을 설정하여 WAL/동기화 비용을 절감합니다.
    - `SET synchronous_commit = off;`
    - `SET statement_timeout = '0';`(태스크 레벨 타임아웃으로 보호)
    - `SET work_mem = '128MB';`(조인/정렬 상황에 맞춰 조정)

- **스테이징 테이블 UNLOGGED 사용**
  - 임시/스테이징 테이블은 `UNLOGGED`로 생성하여 WAL을 줄입니다. 병행 복구 요구가 없다면 효과적입니다.

- **인덱스 점검**
  - MERGE/UPSERT 키(기본키, 조인 키)에 적절한 인덱스가 있는지 확인하고, 필요 시 스테이징 로드 후 `CREATE INDEX CONCURRENTLY`로 구축

---

## 전체 테이블 스캔 최적화 방안

- **소스 DB 병렬 쿼리 최적화**
  - Postgres 9.6+에서 `COPY (SELECT ...)`가 병렬 실행되도록 GUC 조정
  - Airflow에서 `SQLExecuteQueryOperator`의 `hook_params={"options": "-c max_parallel_workers_per_gather=4"}` 등으로 전달
  - **주의**: 클러스터 파라미터는 사전에 DB 관리자가 설정해야 함

- **읽기 복제본 활용**
  - 본선 부하 회피를 위해 읽기 복제본에서 스캔 수행
  - 소스 권한: SELECT만으로 가능

- **테이블/스토리지 상태 최적화**
  - `VACUUM`, `pg_repack` 등으로 bloat 축소하여 순차 스캔 속도 향상
  - **주의**: DDL 권한 필요하므로 읽기 복제본에서 수행 권장

---

## 엔진/구성 측면 개선(현 템플릿에 바로 적용 가능)

- **청크 모드 활용 및 튜닝**
  - 이미 제공되는 `chunk_mode`/`enable_checkpoint`를 활성화하고 `batch_size`를 테이블별로 조정합니다.
  - 설정 위치: `airflow/dags/common/settings.py`의 `DAGSettings.get_table_configs()` 또는 변수 DAG(`variable_setup_dag.py`)
  - 권장: 대용량 테이블 `batch_size` 20k~100k, 실패 재시도 `max_retries` 3~5

- **테이블 병렬화(멀티-테이블 동시 실행)**
  - 현재는 순차 처리 경향이 있으므로 동시 실행 가능한 테이블을 **TaskGroup/동적 매핑**으로 병렬화하고, Airflow `pool`로 DB 부하를 제어합니다.
  - DAG 레벨: `max_active_runs`와 워커 `worker_concurrency`를 점진적으로 상향

- **머지 전략 단순화**
  - 전체 동기화에서는 `TRUNCATE + COPY`가 가능한 경우 MERGE보다 빠릅니다.
  - 증분 동기화는 `INSERT ... ON CONFLICT DO UPDATE`로 통일하여 단계/락을 최소화

- **타겟 세션 최소 로깅**
  - 스테이징 적재 시 `SET synchronous_commit = off;`를 세션 단위로 적용하고, 적재 완료 후 `ANALYZE target_table` 수행

- **통계/자동청소 관리**
  - 대량 적재 직후 `ANALYZE`로 플래너 통계 최신화
  - 장시간 적재 시 autovacuum 경합이 있으면 시간대 조정 또는 임시 비활성화(서비스 영향 검토 필수)

---

## 중기 개선(구현 1~2스프린트)

- **드라이버 레벨 스트리밍(파이썬 내부로 전환)**
  - `psql` 서브프로세스 대신 드라이버(`psycopg`)의 `copy_expert`를 이용해 메모리-제한 스트리밍 구현
  - 장점: 파이프라인 제어, 재시도/체크포인트/메트릭 수집을 코드 내에서 일관되게 처리

- **FDW(postgres_fdw) 기반 서버-사이드 복사**
  - 타겟 DB에 소스 DB를 FDW로 마운트하고, `INSERT INTO target SELECT ... FROM foreign_table` 또는 `CREATE MATERIALIZED VIEW` 갱신으로 수행
  - 장점: 서버-사이드 실행으로 네트워크 왕복 감소, 푸시다운 최적화

- **파티셔닝 도입**
  - 타겟을 날짜/키 범위 파티션으로 설계하여 증분 병합 및 삭제 비용 절감
  - 파티션 단위 `TRUNCATE/ATTACH`로 풀리프레시 시간을 단축

---

## 적용 가능성 및 권한/부하 검토

- **범용성(다른 서버 간 복사 적용 가능 여부)**: 본 문서의 개선안은 모두 Postgres→Postgres 일반 시나리오에 적용 가능합니다. Airflow 외 환경에서도 동일 원칙으로 적용할 수 있습니다.

- **스트리밍 파이프/드라이버 스트리밍**
  - 적용성: 서버 간 네트워크 접근 가능하면 적용 가능
  - 권한: 소스 `SELECT`, 타겟 `INSERT`(및 스키마 생성 권한이 필요한 경우에만 추가 필요)
  - 소스 부하: `COPY (SELECT ...)` 실행 시 해당 `SELECT`의 실행 계획에 따름. 전체 스캔이면 I/O 부하 큼. 증분/범위 필터에 인덱스가 있으면 부하가 크게 감소

- **바이너리 COPY**
  - 적용성: 동일. 전송/파싱 오버헤드 감소
  - 권한: 소스 `SELECT`, 타겟 `INSERT`
  - 소스 부하: 텍스트 CSV 대비 CPU 오버헤드 감소. 읽기 I/O는 쿼리 범위에 비례

- **세션 파라미터 튜닝(타겟 전용)**
  - 적용성: 어디서나 적용. Airflow에서는 `SQLExecuteQueryOperator`의 `hook_params`로 세션 옵션 제어 가능
  - 권한: 타겟 세션 파라미터 변경 권한
  - 소스 부하: 영향 없음(타겟 전용)

- **UNLOGGED 스테이징**
  - 적용성: 타겟에서 스테이징 테이블 생성 가능 시 적용
  - 권한: 타겟 `CREATE TABLE`
  - 소스 부하: 영향 없음(타겟 전용)

- **인덱스 점검/지연 생성(CONCURRENTLY)**
  - 적용성: 타겟에서 적용
  - 권한: 타겟 인덱스 생성 권한
  - 소스 부하: 없음

- **청크 모드(batch/chunk 기반)**
  - 적용성: 범용. 큰 테이블에 권장
  - 권한: 소스 `SELECT`(필수), 타겟 `INSERT`
  - 소스 부하: 청크별 `SELECT`가 반복 실행되므로 총 읽기량은 동일하지만 피크 부하를 분산. 청크 조건(예: PK 범위, 날짜 범위)은 반드시 인덱스로 지원해 랜덤/전테이블 스캔을 피함

- **테이블 병렬화**
  - 적용성: Airflow 병렬 실행/풀로 제어 가능
  - 권한: 동일
  - 소스 부하: 동시 `SELECT`가 늘어 부하 증가. Airflow `pool`과 동시성으로 상한을 두고, 중요 OLTP 시간대를 회피하는 스케줄 권장

- **머지 전략 단순화(풀리프레시 시 TRUNCATE+COPY, 증분은 ON CONFLICT)**
  - 적용성: 범용
  - 권한: 타겟 `TRUNCATE/INSERT`
  - 소스 부하: 없음(쿼리 자체는 동일 `SELECT` 기준)

- **FDW(postgres_fdw) 기반 서버-사이드 복사**
  - 적용성: 네트워크/권한 요건 충족 시 범용. 필터/프로젝션 푸시다운으로 네트워크 효율적
  - 권한: 타겟에서 `CREATE EXTENSION postgres_fdw`, `CREATE SERVER/USER MAPPING` 권한 필요. 소스는 연결 계정의 `SELECT` 권한
  - 소스 부하: 해당 쿼리를 소스에서 실행하므로 인덱스가 적절하면 부하는 제한적. 병렬화 수준은 조심스럽게 증가

- **파티셔닝(타겟)**
  - 적용성: 타겟 설계 변경 가능 시 권장
  - 권한: 타겟 DDL 권한
  - 소스 부하: 없음

- **전체 테이블 스캔 최적화**
  - 적용성: Postgres 9.6+에서 병렬 쿼리 지원 시
  - 권한: 소스 `SELECT`(필수), 클러스터 GUC 설정은 DB 관리자 권한 필요
  - 소스 부하: 병렬 워커 증가로 CPU/메모리 사용량 증가, 벽시계 시간은 단축. 읽기 복제본 활용 권장

권장 운영 팁: 소스 부하를 낮추기 위해 증분 필터(업데이트 타임스탬프, 증가 PK, xmin 등)를 최대한 활용하고, 해당 컬럼에 인덱스를 보장하세요. 병렬화는 Airflow `pool`로 상한을 두고, 세션 레벨 타임아웃(`statement_timeout`)과 잠금 타임아웃(`lock_timeout`)을 설정해 운영 안정성을 확보합니다.

---

## 구현 계획

### Phase 1: 즉시 적용 (1-2주)
1. **세션 파라미터 최적화**
   - 타겟 세션에서 `synchronous_commit=off` 적용
   - `SQLExecuteQueryOperator`의 `hook_params` 활용
   - 적재 후 `ANALYZE` 자동화

2. **UNLOGGED 스테이징 테이블**
   - `DataCopyEngine`에서 임시 테이블 생성 시 `UNLOGGED` 옵션 추가
   - 기존 `create_temp_table` 함수 수정

3. **인덱스 최적화**
   - MERGE 키에 대한 인덱스 존재 여부 점검
   - 필요 시 `CREATE INDEX CONCURRENTLY` 자동화

### Phase 2: 엔진 개선 (2-4주)
1. **스트리밍 파이프 구현**
   - `psql` 파이프라인 방식으로 중간 CSV 파일 제거
   - `subprocess.Popen`을 사용한 STDOUT→STDIN 연결

2. **청크 모드 활성화**
   - `settings.py`에서 `chunk_mode=True` 기본값 설정
   - 테이블별 `batch_size` 최적화

3. **병렬화 구현**
   - `TaskGroup`을 사용한 테이블 동시 실행
   - Airflow `pool` 설정으로 DB 부하 제어

### Phase 3: 고급 최적화 (4-8주)
1. **바이너리 COPY 전환**
   - `FORMAT binary` 옵션 적용
   - 데이터 타입 호환성 검증

2. **FDW 기반 복사**
   - `postgres_fdw` 확장 설치 및 설정
   - 서버-사이드 복사 로직 구현

3. **파티셔닝 도입**
   - 대용량 테이블 파티션 설계
   - 파티션 단위 처리 로직 구현

---

## 적용 우선순위 제안(권장 순서)

1. 스테이징을 UNLOGGED로 전환, 세션 파라미터(`synchronous_commit=off`) 적용, 인덱스/머지 전략 점검
2. 중간 CSV 제거(스트리밍 파이프 또는 드라이버 `copy_expert`)
3. 테이블 병렬화(풀/동시성 튜닝)와 청크 파라미터(batch_size) 조정
4. 파티셔닝 도입(대규모 테이블), FDW 기반 서버-사이드 복사 검토

---

## 체크리스트

### 기본 설정
- [ ] `chunk_mode=True`, `enable_checkpoint=True` 활성화
- [ ] 테이블별 `batch_size` 최적화 (대용량: 20k~100k, 중간: 5k~20k)
- [ ] `max_retries` 설정 (대용량: 5, 일반: 3)

### 세션 최적화
- [ ] 타겟 세션에서 `synchronous_commit=off` 적용
- [ ] `statement_timeout=0` 설정 (태스크 타임아웃으로 보호)
- [ ] 적재 후 `ANALYZE` 자동화

### 스테이징 테이블
- [ ] 임시 테이블 `UNLOGGED` 생성
- [ ] 필요 시 인덱스는 적재 후 `CONCURRENTLY` 생성
- [ ] 임시 테이블 자동 정리 확인

### 네트워크/디스크 최적화
- [ ] 스트리밍 파이프 구성 (중간 파일 제거)
- [ ] 임시 파일 사용 시 빠른 디스크 지정 (`/dev/shm` 등)
- [ ] 바이너리 포맷 적용 검토

### 병렬화 및 부하 제어
- [ ] 동시 실행 테이블 선정
- [ ] Airflow `pool` 설정으로 DB 부하 제어
- [ ] 중요 OLTP 시간대 회피 스케줄

### 머지 전략
- [ ] 풀리프레시 시 `TRUNCATE + COPY` 적용
- [ ] 증분 동기화는 `ON CONFLICT DO UPDATE` 통일
- [ ] MERGE 키 인덱스 최적화

### 모니터링 및 관측성
- [ ] 단계별 처리량/시간 메트릭 수집
- [ ] 기존 `MonitoringManager` 활용
- [ ] 성능 임계값 알림 설정

---

## 참고 소스 경로

- DAG: `airflow/dags/postgres_multi_table_copy_refactored.py`
- 엔진: `airflow/dags/common/data_copy_engine.py`
- 기본 설정: `airflow/dags/common/settings.py` (`get_table_configs`)
- 변수 세팅 DAG: `airflow/dags/variable_setup_dag.py`

필요 시 위 파일에서 설정을 조정하고, 스트리밍/FDW 전환 등 구조 변경은 별도 브랜치에서 단계적으로 적용하시길 권장합니다.

