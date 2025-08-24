# PostgreSQL Provider 설정 및 실행 가이드

## 개요
이 문서는 Apache Airflow PostgreSQL Provider를 사용하여 다른 PostgreSQL 데이터베이스에서 데이터를 복사하는 방법을 설명합니다.

## 사전 요구사항

### 1. PostgreSQL Provider 설치 확인
```bash
# 이미 설치됨 (버전 6.2.3)
pip show apache-airflow-providers-postgres
```

### 2. 필요한 Python 패키지
- `psycopg2-binary` (이미 설치됨)
- `asyncpg` (비동기 지원, 이미 설치됨)

## 연결 설정

### 1. Airflow UI에서 연결 설정

#### 소스 PostgreSQL 연결
1. **Admin** → **Connections** 메뉴로 이동
2. **+** 버튼을 클릭하여 새 연결 추가
3. 다음 정보 입력:
   - **Connection Id**: `source_postgres_conn`
   - **Connection Type**: `Postgres`
   - **Host**: 소스 PostgreSQL 서버 호스트
   - **Schema**: 소스 데이터베이스 이름
   - **Login**: 소스 데이터베이스 사용자명
   - **Password**: 소스 데이터베이스 비밀번호
   - **Port**: 5432 (기본값)

#### 타겟 PostgreSQL 연결
1. 동일한 방법으로 새 연결 추가
2. 다음 정보 입력:
   - **Connection Id**: `target_postgres_conn`
   - **Connection Type**: `Postgres`
   - **Host**: 타겟 PostgreSQL 서버 호스트
   - **Schema**: 타겟 데이터베이스 이름
   - **Login**: 타겟 데이터베이스 사용자명
   - **Password**: 타겟 데이터베이스 비밀번호
   - **Port**: 5432 (기본값)

### 2. 환경 변수로 연결 설정 (선택사항)
```bash
# 소스 연결
export AIRFLOW_CONN_SOURCE_POSTGRES_CONN='postgresql://username:password@hostname:5432/database_name'

# 타겟 연결
export AIRFLOW_CONN_TARGET_POSTGRES_CONN='postgresql://username:password@hostname:5432/database_name'
```

## 테이블 준비

### 1. 소스 테이블 확인
```sql
-- 소스 데이터베이스에서 실행
SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'your_source_table'
ORDER BY ordinal_position;
```

### 2. 타겟 테이블 생성
```sql
-- 타겟 데이터베이스에서 실행
-- 소스 테이블과 동일한 스키마로 테이블 생성
CREATE TABLE target_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## DAG 실행

### 1. DAG 파일 확인
생성된 DAG 파일들이 `airflow/dags/` 디렉토리에 있는지 확인:
- `postgres_data_copy_dag.py` (고급 기능)
- `simple_postgres_copy_dag.py` (기본 기능)

### 2. DAG 활성화
1. Airflow UI에서 **DAGs** 메뉴로 이동
2. `postgres_data_copy` 또는 `simple_postgres_copy` DAG 찾기
3. **Toggle** 버튼을 클릭하여 DAG 활성화

### 3. 수동 실행
1. DAG 페이지에서 **Trigger DAG** 버튼 클릭
2. 실행 파라미터 설정 (필요시)
3. **Trigger** 버튼 클릭

## 모니터링 및 디버깅

### 1. DAG 실행 상태 확인
- **Graph View**: 전체 워크플로우 시각화
- **Tree View**: 실행 히스토리 및 상태
- **Gantt Chart**: 작업별 실행 시간 분석

### 2. 로그 확인
각 작업의 **Log** 버튼을 클릭하여 상세 로그 확인

### 3. XCom 데이터 확인
작업 간 전달되는 데이터를 **XCom** 탭에서 확인

## 일반적인 문제 해결

### 1. 연결 오류
```
Error: connection to server at "host" (IP), port 5432 failed
```
**해결 방법**:
- 연결 정보 확인 (호스트, 포트, 사용자명, 비밀번호)
- 방화벽 설정 확인
- PostgreSQL 서버 실행 상태 확인

### 2. 권한 오류
```
Error: permission denied for table "table_name"
```
**해결 방법**:
- 데이터베이스 사용자에게 적절한 권한 부여
```sql
GRANT SELECT ON source_table TO username;
GRANT INSERT, UPDATE, DELETE ON target_table TO username;
```

### 3. 메모리 부족 오류
```
Error: out of memory
```
**해결 방법**:
- `BATCH_SIZE` 값을 줄이기 (기본값: 10000)
- DAG의 `max_active_runs` 값 조정

## 성능 최적화

### 1. 배치 크기 조정
```python
# DAG 파일에서 BATCH_SIZE 조정
BATCH_SIZE = 5000  # 메모리 사용량에 따라 조정
```

### 2. 연결 풀 설정
```python
hook = PostgresHook(
    postgres_conn_id='my_conn',
    pool_size=20,  # 연결 풀 크기
    max_overflow=30  # 최대 오버플로우 연결
)
```

### 3. 인덱스 최적화
```sql
-- 자주 조회되는 컬럼에 인덱스 생성
CREATE INDEX idx_table_column ON table_name(column_name);

-- 복합 인덱스 (여러 컬럼 조회시)
CREATE INDEX idx_table_multi ON table_name(col1, col2, col3);
```

## 보안 고려사항

### 1. SSL 연결 사용
```python
hook = PostgresHook(
    postgres_conn_id='my_conn',
    sslmode='require'  # SSL 강제 사용
)
```

### 2. 연결 문자열 암호화
- 민감한 정보는 환경 변수나 Airflow Variables에 저장
- 연결 정보를 코드에 하드코딩하지 않기

### 3. 최소 권한 원칙
- 데이터베이스 사용자에게 필요한 최소한의 권한만 부여
- 읽기 전용 연결과 쓰기 연결을 분리

## 예제 시나리오

### 시나리오 1: 일일 데이터 동기화
- **소스**: 운영 데이터베이스
- **타겟**: 분석용 데이터베이스
- **스케줄**: 매일 새벽 2시
- **전략**: 전체 테이블 교체

### 시나리오 2: 증분 데이터 복사
- **소스**: 트랜잭션 데이터베이스
- **타겟**: 데이터 웨어하우스
- **스케줄**: 매시간
- **전략**: 변경된 데이터만 복사

### 시나리오 3: 백업 및 복구
- **소스**: 주 데이터베이스
- **타겟**: 백업 데이터베이스
- **스케줄**: 매주 일요일
- **전략**: 전체 백업

## 추가 리소스

### 1. 공식 문서
- [PostgreSQL Provider 공식 문서](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/index.html)
- [Airflow 공식 문서](https://airflow.apache.org/docs/)

### 2. 커뮤니티
- [Airflow Slack](https://apache-airflow.slack.com/)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/apache-airflow)

### 3. 예제 코드
- [GitHub 저장소](https://github.com/apache/airflow/tree/providers-postgres/6.2.3/providers/postgres)
- [시스템 테스트 예제](https://github.com/apache/airflow/tree/providers-postgres/6.2.3/providers/postgres/tests/system/postgres)
