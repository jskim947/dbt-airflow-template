# Apache Airflow PostgreSQL Provider 사용법 가이드

## 개요
`apache-airflow-providers-postgres`는 Airflow에서 PostgreSQL 데이터베이스와 상호작용할 수 있게 해주는 공식 Provider입니다.

## 설치된 버전
- **Provider 버전**: 6.2.3
- **Airflow 버전**: 2.10.3+
- **Python 버전**: 3.12+

## 주요 기능

### 1. PostgresHook
PostgreSQL 데이터베이스에 연결하고 쿼리를 실행하는 핵심 클래스입니다.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 기본 사용법
hook = PostgresHook(postgres_conn_id='my_postgres_conn')
conn = hook.get_conn()
cursor = conn.cursor()

# 쿼리 실행
cursor.execute("SELECT * FROM users")
results = cursor.fetchall()

# 연결 종료
cursor.close()
conn.close()
```

### 2. SQLExecuteQueryOperator
SQL 쿼리를 실행하는 표준화된 Operator입니다.

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 기본 쿼리 실행
run_query = SQLExecuteQueryOperator(
    task_id='run_sql',
    conn_id='my_postgres_conn',
    sql='SELECT * FROM users WHERE created_date > %(date)s',
    parameters={'date': '2024-01-01'}
)

# INSERT 쿼리
insert_data = SQLExecuteQueryOperator(
    task_id='insert_data',
    conn_id='my_postgres_conn',
    sql='''
        INSERT INTO target_table (id, name, created_at)
        VALUES (%(id)s, %(name)s, %(created_at)s)
    ''',
    parameters={
        'id': 1,
        'name': 'John Doe',
        'created_at': '2024-01-01'
    }
)
```

### 3. 비동기 지원
`asyncpg` 라이브러리를 통한 고성능 비동기 연결을 지원합니다.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 비동기 연결
hook = PostgresHook(
    postgres_conn_id='my_postgres_conn',
    client_encoding='utf8'
)

# 비동기 쿼리 실행
async def async_query():
    async with hook.get_async_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM large_table")
            results = await cur.fetchall()
            return results
```

## 연결 설정

### Airflow UI에서 연결 설정
1. **Admin** → **Connections** 메뉴로 이동
2. **+** 버튼을 클릭하여 새 연결 추가
3. **Connection Id**: `my_postgres_conn`
4. **Connection Type**: `Postgres`
5. **Host**: PostgreSQL 서버 호스트
6. **Schema**: 데이터베이스 이름
7. **Login**: 사용자명
8. **Password**: 비밀번호
9. **Port**: 5432 (기본값)

### 환경 변수로 연결 설정
```bash
export AIRFLOW_CONN_MY_POSTGRES_CONN='postgresql://user:password@host:5432/dbname'
```

## 고급 기능

### 1. 연결 풀링
```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(
    postgres_conn_id='my_postgres_conn',
    pool_size=10,  # 연결 풀 크기
    max_overflow=20  # 최대 오버플로우 연결 수
)
```

### 2. 데이터베이스 메시지 로깅
```python
hook = PostgresHook(
    postgres_conn_id='my_postgres_conn',
    log_sql=True  # SQL 로깅 활성화
)
```

### 3. 세미콜론 자동 제거
```python
hook = PostgresHook(
    postgres_conn_id='my_postgres_conn',
    autocommit=True,  # 자동 커밋
    parameters=None
)
```

## 에러 처리

### 연결 재시도
```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def execute_with_retry(sql, parameters=None):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    return hook.run(sql, parameters=parameters)
```

### 연결 상태 확인
```python
def check_connection():
    try:
        hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
```

## 성능 최적화

### 1. 배치 처리
```python
def batch_insert(data_list, batch_size=1000):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')

    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch[0]))

        sql = f"INSERT INTO table_name VALUES ({placeholders})"
        hook.run(sql, parameters=batch)
```

### 2. COPY 명령어 사용
```python
def fast_copy_from_file(file_path, table_name):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    with open(file_path, 'r') as f:
        cursor.copy_from(f, table_name, sep=',')

    conn.commit()
    cursor.close()
    conn.close()
```

## 모니터링 및 로깅

### 1. 쿼리 실행 시간 측정
```python
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook

def timed_query(sql, parameters=None):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')

    start_time = time.time()
    result = hook.run(sql, parameters=parameters)
    execution_time = time.time() - start_time

    print(f"Query executed in {execution_time:.2f} seconds")
    return result
```

### 2. 연결 풀 상태 모니터링
```python
def get_pool_status():
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    pool = hook.get_pool()

    print(f"Pool size: {pool.size()}")
    print(f"Checked out connections: {pool.checkedout()}")
    print(f"Available connections: {pool.checkedin()}")
```

## 보안 고려사항

### 1. SSL 연결
```python
hook = PostgresHook(
    postgres_conn_id='my_postgres_conn',
    sslmode='require',  # SSL 강제 사용
    sslcert='/path/to/client-cert.pem',
    sslkey='/path/to/client-key.pem'
)
```

### 2. 연결 문자열 암호화
```python
from cryptography.fernet import Fernet

# 암호화된 연결 문자열 사용
encrypted_conn = "encrypted_connection_string"
hook = PostgresHook(postgres_conn_id=encrypted_conn)
```

## 참고 자료
- [공식 문서](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/index.html)
- [GitHub 저장소](https://github.com/apache/airflow/tree/providers-postgres/6.2.3/providers/postgres)
- [시스템 테스트 예제](https://github.com/apache/airflow/tree/providers-postgres/6.2.3/providers/postgres/tests/system/postgres)
