
# DBT-Airflow Template

PostgreSQL 데이터베이스 간 데이터 복사 및 변환을 위한 Airflow DAG와 DBT 통합 템플릿입니다.

## 🚀 프로젝트 개요

이 프로젝트는 다음과 같은 기능을 제공합니다:

- **데이터 복사**: PostgreSQL 소스에서 타겟으로 데이터 복사
- **데이터 변환**: DBT를 통한 데이터 변환 및 스냅샷 생성
- **자동화**: Airflow를 통한 스케줄링 및 워크플로우 관리
- **모니터링**: 데이터 파이프라인 실행 상태 추적

## 🏗️ 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Source DB     │    │     Airflow     │    │   Target DB     │
│  (PostgreSQL)   │───▶│      DAGs       │───▶│  (PostgreSQL)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │      DBT        │
                       │   Snapshots     │
                       │   Models        │
                       └─────────────────┘
```

## 📁 프로젝트 구조

```
dbt-airflow-template/
├── airflow/
│   ├── dags/                    # Airflow DAG 파일들
│   │   ├── common/             # 공통 유틸리티 모듈
│   │   ├── postgres_data_copy_dag.py          # 메인 데이터 복사 DAG
│   │   ├── postgres_data_copy_dag_refactored.py # 리팩토링된 데이터 복사 DAG
│   │   ├── dbt_processing_dag.py               # DBT 처리 DAG
│   │   └── main_orchestration_dag.py           # 메인 오케스트레이션 DAG
│   ├── dbt/                    # DBT 프로젝트
│   │   ├── models/             # DBT 모델
│   │   ├── snapshots/          # DBT 스냅샷
│   │   └── dbt_project.yml     # DBT 프로젝트 설정
│   └── plugins/                # Airflow 플러그인
├── docker/                     # Docker 설정
├── docs/                       # 프로젝트 문서
└── README.md                   # 이 파일
```

## 🛠️ 주요 기능

### 데이터 복사 엔진
- **스마트 스키마 감지**: 소스 테이블 스키마 자동 감지
- **데이터 타입 변환**: CSV 읽기/쓰기 시 자동 데이터 타입 변환
- **증분 동기화**: 변경된 데이터만 선택적 복사
- **배치 처리**: 대용량 데이터 처리 최적화

### DBT 통합
- **자동 스냅샷**: 데이터 변경사항 자동 추적
- **모델 실행**: 데이터 변환 파이프라인
- **테스트 실행**: 데이터 품질 검증

### 모니터링 및 로깅
- **실행 상태 추적**: 각 단계별 성공/실패 상태
- **성능 메트릭**: 실행 시간 및 처리된 레코드 수
- **에러 핸들링**: 상세한 에러 로그 및 복구 방안

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 저장소 클론
git clone <repository-url>
cd dbt-airflow-template

# 가상환경 생성 및 활성화
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# 또는
.venv\Scripts\activate     # Windows

# 의존성 설치
pip install -r requirements.txt
```

### 2. 데이터베이스 연결 설정

```bash
# 환경 변수 설정
cp .env.example .env
# .env 파일 편집하여 데이터베이스 연결 정보 입력
```

### 3. Airflow 실행

```bash
# Airflow 초기화
airflow db init

# 사용자 생성
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Airflow 웹서버 시작
airflow webserver --port 8080

# Airflow 스케줄러 시작 (새 터미널)
airflow scheduler
```

### 4. DAG 활성화

Airflow 웹 UI에서 다음 DAG들을 활성화:
- `postgres_multi_table_copy`: 메인 데이터 복사 DAG
- `dbt_processing`: DBT 처리 DAG

## 📊 사용 예시

### 기본 데이터 복사 설정

```python
table_config = {
    "source": "source_schema.table_name",
    "target": "target_schema.table_name",
    "primary_key": ["id"],
    "sync_mode": "incremental_sync",
    "incremental_field": "updated_at",
    "batch_size": 10000
}
```

### 증분 동기화 설정

```python
table_config = {
    "source": "m23.edi_690",
    "target": "raw_data.edi_690",
    "primary_key": ["eventcd", "eventid", "optionid"],
    "sync_mode": "incremental_sync",
    "incremental_field": "changed",
    "incremental_field_type": "timestamp"
}
```

## 🔧 설정 옵션

### 동기화 모드
- **`full_sync`**: 전체 데이터 복사
- **`incremental_sync`**: 변경된 데이터만 복사

### 증분 필드 타입
- **`timestamp`**: 타임스탬프 기반 증분
- **`yyyymmdd`**: 날짜 기반 증분
- **`date`**: 날짜 기반 증분
- **`datetime`**: 날짜시간 기반 증분

### 배치 크기
- 기본값: 10,000
- 메모리 사용량과 성능을 고려하여 조정

## 📈 모니터링

### Airflow UI
- DAG 실행 상태 확인
- 태스크별 실행 시간 및 로그
- 실패한 태스크 재실행

### 로그 분석
- 각 단계별 상세 로그
- 데이터 처리 통계
- 에러 및 경고 메시지

## 🐛 문제 해결

### 일반적인 문제들

1. **데이터베이스 연결 실패**
   - 연결 정보 확인
   - 방화벽 설정 확인
   - 데이터베이스 서비스 상태 확인

2. **스키마 불일치**
   - 소스/타겟 테이블 구조 확인
   - 컬럼 타입 매핑 확인

3. **메모리 부족**
   - 배치 크기 줄이기
   - JVM 힙 크기 조정

### 로그 확인

```bash
# Airflow 로그 확인
airflow tasks logs <dag_id> <task_id> <execution_date>

# DBT 로그 확인
dbt run --log-level debug
```

## 🤝 기여하기

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

## 📞 지원

문제가 있거나 질문이 있으시면:
- [Issues](../../issues) 페이지에 이슈 생성
- 프로젝트 문서 참조
- 코드 리뷰 요청

---

**참고**: 이 프로젝트는 PostgreSQL과 Airflow를 사용하는 데이터 엔지니어링 워크플로우를 위한 템플릿입니다. 프로덕션 환경에서 사용하기 전에 보안 및 성능 요구사항을 충족하는지 확인하세요.

## FastAPI 데이터베이스 API

이 프로젝트는 PostgreSQL 데이터베이스 조회를 위한 FastAPI를 포함하고 있습니다.

### 실행 방법

#### 1. Docker Compose로 실행 (권장)
```bash
cd docker
docker-compose up -d api
```

#### 2. 직접 실행
```bash
# 의존성 설치
pip install fastapi uvicorn[standard] asyncpg

# API 실행
python run_api.py
```

### API 엔드포인트

#### 기본 정보
- **루트**: `GET /` - API 정보 및 사용 가능한 엔드포인트
- **헬스체크**: `GET /health` - 서버 상태 확인
- **API 문서**: `GET /docs` - Swagger UI
- **ReDoc**: `GET /redoc` - ReDoc 문서

#### 데이터베이스 조회
- **스키마 목록**: `GET /api/schemas` - 모든 스키마 조회
- **테이블 목록**: `GET /api/tables` - 기본 스키마의 테이블 목록
- **스키마별 테이블**: `GET /api/schemas/{schema}/tables` - 특정 스키마의 테이블 목록
- **테이블 스키마**: `GET /api/schema/{table_name}` - 테이블 스키마 정보
- **데이터 조회**: `GET /api/query/{table_name}` - 테이블 데이터 조회 (페이지네이션, 검색, 정렬 지원)
- **데이터 스트리밍**: `GET /api/stream/{table_name}` - 대용량 데이터 스트리밍

#### 쿼리 파라미터
- `page`: 페이지 번호 (기본값: 1)
- `size`: 페이지 크기 (기본값: 100, 최대: 1000)
- `sort_by`: 정렬 컬럼
- `order`: 정렬 순서 (ASC/DESC)
- `search`: 검색어
- `search_column`: 검색할 컬럼명
- `limit`: 스트리밍 시 최대 행 수 (최대: 10000)

### 사용 예시

#### 1. 스키마 목록 조회
```bash
curl http://localhost:8004/api/schemas
```

#### 2. 특정 스키마의 테이블 목록
```bash
curl http://localhost:8004/api/schemas/public/tables
```

#### 3. 테이블 데이터 조회 (페이지네이션)
```bash
curl "http://localhost:8004/api/query/users?page=1&size=50"
```

#### 4. 검색 및 정렬
```bash
curl "http://localhost:8004/api/query/users?search=john&search_column=name&sort_by=created_at&order=DESC"
```

#### 5. 데이터 스트리밍
```bash
curl "http://localhost:8004/api/stream/users?limit=5000" > users_data.jsonl
```

### 환경변수 설정

`.env` 파일에서 다음 설정을 확인하세요:

```bash
# FastAPI 설정
API_HOST=0.0.0.0
API_PORT=8004

# PostgreSQL 연결 설정
POSTGRES_HOST=10.150.2.150
POSTGRES_PORT=15432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```
