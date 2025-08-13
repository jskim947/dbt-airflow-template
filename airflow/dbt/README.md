# Postgres Data Copy DBT Project

이 dbt 프로젝트는 PostgreSQL 데이터 복사 파이프라인을 위한 데이터 변환 및 스냅샷을 담당합니다.

## 프로젝트 구조

```
dbt/
├── models/                 # 데이터 모델
│   ├── staging/           # 스테이징 모델
│   └── marts/             # 마트 모델
├── snapshots/             # 스냅샷 정의
├── macros/                # 재사용 가능한 매크로
├── tests/                 # 데이터 테스트
├── seeds/                 # 정적 데이터
├── analyses/              # 임시 분석
├── dbt_project.yml        # 프로젝트 설정
├── profiles.yml           # 프로필 설정
└── packages.yml           # 패키지 의존성
```

## 주요 모델

### 스테이징 모델
- `stg_infomax_stock_master`: 인포맥스 종목 마스터 스테이징 모델

### 스냅샷
- `인포맥스종목마스터_snapshot`: 종목 마스터 데이터의 변경 이력 추적

## 사용법

### 1. 의존성 설치
```bash
dbt deps
```

### 2. 프로젝트 파싱
```bash
dbt parse
```

### 3. 모델 실행
```bash
dbt run
```

### 4. 스냅샷 실행
```bash
dbt snapshot
```

### 5. 테스트 실행
```bash
dbt test
```

### 6. 문서 생성
```bash
dbt docs generate
dbt docs serve
```

## 환경 설정

프로젝트는 `profiles.yml`에서 정의된 PostgreSQL 연결을 사용합니다:

- **Host**: 10.150.2.150
- **Port**: 15432
- **Database**: airflow
- **Schema**: raw_data

## 데이터 소스

- `raw_data.인포맥스종목마스터`: 원본 종목 마스터 데이터

## 주의사항

1. 스냅샷 실행 전에 반드시 `dbt deps`와 `dbt parse`를 실행하세요.
2. 프로젝트 설정 변경 시 `dbt parse`로 파싱 오류를 확인하세요.
3. 데이터 무결성 검사를 위해 `dbt test`를 정기적으로 실행하세요.
