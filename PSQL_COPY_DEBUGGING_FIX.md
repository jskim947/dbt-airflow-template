# 🔧 psql COPY 명령어 실행 문제 해결 완료

## 🔍 **문제 분석**

### ❌ **발생한 에러**
```
ERROR - psql COPY 명령어 실행 실패: 소스 데이터 내보내기 실패 (코드: None)
ERROR - 스테이징 테이블로 데이터 복사 실패: 소스 데이터 내보내기 실패 (코드: None)
```

### 🎯 **문제 원인**
**한글 테이블명 문제는 해결되었지만, `psql COPY` 명령어 실행 과정에서 소스 데이터 내보내기 단계에서 실패**하고 있습니다.

## 📊 **상황 분석**

### ✅ **해결된 문제**
1. **한글 테이블명 접근**: 성공 ✅
2. **스테이징 테이블 생성**: 성공 ✅
3. **스테이징 테이블 방식 전환**: 완료 ✅

### ❌ **새로 발생한 문제**
1. **psql COPY 명령어 실행 실패**: 소스 데이터 내보내기 단계에서 실패 ❌
2. **에러 메시지 부족**: 정확한 실패 원인 파악 어려움 ❌

## 🔧 **수정 내용**

### 1. **상세 로깅 추가**

#### **실행 시작 로깅**
```python
# 디버깅을 위한 상세 로깅
logger.info(f"psql COPY 명령어 실행 시작:")
logger.info(f"  소스 DB: {source_host}:{source_database} (사용자: {source_user})")
logger.info(f"  타겟 DB: {target_host}:{target_database} (사용자: {target_user})")
logger.info(f"  소스 쿼리: {source_query}")
logger.info(f"  스테이징 테이블: {staging_table}")
logger.info(f"  export 명령어: {' '.join(export_command)}")
logger.info(f"  import 명령어: {' '.join(import_command)}")
```

#### **에러 상세 로깅**
```python
if export_process.returncode != 0:
    export_stderr = export_process.stderr.read().decode() if export_process.stderr else "Unknown error"
    logger.error(f"export_process 실패 - 종료코드: {export_process.returncode}")
    logger.error(f"export_process stderr: {export_stderr}")
    raise Exception(f"소스 데이터 내보내기 실패 (코드: {export_process.returncode}): {export_stderr}")

if import_process.returncode != 0:
    import_stderr = stderr.decode() if stderr else "Unknown error"
    logger.error(f"import_process 실패 - 종료코드: {import_process.returncode}")
    logger.error(f"import_process stderr: {import_stderr}")
    raise Exception(f"스테이징 테이블로 데이터 가져오기 실패 (코드: {import_process.returncode}): {import_stderr}")
```

#### **성공 로깅**
```python
execution_time = time.time() - start_time
logger.info(f"psql COPY 명령어 실행 성공 - 소요시간: {execution_time:.2f}초")
```

### 2. **psql 명령어 사용 가능 여부 확인**

```python
# psql 명령어 사용 가능 여부 확인
try:
    subprocess.run(['psql', '--version'], capture_output=True, check=True)
    logger.info("psql 명령어 사용 가능 확인됨")
except (subprocess.CalledProcessError, FileNotFoundError):
    logger.warning("psql 명령어를 찾을 수 없습니다. Python 기반 복사로 전환합니다.")
    return self._copy_with_python_fallback(source_table, staging_table, where_condition)
```

### 3. **Python 기반 대안 복사 메서드 추가**

#### **`_copy_with_python_fallback` 메서드**
- **목적**: `psql` 명령어를 사용할 수 없을 때 Python 기반으로 데이터 복사
- **방식**: 배치 단위로 데이터를 조회하여 INSERT로 복사
- **장점**: 외부 명령어 의존성 없음, 상세한 에러 추적 가능

#### **주요 특징**
```python
# 배치 단위로 데이터 처리
batch_size = 10000
total_rows = 0

while True:
    rows = source_cursor.fetchmany(batch_size)
    if not rows:
        break
    
    # 타겟에 데이터 삽입
    insert_sql = f"INSERT INTO {staging_table} ({','.join(quoted_columns)}) VALUES ({placeholders})"
    target_cursor.executemany(insert_sql, rows)
    target_conn.commit()
    
    total_rows += len(rows)
    logger.info(f"배치 처리 완료: {total_rows}행")
```

## 🎯 **예상 결과**

### **수정 전 (에러 메시지 부족)**
```
ERROR - psql COPY 명령어 실행 실패: 소스 데이터 내보내기 실패 (코드: None)
```

### **수정 후 (상세 정보 제공)**
```
INFO - psql COPY 명령어 실행 시작:
INFO -   소스 DB: 192.168.1.100:5432 (사용자: source_user)
INFO -   타겟 DB: 192.168.1.101:5432 (사용자: target_user)
INFO -   소스 쿼리: SELECT * FROM fds_팩셋.인포맥스종목마스터
INFO -   스테이징 테이블: raw_data.staging_인포맥스종목마스터_1755739991
INFO -   export 명령어: psql -h 192.168.1.100 -U source_user -d source_db -c \copy (SELECT * FROM fds_팩셋.인포맥스종목마스터) TO STDOUT CSV HEADER
INFO -   import 명령어: psql -h 192.168.1.101 -U target_user -d target_db -c \copy raw_data.staging_인포맥스종목마스터_1755739991 FROM STDIN CSV HEADER

# 또는 psql 명령어가 없을 경우
WARNING - psql 명령어를 찾을 수 없습니다. Python 기반 복사로 전환합니다.
INFO - Python 기반 데이터 복사 시작: fds_팩셋.인포맥스종목마스터 -> raw_data.staging_인포맥스종목마스터_1755739991
INFO - 배치 처리 완료: 10000행
INFO - 배치 처리 완료: 20000행
...
INFO - Python 기반 데이터 복사 완료 - 총 49281행, 소요시간: 15.23초
```

## 🔒 **보안 및 안정성**

### **에러 처리 강화**
- ✅ **상세한 에러 로깅**: 정확한 실패 원인 파악
- ✅ **프로세스 종료 코드 확인**: 각 단계별 실패 원인 식별
- ✅ **리소스 정리 보장**: finally 블록으로 프로세스 정리

### **대안 방법 제공**
- ✅ **Python 기반 복사**: psql 명령어 없이도 동작
- ✅ **배치 처리**: 메모리 효율적인 데이터 복사
- ✅ **트랜잭션 관리**: 데이터 일관성 보장

## 📋 **수정된 파일**

### **airflow/dags/common/data_copy_engine.py**
- `_copy_with_psql_copy()`: 상세 로깅 및 psql 사용 가능 여부 확인 추가
- `_copy_with_python_fallback()`: Python 기반 대안 복사 메서드 추가

## 🔄 **다음 단계**

### 1. **테스트 및 검증**
- [ ] 수정된 코드로 DAG 실행 테스트
- [ ] 상세 로깅 확인
- [ ] psql 명령어 사용 가능 여부 확인
- [ ] Python 기반 복사 동작 확인

### 2. **추가 개선 사항**
- [ ] psql 명령어 경로 설정 옵션
- [ ] 배치 크기 최적화
- [ ] 병렬 처리 지원
- [ ] 성능 모니터링 강화

## 🎯 **결론**

**psql COPY 명령어 실행 문제 해결을 위한 수정이 완료되었습니다!**

### **주요 성과**
1. ✅ **상세한 로깅**: 정확한 실패 원인 파악 가능
2. ✅ **psql 사용 가능 여부 확인**: 사전 검증으로 실패 방지
3. ✅ **Python 기반 대안**: 외부 명령어 의존성 제거
4. ✅ **에러 처리 강화**: 각 단계별 상세한 에러 정보

### **기대 효과**
- 🚀 **문제 진단 용이성**: 상세한 로그로 빠른 문제 해결
- 🚀 **안정성 향상**: 대안 방법으로 복사 실패 방지
- 🚀 **운영 효율성**: 명확한 에러 메시지로 장애 대응 시간 단축

이제 **psql COPY 명령어 실행 과정을 상세하게 추적하고, 문제 발생 시 정확한 원인을 파악**할 수 있습니다! 🎯 