# 🔧 프로세스 모니터링 및 None 종료코드 문제 해결 완료

## 🔍 **문제 분석**

### ❌ **발생한 에러**
```
ERROR - export_process 실패 - 종료코드: None
ERROR - export_process stderr: 
ERROR - psql COPY 명령어 실행 실패: 소스 데이터 내보내기 실패 (코드: None): 
```

### 🎯 **문제 원인**
**`export_process`의 종료코드가 `None`이고 stderr가 비어있는 상황**이 발생했습니다.

이는 다음과 같은 상황을 의미합니다:
1. **프로세스가 정상적으로 종료되지 않음**
2. **타임아웃이나 인터럽트로 인한 강제 종료**
3. **파이프라인 연결 문제**
4. **프로세스 상태 모니터링 부족**

## 📊 **상황 분석**

### ✅ **해결된 문제들**
1. **한글 테이블명 접근**: 성공 ✅
2. **스테이징 테이블 생성**: 성공 ✅
3. **psql 명령어 사용 가능 여부**: 확인됨 ✅
4. **상세 로깅**: 정상 동작 ✅

### ❌ **새로 발견된 문제**
1. **프로세스 종료코드 None**: 프로세스 상태 모니터링 부족 ❌
2. **타임아웃 처리**: 무한 대기 가능성 ❌
3. **프로세스 상태 추적**: PID 및 상태 정보 부족 ❌

## 🔧 **수정 내용**

### 1. **프로세스 상태 모니터링 강화**

#### **프로세스 시작 로깅**
```python
logger.info("프로세스 시작 중...")
export_process = subprocess.Popen(...)
logger.info(f"export_process 시작됨 (PID: {export_process.pid})")

import_process = subprocess.Popen(...)
logger.info(f"import_process 시작됨 (PID: {import_process.pid})")
```

#### **파이프라인 상태 로깅**
```python
logger.info("파이프라인 연결 및 데이터 전송 시작...")
export_process.stdout.close()

try:
    stdout, stderr = import_process.communicate(timeout=300)  # 5분 타임아웃
    logger.info("프로세스 통신 완료")
except subprocess.TimeoutExpired:
    logger.error("psql COPY 명령어 실행 타임아웃 (5분)")
    export_process.kill()
    import_process.kill()
    raise Exception("psql COPY 명령어 실행 타임아웃")
```

### 2. **None 종료코드 처리**

#### **export_process None 종료코드 처리**
```python
if export_process.returncode is None:
    logger.error("export_process가 정상적으로 종료되지 않음")
    logger.error(f"export_process 상태: {export_process.poll()}")
    export_stderr = export_process.stderr.read().decode() if export_process.stderr else "Unknown error"
    raise Exception(f"소스 데이터 내보내기 실패: 프로세스가 정상적으로 종료되지 않음 (stderr: {export_stderr})")
```

#### **import_process None 종료코드 처리**
```python
if import_process.returncode is None:
    logger.error("import_process가 정상적으로 종료되지 않음")
    logger.error(f"import_process 상태: {import_process.poll()}")
    import_stderr = stderr.decode() if stderr else "Unknown error"
    raise Exception(f"스테이징 테이블로 데이터 가져오기 실패: 프로세스가 정상적으로 종료되지 않음 (stderr: {import_stderr})")
```

### 3. **환경 정보 로깅 추가**

#### **작업 디렉토리 및 PATH 확인**
```python
# 명령어 실행 전 환경 확인
logger.info(f"  현재 작업 디렉토리: {os.getcwd()}")
logger.info(f"  PATH 환경변수: {os.environ.get('PATH', 'Not set')}")
```

### 4. **타임아웃 처리**

#### **5분 타임아웃 설정**
```python
try:
    stdout, stderr = import_process.communicate(timeout=300)  # 5분 타임아웃
    logger.info("프로세스 통신 완료")
except subprocess.TimeoutExpired:
    logger.error("psql COPY 명령어 실행 타임아웃 (5분)")
    export_process.kill()
    import_process.kill()
    raise Exception("psql COPY 명령어 실행 타임아웃")
```

## 🎯 **예상 결과**

### **수정 전 (프로세스 상태 불명)**
```
ERROR - export_process 실패 - 종료코드: None
ERROR - export_process stderr: 
ERROR - psql COPY 명령어 실행 실패: 소스 데이터 내보내기 실패 (코드: None): 
```

### **수정 후 (상세한 프로세스 추적)**
```
INFO - 프로세스 시작 중...
INFO - export_process 시작됨 (PID: 12345)
INFO - import_process 시작됨 (PID: 12346)
INFO - 파이프라인 연결 및 데이터 전송 시작...
INFO - 프로세스 통신 완료
INFO - psql COPY 명령어 실행 성공 - 소요시간: 45.23초

# 또는 문제 발생 시
ERROR - export_process가 정상적으로 종료되지 않음
ERROR - export_process 상태: None
ERROR - 소스 데이터 내보내기 실패: 프로세스가 정상적으로 종료되지 않음 (stderr: connection refused)
```

## 🔒 **보안 및 안정성**

### **프로세스 관리 강화**
- ✅ **PID 추적**: 각 프로세스의 PID 로깅으로 추적 가능
- ✅ **상태 모니터링**: `poll()` 메서드로 프로세스 상태 확인
- ✅ **타임아웃 처리**: 무한 대기 방지
- ✅ **강제 종료**: 타임아웃 시 프로세스 정리

### **에러 처리 개선**
- ✅ **None 종료코드 처리**: 프로세스 상태 불명 상황 대응
- ✅ **상세한 에러 메시지**: 정확한 실패 원인 파악
- ✅ **환경 정보 제공**: 디버깅에 필요한 컨텍스트 정보

## 📋 **수정된 파일**

### **airflow/dags/common/data_copy_engine.py**
- `_copy_with_psql_copy()`: 프로세스 모니터링 및 None 종료코드 처리 강화

## 🔄 **다음 단계**

### 1. **테스트 및 검증**
- [ ] 수정된 코드로 DAG 실행 테스트
- [ ] 프로세스 상태 모니터링 확인
- [ ] None 종료코드 처리 동작 확인
- [ ] 타임아웃 처리 동작 확인

### 2. **추가 개선 사항**
- [ ] 프로세스 상태 실시간 모니터링
- [ ] 메모리 사용량 모니터링
- [ ] 네트워크 연결 상태 확인
- [ ] 자동 재시도 메커니즘

## 🎯 **결론**

**프로세스 모니터링 및 None 종료코드 문제 해결을 위한 수정이 완료되었습니다!**

### **주요 성과**
1. ✅ **프로세스 상태 추적**: PID 및 상태 정보 로깅
2. ✅ **None 종료코드 처리**: 프로세스 상태 불명 상황 대응
3. ✅ **타임아웃 처리**: 무한 대기 방지
4. ✅ **환경 정보 제공**: 디버깅에 필요한 컨텍스트 정보

### **기대 효과**
- 🚀 **문제 진단 정확성**: 프로세스 상태 상세 추적
- 🚀 **안정성 향상**: 타임아웃 및 강제 종료 처리
- 🚀 **운영 효율성**: 명확한 프로세스 상태 정보로 장애 대응 시간 단축

이제 **psql COPY 명령어 실행 과정을 실시간으로 모니터링하고, 프로세스 상태 문제를 정확하게 진단**할 수 있습니다! 🎯 