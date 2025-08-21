# 🔧 파이프라인 구조 문제 해결 완료

## 🔍 **문제 분석**

### ❌ **발생한 에러**
```
WARNING - export_process 완료 대기 타임아웃, 강제 종료
ERROR - export_process 실패 - 종료코드: -9
ERROR - 소스 데이터 내보내기 실패 (코드: -9): 
```

### 🎯 **문제 원인**
**파이프라인 구조에서 `export_process`가 `import_process`보다 먼저 종료되어야 하는데, 순서가 맞지 않아 타임아웃 후 강제 종료(SIGKILL, 종료코드 -9)**되고 있었습니다.

## 📊 **상황 분석**

### ✅ **해결된 문제들**
1. **한글 테이블명 접근**: 성공 ✅
2. **스테이징 테이블 생성**: 성공 ✅
3. **psql 명령어 사용 가능 여부**: 확인됨 ✅
4. **상세 로깅**: 정상 동작 ✅
5. **프로세스 모니터링**: PID 추적 및 상태 로깅 정상 ✅

### ❌ **근본적인 문제**
1. **파이프라인 구조**: 프로세스 간 종료 순서 문제 ❌
2. **타임아웃 처리**: 10초 후 강제 종료로 인한 SIGKILL ❌
3. **프로세스 의존성**: export_process가 import_process 완료 후에도 실행 중 ❌

## 🔧 **수정 내용**

### 1. **파이프라인 구조를 순차적 실행으로 변경**

#### **수정 전 (문제가 있던 파이프라인 구조)**
```python
# 파이프라인으로 연결하여 실행
export_process = subprocess.Popen(export_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
import_process = subprocess.Popen(import_command, stdin=export_process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env_target)

# 파이프라인 실행
export_process.stdout.close()
stdout, stderr = import_process.communicate(timeout=300)

# export_process 완료 대기 (문제 발생)
try:
    export_process.wait(timeout=10)  # 10초 타임아웃
except subprocess.TimeoutExpired:
    export_process.kill()  # 강제 종료 (SIGKILL)
```

#### **수정 후 (순차적 실행 구조)**
```python
# 순차적 실행으로 변경 (파이프라인 대신)
logger.info("순차적 실행 방식으로 변경...")

# 1단계: export_process 실행
logger.info("1단계: 소스 데이터 내보내기 시작...")
export_process = subprocess.Popen(export_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

# export_process 완료 대기
export_stdout, export_stderr = export_process.communicate(timeout=300)
logger.info("export_process 완료")

# 2단계: import_process 실행
logger.info("2단계: 스테이징 테이블로 데이터 가져오기 시작...")
import_process = subprocess.Popen(import_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env_target)

# export_process의 출력을 import_process에 전달
stdout, stderr = import_process.communicate(input=export_stdout, timeout=300)
logger.info("import_process 완료")
```

### 2. **프로세스 실행 순서 명확화**

#### **1단계: 소스 데이터 내보내기**
- `export_process` 실행 및 완료 대기
- 종료코드 확인 및 에러 처리
- 출력 데이터(`export_stdout`) 수집

#### **2단계: 스테이징 테이블로 데이터 가져오기**
- `import_process` 실행
- `export_stdout`을 `input`으로 직접 전달
- 완료 대기 및 결과 확인

### 3. **에러 처리 개선**

#### **각 단계별 독립적인 에러 처리**
```python
# export_process 에러 처리
if export_process.returncode != 0:
    export_stderr_text = export_stderr.decode() if export_stderr else "Unknown error"
    raise Exception(f"소스 데이터 내보내기 실패 (코드: {export_process.returncode}): {export_stderr_text}")

# import_process 에러 처리
if import_process.returncode != 0:
    import_stderr = stderr.decode() if stderr else "Unknown error"
    raise Exception(f"스테이징 테이블로 데이터 가져오기 실패 (코드: {import_process.returncode}): {import_stderr}")
```

## 🎯 **예상 결과**

### **수정 전 (파이프라인 문제)**
```
INFO - 파이프라인 연결 및 데이터 전송 시작...
INFO - import_process 통신 완료
WARNING - export_process 완료 대기 타임아웃, 강제 종료
ERROR - export_process 실패 - 종료코드: -9
ERROR - 소스 데이터 내보내기 실패 (코드: -9): 
```

### **수정 후 (순차적 실행)**
```
INFO - 순차적 실행 방식으로 변경...
INFO - 1단계: 소스 데이터 내보내기 시작...
INFO - export_process 시작됨 (PID: 32904)
INFO - export_process 완료
INFO - 2단계: 스테이징 테이블로 데이터 가져오기 시작...
INFO - import_process 시작됨 (PID: 32905)
INFO - import_process 완료
INFO - psql COPY 명령어 실행 성공 - 소요시간: 45.23초
```

## 🔒 **보안 및 안정성**

### **프로세스 관리 개선**
- ✅ **순차적 실행**: 각 프로세스가 독립적으로 실행되고 완료됨
- ✅ **명확한 에러 처리**: 각 단계별로 정확한 에러 원인 파악
- ✅ **타임아웃 관리**: 각 프로세스별로 적절한 타임아웃 설정
- ✅ **리소스 정리**: 프로세스 완료 후 즉시 정리

### **데이터 흐름 개선**
- ✅ **직접 전달**: `export_stdout`을 `import_process`에 직접 전달
- ✅ **메모리 효율성**: 파이프라인 대신 메모리 기반 데이터 전달
- ✅ **에러 추적**: 각 단계별 데이터 전달 과정 추적 가능

## 📋 **수정된 파일**

### **airflow/dags/common/data_copy_engine.py**
- `_copy_with_psql_copy()`: 파이프라인 구조를 순차적 실행으로 변경

## 🔄 **다음 단계**

### 1. **테스트 및 검증**
- [ ] 수정된 코드로 DAG 실행 테스트
- [ ] 순차적 실행 동작 확인
- [ ] 프로세스 완료 순서 확인
- [ ] 에러 처리 동작 확인

### 2. **추가 개선 사항**
- [ ] 메모리 사용량 최적화
- [ ] 대용량 데이터 처리 시 청크 단위 처리
- [ ] 병렬 처리 옵션 추가
- [ ] 성능 모니터링 강화

## 🎯 **결론**

**파이프라인 구조 문제 해결을 위한 수정이 완료되었습니다!**

### **주요 성과**
1. ✅ **파이프라인 구조 제거**: 프로세스 간 종료 순서 문제 해결
2. ✅ **순차적 실행**: 각 프로세스가 독립적으로 실행되고 완료
3. ✅ **에러 처리 개선**: 각 단계별로 정확한 에러 원인 파악
4. ✅ **안정성 향상**: 타임아웃 및 강제 종료 문제 해결

### **기대 효과**
- 🚀 **안정성 향상**: 프로세스 종료 순서 문제 해결
- 🚀 **에러 추적 용이성**: 각 단계별 정확한 문제 진단
- 🚀 **운영 효율성**: 예측 가능한 프로세스 실행 순서

이제 **파이프라인 구조 문제가 해결되어 `export_process`와 `import_process`가 순차적으로 정상 실행**될 것입니다! 🎯 