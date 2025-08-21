# Data Copy Engine 청크 방식 전환 계획서

## 📋 프로젝트 개요

### **목표**
기존의 메모리 누적 방식에서 진짜 청크 단위 처리 방식으로 `data_copy_engine.py`를 전환하여 메모리 사용량을 대폭 줄이고 대용량 테이블 처리 안정성을 향상시킨다.

### **현재 문제점**
- 메모리에 전체 데이터 누적 후 한 번에 CSV 저장
- 596만 행 테이블 처리 시 약 400-600MB 메모리 사용
- PostgreSQL MVCC 충돌로 인한 안정성 문제
- 네트워크 지연 시 메모리 사용량 지속 증가
- **데이터베이스 세션 타임아웃으로 인한 연결 끊김 위험**
- **청크 실패 시 데이터 무결성 보장 부족**

### **개선 목표**
- 메모리 사용량: 90% 이상 감소 (500MB → 50MB 이하)
- 처리 안정성: PostgreSQL 충돌 95% 감소
- 확장성: 테이블 크기에 관계없이 일정한 메모리 사용
- **세션 안정성: 99% 이상 연결 유지**
- **데이터 무결성: 100% 보장**

## 🏗️ 아키텍처 변경 계획

### **기존 구조**
```
소스 DB → 배치별 데이터 가져오기 → 메모리 누적 → DataFrame 변환 → CSV 저장 → 타겟 DB
```

### **새로운 구조**
```
소스 DB → 배치별 데이터 가져오기 → 즉시 CSV 추가 → 메모리 해제 → 타겟 DB
```

### **개선된 안전 구조**
```
소스 DB → 세션 관리 → 트랜잭션 기반 청크 처리 → 체크포인트 저장 → CSV 추가 → 메모리 해제 → 타겟 DB
```

## 🔧 구현 세부 계획

### **1단계: export_to_csv 메서드 전면 재작성**

#### **1.1 메서드 시그니처 변경**
```python
def export_to_csv(
    self,
    table_name: str,
    csv_path: str,
    where_clause: str | None = None,
    batch_size: int = 10000,
    order_by_field: str | None = None,
    chunk_mode: bool = True,  # 새로운 파라미터
    enable_checkpoint: bool = True,  # 체크포인트 활성화
    max_retries: int = 3  # 최대 재시도 횟수
) -> int:
```

#### **1.2 개선된 청크 처리 로직 구현**
```python
def _export_to_csv_chunked(self, table_name, csv_path, where_clause, batch_size, order_by_field, enable_checkpoint=True, max_retries=3):
    """
    세션 관리와 트랜잭션 기반 청크 처리 (메모리 누적 없음)
    """
    # 체크포인트에서 복구 시도
    start_offset, total_exported = 0, 0
    if enable_checkpoint:
        start_offset, total_exported = self._resume_from_checkpoint(table_name, csv_path)
    
    session_refresh_interval = 50  # 50개 청크마다 세션 갱신
    chunk_count = 0
    
    with open(csv_path, 'a' if start_offset > 0 else 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # 헤더는 처음에만 쓰기
        if start_offset == 0:
            columns = self._get_table_columns(table_name)
            writer.writerow(columns)
        
        offset = start_offset
        
        while offset < total_count:
            try:
                # 세션 상태 확인 및 갱신
                if chunk_count % session_refresh_interval == 0:
                    self._refresh_database_session()
                    logger.info(f"데이터베이스 세션 갱신 (청크 {chunk_count})")
                
                # 트랜잭션 기반 청크 처리
                batch_result = self._process_single_chunk_with_transaction(
                    table_name, where_clause, batch_size, offset, order_by_field, writer, max_retries
                )
                
                if batch_result['success']:
                    total_exported += batch_result['rows_processed']
                    offset += batch_size
                    chunk_count += 1
                    
                    # 체크포인트 저장
                    if enable_checkpoint:
                        self._save_checkpoint(table_name, offset, total_exported, csv_path)
                    
                    # 진행률 로깅
                    logger.info(f"청크 처리 진행률: {total_exported}/{total_count}")
                    
                    # 메모리 사용량 모니터링
                    self._check_memory_usage()
                else:
                    # 청크 실패 시 오류 처리
                    error_action = self._handle_chunk_error(batch_result['error'], offset, batch_size)
                    if error_action == 'skip':
                        offset += batch_size
                        logger.warning(f"청크 건너뛰기 (offset {offset})")
                    elif error_action == 'retry':
                        continue  # 재시도
                    else:
                        raise Exception(f"치명적 오류 발생: {batch_result['error']}")
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_error:
                # 데이터베이스 연결 오류 시 세션 재생성
                logger.error(f"데이터베이스 오류 발생: {db_error}")
                self._refresh_database_session()
                
                # 실패한 청크 재시도
                continue
                
            except Exception as chunk_error:
                # 기타 오류 시 로깅 및 재시도
                logger.error(f"청크 처리 오류 (offset {offset}): {chunk_error}")
                error_action = self._handle_chunk_error(chunk_error, offset, batch_size)
                if error_action == 'skip':
                    offset += batch_size
                elif error_action == 'retry':
                    continue
                else:
                    raise chunk_error
    
    # 체크포인트 정리
    if enable_checkpoint:
        self._cleanup_checkpoint(csv_path)
    
    return total_exported
```

### **2단계: 세션 관리 및 모니터링 시스템**

#### **2.1 세션 상태 확인 및 갱신**
```python
def _refresh_database_session(self):
    """
    데이터베이스 세션 상태 확인 및 갱신
    """
    try:
        # 현재 세션 상태 확인
        if hasattr(self, 'db_ops') and hasattr(self.db_ops, 'connection'):
            # 연결 상태 확인
            if self.db_ops.connection.closed:
                logger.warning("데이터베이스 연결이 끊어짐, 재연결 시도")
                self.db_ops.reconnect()
            else:
                # 간단한 쿼리로 세션 상태 확인
                self.db_ops.execute_query("SELECT 1")
                
    except Exception as e:
        logger.error(f"세션 상태 확인 실패: {e}")
        # 강제 재연결
        self.db_ops.reconnect()

def _check_session_health(self):
    """
    세션 상태 종합 점검
    """
    try:
        # 연결 상태 확인
        if self.db_ops.connection.closed:
            return False
        
        # 세션 타임아웃 확인
        cursor = self.db_ops.connection.cursor()
        cursor.execute("SELECT current_timestamp - query_start FROM pg_stat_activity WHERE pid = pg_backend_pid()")
        result = cursor.fetchone()
        
        if result and result[0]:
            # 30분 이상 실행 중인 쿼리가 있으면 경고
            if result[0].total_seconds() > 1800:
                logger.warning("세션에 장시간 실행 중인 쿼리 감지")
                return False
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"세션 상태 점검 실패: {e}")
        return False
```

#### **2.2 메모리 사용량 체크 메서드**
```python
def _check_memory_usage(self):
    """
    메모리 사용량 모니터링 및 관리
    """
    import psutil
    
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    # 메모리 임계값 설정
    WARNING_THRESHOLD = 100  # 100MB
    CRITICAL_THRESHOLD = 200  # 200MB
    
    if memory_mb > CRITICAL_THRESHOLD:
        logger.error(f"메모리 사용량 위험: {memory_mb:.1f}MB")
        self._force_memory_cleanup()
    elif memory_mb > WARNING_THRESHOLD:
        logger.warning(f"메모리 사용량 높음: {memory_mb:.1f}MB")
        gc.collect()
    
    return memory_mb
```

#### **2.3 강제 메모리 정리 메서드**
```python
def _force_memory_cleanup(self):
    """
    강제 메모리 정리
    """
    import gc
    
    # 가비지 컬렉션 강제 실행
    gc.collect()
    
    # Python 객체 참조 정리
    for obj in gc.get_objects():
        if hasattr(obj, '__dict__'):
            obj.__dict__.clear()
    
    logger.info("강제 메모리 정리 완료")
```

### **3단계: 트랜잭션 기반 오류 처리 및 복구 시스템**

#### **3.1 트랜잭션 기반 청크 처리**
```python
def _process_single_chunk_with_transaction(self, table_name, where_clause, batch_size, offset, order_by_field, writer, max_retries=3):
    """
    트랜잭션 기반 청크 처리
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 트랜잭션 시작
            with self.db_ops.connection.cursor() as cursor:
                # 청크 데이터 조회
                query = self._build_chunk_query(table_name, where_clause, batch_size, offset, order_by_field)
                cursor.execute(query)
                
                rows_processed = 0
                for row in cursor:
                    writer.writerow(row)
                    rows_processed += 1
                
                # 트랜잭션 커밋
                self.db_ops.connection.commit()
                
                return {
                    'rows_processed': rows_processed,
                    'success': True,
                    'error': None
                }
                
        except Exception as e:
            # 트랜잭션 롤백
            self.db_ops.connection.rollback()
            retry_count += 1
            
            if retry_count >= max_retries:
                logger.error(f"청크 처리 최대 재시도 횟수 초과: {e}")
                return {
                    'rows_processed': 0,
                    'success': False,
                    'error': str(e)
                }
            
            logger.warning(f"청크 처리 재시도 {retry_count}/{max_retries}: {e}")
            time.sleep(2 ** retry_count)  # 지수 백오프
```

#### **3.2 세분화된 오류 분류 및 처리**
```python
def _handle_chunk_error(self, error, offset, batch_size):
    """
    청크 오류 처리 및 데이터 무결성 보장
    """
    error_type = type(error).__name__
    
    if isinstance(error, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        # 데이터베이스 연결 오류 - 재시도 가능
        logger.warning(f"데이터베이스 오류로 인한 청크 실패 (offset {offset}), 재시도 예정")
        return 'retry'
        
    elif isinstance(error, (psycopg2.DataError, psycopg2.IntegrityError)):
        # 데이터 오류 - 재시도 불가능
        logger.error(f"데이터 오류로 인한 청크 실패 (offset {offset}), 건너뛰기")
        return 'skip'
        
    elif isinstance(error, psycopg2.InternalError):
        # 내부 오류 - 재시도 가능
        logger.warning(f"내부 오류로 인한 청크 실패 (offset {offset}), 재시도 예정")
        return 'retry'
        
    else:
        # 기타 오류 - 로깅 후 재시도
        logger.error(f"예상치 못한 오류로 인한 청크 실패 (offset {offset}): {error}")
        return 'retry'
```

### **4단계: 체크포인트 및 복구 시스템**

#### **4.1 진행 상황 저장**
```python
def _save_checkpoint(self, table_name, offset, total_exported, csv_path):
    """
    청크 처리 진행 상황 저장
    """
    checkpoint_data = {
        'table_name': table_name,
        'offset': offset,
        'total_exported': total_exported,
        'csv_path': csv_path,
        'timestamp': pd.Timestamp.now().isoformat(),
        'status': 'in_progress',
        'checksum': self._calculate_csv_checksum(csv_path)
    }
    
    checkpoint_file = f"{csv_path}.checkpoint"
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)
    
    logger.debug(f"체크포인트 저장: offset {offset}, 처리된 행 {total_exported}")
```

#### **4.2 복구 기능**
```python
def _resume_from_checkpoint(self, table_name, csv_path):
    """
    체크포인트에서 복구
    """
    checkpoint_file = f"{csv_path}.checkpoint"
    
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            if checkpoint_data['status'] == 'in_progress':
                # 체크섬 검증
                if self._verify_checkpoint_integrity(checkpoint_data, csv_path):
                    logger.info(f"체크포인트에서 복구: {checkpoint_data['offset']}부터 시작")
                    return checkpoint_data['offset'], checkpoint_data['total_exported']
                else:
                    logger.warning("체크포인트 무결성 검증 실패, 처음부터 시작")
                    return 0, 0
        except Exception as e:
            logger.error(f"체크포인트 읽기 실패: {e}")
            return 0, 0
    
    return 0, 0

def _verify_checkpoint_integrity(self, checkpoint_data, csv_path):
    """
    체크포인트 무결성 검증
    """
    try:
        if not os.path.exists(csv_path):
            return False
        
        # 파일 크기 확인
        file_size = os.path.getsize(csv_path)
        if file_size == 0:
            return False
        
        # 체크섬 검증
        current_checksum = self._calculate_csv_checksum(csv_path)
        if current_checksum != checkpoint_data.get('checksum'):
            logger.warning("체크섬 불일치, 체크포인트 무효화")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"체크포인트 무결성 검증 실패: {e}")
        return False

def _calculate_csv_checksum(self, csv_path):
    """
    CSV 파일 체크섬 계산
    """
    import hashlib
    
    try:
        with open(csv_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception:
        return None

def _cleanup_checkpoint(self, csv_path):
    """
    체크포인트 파일 정리
    """
    checkpoint_file = f"{csv_path}.checkpoint"
    if os.path.exists(checkpoint_file):
        try:
            # 완료 상태로 업데이트
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            checkpoint_data['status'] = 'completed'
            checkpoint_data['completed_at'] = pd.Timestamp.now().isoformat()
            
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            # 일정 시간 후 삭제 (선택사항)
            # os.remove(checkpoint_file)
            
        except Exception as e:
            logger.error(f"체크포인트 정리 실패: {e}")
```

### **5단계: 성능 최적화**

#### **5.1 배치 크기 동적 조정**
```python
def _optimize_batch_size_dynamically(self, initial_batch_size, memory_usage, processing_time, session_health):
    """
    메모리 사용량, 처리 시간, 세션 상태에 따른 배치 크기 동적 조정
    """
    if not session_health:
        # 세션 상태 불량 시 배치 크기 감소
        new_batch_size = max(initial_batch_size // 4, 100)
        logger.info(f"세션 상태 불량, 배치 크기 조정: {initial_batch_size} → {new_batch_size}")
        return new_batch_size
    
    elif memory_usage > 150:  # 150MB 초과
        new_batch_size = max(initial_batch_size // 2, 100)
        logger.info(f"메모리 사용량 높음, 배치 크기 조정: {initial_batch_size} → {new_batch_size}")
        return new_batch_size
    
    elif processing_time > 30:  # 30초 초과
        new_batch_size = max(initial_batch_size // 2, 100)
        logger.info(f"처리 시간 길음, 배치 크기 조정: {initial_batch_size} → {new_batch_size}")
        return new_batch_size
    
    elif memory_usage < 50 and processing_time < 10 and session_health:  # 여유로운 상황
        new_batch_size = min(initial_batch_size * 2, 2000)
        logger.info(f"여유로운 상황, 배치 크기 증가: {initial_batch_size} → {new_batch_size}")
        return new_batch_size
    
    return initial_batch_size
```

#### **5.2 병렬 처리 지원 (선택적)**
```python
def _export_to_csv_parallel(self, table_name, csv_path, where_clause, batch_size, order_by_field, num_workers=2):
    """
    병렬 처리를 통한 성능 향상 (선택적)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    # 청크 범위 계산
    total_count = self.db_ops.get_table_row_count(table_name, where_clause)
    chunk_ranges = self._calculate_chunk_ranges(total_count, batch_size, num_workers)
    
    # 병렬 처리
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        
        for chunk_start, chunk_end in chunk_ranges:
            future = executor.submit(
                self._process_chunk_range,
                table_name, where_clause, chunk_start, chunk_end, order_by_field
            )
            futures.append(future)
        
        # 결과 수집 및 CSV 병합
        all_chunks = []
        for future in as_completed(futures):
            chunk_data = future.result()
            all_chunks.append(chunk_data)
        
        # 청크별로 CSV에 순차적으로 쓰기
        self._merge_chunks_to_csv(all_chunks, csv_path)
```

## 📊 구현 우선순위

### **Phase 1: 핵심 안전성 기능 (1-2주)**
- [ ] 세션 관리 및 모니터링 시스템 구현
- [ ] 트랜잭션 기반 청크 처리
- [ ] 체크포인트 및 복구 시스템

### **Phase 2: 오류 처리 강화 (1-2주)**
- [ ] 세분화된 오류 분류 및 처리
- [ ] 재시도 메커니즘 구현
- [ ] 데이터 무결성 검증

### **Phase 3: 성능 최적화 (1주)**
- [ ] 세션 풀링 최적화
- [ ] 배치 크기 동적 조정
- [ ] 성능 모니터링

## ✅ 체크리스트

### **설계 및 계획**
- [ ] 현재 코드 분석 및 영향도 평가
- [ ] 새로운 아키텍처 설계 검토
- [ ] 테스트 계획 수립
- [ ] 롤백 계획 수립

### **개발 환경 준비**
- [ ] 개발 브랜치 생성
- [ ] 테스트 데이터 준비 (대용량 테이블)
- [ ] 메모리 모니터링 도구 설정
- [ ] 성능 측정 도구 설정
- [ ] **세션 모니터링 도구 설정**

### **핵심 기능 구현**
- [ ] `_export_to_csv_chunked` 메서드 구현
- [ ] **세션 관리 및 모니터링 메서드 구현**
- [ ] **트랜잭션 기반 청크 처리 구현**
- [ ] **체크포인트 및 복구 시스템 구현**
- [ ] 메모리 모니터링 메서드 구현
- [ ] 에러 복구 시스템 구현
- [ ] 기존 메서드와의 호환성 확인

### **테스트 및 검증**
- [ ] 단위 테스트 작성
- [ ] **세션 안정성 테스트**
- [ ] **트랜잭션 무결성 테스트**
- [ ] 메모리 사용량 테스트
- [ ] 대용량 테이블 처리 테스트
- [ ] **오류 상황 복구 테스트**
- [ ] 에러 상황 테스트
- [ ] 성능 비교 테스트

### **배포 및 모니터링**
- [ ] 스테이징 환경 테스트
- [ ] 프로덕션 배포
- [ ] 실시간 모니터링 설정
- [ ] **세션 상태 모니터링 설정**
- [ ] 성능 지표 수집

## ⚠️ 위험 요소 및 대응 방안

### **1. 데이터 무결성 위험**
- **위험**: 청크 처리 중 일부 실패 시 데이터 손실
- **대응**: **트랜잭션 기반 처리, 체크포인트 시스템, 실패한 청크 재시도**

### **2. 세션 타임아웃 위험**
- **위험**: 긴 루프 동안 데이터베이스 세션 타임아웃
- **대응**: **세션 상태 모니터링, 주기적 세션 갱신, 자동 재연결**

### **3. 성능 저하 위험**
- **위험**: 청크별 파일 I/O로 인한 성능 저하
- **대응**: 배치 크기 최적화, 버퍼링, 병렬 처리 검토

### **4. 호환성 위험**
- **위험**: 기존 코드와의 호환성 문제
- **대응**: 점진적 전환, 플래그 기반 동작, 철저한 테스트

## 🎯 성공 지표

### **정량적 지표**
- 메모리 사용량: 500MB → 50MB 이하 (90% 감소)
- PostgreSQL 충돌: 95% 감소
- 처리 안정성: 99% 이상 성공률
- **세션 안정성: 99% 이상 연결 유지**
- **데이터 무결성: 100% 보장**

### **정성적 지표**
- 코드 가독성 및 유지보수성 향상
- **오류 처리 및 복구 능력 대폭 향상**
- **세션 관리 안정성 향상**
- 확장성 및 안정성 향상

## 📝 결론

이 계획서는 `data_copy_engine.py`를 **세션 관리와 트랜잭션 기반의 안전한 청크 방식**으로 전환하여 메모리 문제와 데이터 무결성 문제를 동시에 해결하는 것을 목표로 합니다. 

**핵심은 "메모리에 누적하지 않고, 세션을 안전하게 관리하며, 트랜잭션으로 데이터 무결성을 보장하고, 체크포인트로 복구 가능하게" 하는 것입니다.**

**주요 개선사항:**
1. **세션 관리**: 주기적 세션 상태 확인 및 갱신
2. **트랜잭션 기반**: 각 청크를 독립적인 트랜잭션으로 처리
3. **체크포인트 시스템**: 진행 상황 저장 및 복구 기능
4. **세분화된 오류 처리**: 오류 유형별 적절한 대응 방안

단계별 구현을 통해 안전하고 효율적인 전환을 진행하며, 각 단계마다 철저한 테스트와 검증을 거쳐 안정성을 확보할 것입니다.

**예상 완료 기간: 4-7주**
**예상 효과: 메모리 사용량 90% 감소, 안정성 대폭 향상, 데이터 무결성 100% 보장** 🚀