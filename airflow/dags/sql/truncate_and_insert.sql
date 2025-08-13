-- TRUNCATE + INSERT 방식으로 안전하게 데이터 교체
-- 1단계: 기존 테이블 데이터 삭제 (테이블 구조는 유지)
TRUNCATE TABLE raw_data.인포맥스종목마스터;

-- 2단계: 임시 테이블의 데이터를 기존 테이블에 삽입
INSERT INTO raw_data.인포맥스종목마스터
SELECT * FROM raw_data.temp_인포맥스종목마스터;

-- 3단계: 삽입된 레코드 수 확인
SELECT COUNT(*) as inserted_count FROM raw_data.인포맥스종목마스터;
