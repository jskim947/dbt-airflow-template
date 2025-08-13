-- 동적으로 생성된 스키마로 임시 테이블 생성
-- 기존 임시 테이블이 있으면 삭제
DROP TABLE IF EXISTS raw_data.temp_인포맥스종목마스터;

-- 임시 테이블 생성 (컬럼 정의는 Python에서 동적 생성)
-- 이 파일은 템플릿으로 사용되며, 실제 컬럼 정의는 Python에서 주입
CREATE TABLE raw_data.temp_인포맥스종목마스터 (
    -- 컬럼 정의는 Python에서 동적 생성
    -- 예시: id INTEGER, name VARCHAR(255), created_at TIMESTAMP
);
