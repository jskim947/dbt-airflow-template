-- 소스 테이블의 스키마 정보를 가져오는 쿼리
-- 공식 문서 방식으로 information_schema 사용
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    ordinal_position
FROM information_schema.columns
WHERE table_schema = %(schema_name)s
  AND table_name = %(table_name)s
ORDER BY ordinal_position;
