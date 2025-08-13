-- 전체 동기화 MERGE SQL (DELETE 절 포함)
-- 소스에 없는 데이터를 타겟에서 삭제하여 완전한 동기화 수행

-- 이 SQL은 Python 코드에서 동적으로 파라미터를 치환하여 사용됩니다.
-- 실제 실행 시에는 모든 placeholder가 실제 값으로 대체됩니다.

MERGE INTO TARGET_TABLE AS target
USING TEMP_TABLE AS source
ON PRIMARY_KEY_CONDITION

-- 매칭되는 레코드가 있으면 업데이트
WHEN MATCHED THEN
    UPDATE SET
        UPDATE_COLUMNS

-- 매칭되는 레코드가 없으면 삽입
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ALL_COLUMNS)
    VALUES (INSERT_VALUES)

-- 소스에 없는 레코드는 타겟에서 삭제 (전체 동기화의 핵심)
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;

-- MERGE 결과 확인
SELECT
    COUNT(*) as total_processed,
    COUNT(CASE WHEN target.PRIMARY_KEY IS NOT NULL THEN 1 END) as updated_records,
    COUNT(CASE WHEN target.PRIMARY_KEY IS NULL THEN 1 END) as inserted_records
FROM TARGET_TABLE target;
