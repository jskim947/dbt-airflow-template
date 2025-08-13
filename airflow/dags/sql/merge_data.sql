-- MERGE 방식으로 데이터를 효율적으로 업데이트/삽입
-- 공식 문서 기준으로 SQLExecuteQueryOperator에서 파라미터를 동적으로 전달받음

-- 1단계: 기존 테이블과 임시 테이블을 MERGE
MERGE INTO {{ params.target_table }} AS target
USING {{ params.temp_table }} AS source
ON target.{{ params.primary_key }} = source.{{ params.primary_key }}

-- 2단계: 매칭되는 레코드가 있으면 업데이트
WHEN MATCHED THEN
    UPDATE SET
        {% for column in params.update_columns %}
        {{ column }} = source.{{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
        {{ params.incremental_field }} = source.{{ params.incremental_field }},
        updated_at = CURRENT_TIMESTAMP

-- 3단계: 매칭되는 레코드가 없으면 삽입
WHEN NOT MATCHED THEN
    INSERT (
        {% for column in params.all_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
    VALUES (
        {% for column in params.all_columns %}
        source.{{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    );

-- 4단계: MERGE 결과 확인
SELECT
    COUNT(*) as total_processed,
    COUNT(CASE WHEN target.{{ params.primary_key }} IS NOT NULL THEN 1 END) as updated_records,
    COUNT(CASE WHEN target.{{ params.primary_key }} IS NULL THEN 1 END) as inserted_records
FROM {{ params.target_table }} target
WHERE target.{{ params.incremental_field }} >= '{{ params.last_update_time }}';
