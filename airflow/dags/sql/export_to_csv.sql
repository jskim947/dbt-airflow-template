-- 소스 테이블에서 CSV로 데이터 내보내기
-- COPY 명령어를 사용하여 효율적으로 데이터 추출
COPY (
    SELECT * FROM fds_팩셋.인포맥스종목마스터
    ORDER BY 1
) TO '/tmp/source_data.csv' WITH CSV HEADER;
