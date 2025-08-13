-- CSV 파일에서 타겟 테이블로 데이터 가져오기
-- COPY 명령어를 사용하여 효율적으로 데이터 삽입
COPY raw_data.temp_인포맥스종목마스터 FROM '/tmp/source_data.csv' WITH CSV HEADER;
