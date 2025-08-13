#!/bin/bash

# dbt 프로젝트 설치 및 설정 스크립트

echo "Installing dbt project dependencies..."

# dbt 프로젝트 디렉토리로 이동
cd "$(dirname "$0")"

# dbt 의존성 설치
echo "Installing dbt packages..."
dbt deps

# dbt 프로젝트 파싱 테스트
echo "Testing dbt project parsing..."
dbt parse

if [ $? -eq 0 ]; then
    echo "dbt project parsing successful!"

    # dbt 모델 실행
    echo "Running dbt models..."
    dbt run

    # dbt 테스트 실행
    echo "Running dbt tests..."
    dbt test

    echo "dbt project setup completed successfully!"
else
    echo "dbt project parsing failed!"
    exit 1
fi
