#!/usr/bin/env python3
"""
FastAPI 실행 스크립트
PostgreSQL 데이터베이스 조회 API를 실행합니다.
"""

import uvicorn
from airflow.api.main import app

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8004,
        log_level="info",
        reload=True
    ) 