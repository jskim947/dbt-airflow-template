"""
FastAPI Configuration
FastAPI 애플리케이션 설정을 관리
"""

import os
from typing import Optional


class APIConfig:
    """FastAPI 설정 클래스"""
    
    # 서버 설정
    HOST: str = os.getenv("API_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("API_PORT", "8004"))
    
    # 데이터베이스 설정
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "10.150.2.150")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "15432"))
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "airflow")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "airflow")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "airflow")
    
    # API 설정
    TITLE: str = "PostgreSQL Database API"
    DESCRIPTION: str = "PostgreSQL 데이터베이스 조회 API (다중 스키마 지원)"
    VERSION: str = "1.0.0"
    
    # CORS 설정
    CORS_ORIGINS: list = ["*"]
    CORS_METHODS: list = ["*"]
    CORS_HEADERS: list = ["*"]
    
    # 페이지네이션 설정
    DEFAULT_PAGE_SIZE: int = 100
    MAX_PAGE_SIZE: int = 1000
    MAX_STREAM_LIMIT: int = 10000 