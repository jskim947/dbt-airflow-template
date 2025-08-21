"""
FastAPI Dependencies
FastAPI 애플리케이션의 의존성 주입을 관리
"""

import asyncpg
from fastapi import HTTPException
from typing import AsyncGenerator

from airflow.api.config import APIConfig


async def get_db_connection() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    PostgreSQL 데이터베이스 연결을 제공하는 의존성
    
    Yields:
        asyncpg.Connection: 데이터베이스 연결 객체
        
    Raises:
        HTTPException: 데이터베이스 연결 실패 시
    """
    try:
        conn = await asyncpg.connect(
            host=APIConfig.POSTGRES_HOST,
            port=APIConfig.POSTGRES_PORT,
            user=APIConfig.POSTGRES_USER,
            password=APIConfig.POSTGRES_PASSWORD,
            database=APIConfig.POSTGRES_DB
        )
        try:
            yield conn
        finally:
            await conn.close()
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"데이터베이스 연결 실패: {str(e)}"
        ) 