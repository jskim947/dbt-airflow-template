"""
FastAPI Main Application
PostgreSQL 데이터베이스 조회를 위한 FastAPI 메인 애플리케이션
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from airflow.api.config import APIConfig
from airflow.api.routers import database_router
from airflow.api.models import RootResponse, HealthResponse

# FastAPI 애플리케이션 생성
app = FastAPI(
    title=APIConfig.TITLE,
    description=APIConfig.DESCRIPTION,
    version=APIConfig.VERSION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS 미들웨어 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=APIConfig.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=APIConfig.CORS_METHODS,
    allow_headers=APIConfig.CORS_HEADERS,
)

# 라우터 등록
app.include_router(database_router)


@app.get("/", response_model=RootResponse)
async def root():
    """루트 엔드포인트 - API 정보와 사용 가능한 엔드포인트를 반환합니다."""
    return RootResponse(
        message="PostgreSQL Database API Server (Multi-Schema Support)",
        docs="/docs",
        endpoints={
            "schemas": "/api/schemas",
            "tables": "/api/tables",
            "tables_by_schema": "/api/schemas/{schema}/tables",
            "schema_info": "/api/schema/{table_name}",
            "schema_info_by_schema": "/api/schemas/{schema}/tables/{table_name}/schema",
            "query": "/api/query/{table_name}",
            "query_by_schema": "/api/schemas/{schema}/tables/{table_name}/query",
            "stream": "/api/stream/{table_name}",
            "stream_by_schema": "/api/schemas/{schema}/tables/{table_name}/stream"
        }
    )


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """서버 상태를 확인합니다."""
    try:
        # 간단한 헬스체크 - 실제로는 데이터베이스 연결 테스트를 할 수 있습니다
        return HealthResponse(
            status="healthy",
            database="connected"
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            database="disconnected",
            error=str(e)
        )


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """전역 예외 핸들러"""
    return JSONResponse(
        status_code=500,
        content={
            "detail": "내부 서버 오류가 발생했습니다",
            "error": str(exc)
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host=APIConfig.HOST, 
        port=APIConfig.PORT,
        log_level="info"
    ) 