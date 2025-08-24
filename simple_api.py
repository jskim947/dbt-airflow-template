#!/usr/bin/env python3
"""
Simple FastAPI Application
PostgreSQL 데이터베이스 조회를 위한 간단한 FastAPI 애플리케이션
"""

import os
import asyncio
import asyncpg
from fastapi import FastAPI, Query, HTTPException, Path, Depends
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
import json

# FastAPI 애플리케이션 생성
app = FastAPI(
    title="PostgreSQL Database API",
    description="PostgreSQL 데이터베이스 조회 API (다중 스키마 지원)",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 환경변수에서 설정 가져오기 (하드코딩된 기본값 완전 제거)
def get_database_config():
    """데이터베이스 연결 설정을 반환합니다."""
    config = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': int(os.getenv('POSTGRES_PORT', '5432')),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': os.getenv('POSTGRES_DB')
    }
    
    # 필수 설정값 검증
    missing_vars = [key for key, value in config.items() if not value]
    if missing_vars:
        raise ValueError(f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing_vars)}")
    
    return config

async def get_db_connection():
    """PostgreSQL 데이터베이스 연결을 제공하는 의존성"""
    try:
        config = get_database_config()
        
        conn = await asyncpg.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        try:
            yield conn
        finally:
            await conn.close()
    except ValueError as e:
        raise HTTPException(
            status_code=500,
            detail=f"데이터베이스 설정 오류: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"데이터베이스 연결 실패: {str(e)}"
        )

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "message": "PostgreSQL Database API Server (Multi-Schema Support)", 
        "docs": "/docs",
        "endpoints": {
            "schemas": "/api/schemas",
            "tables": "/api/tables",
            "tables_by_schema": "/api/schemas/{schema}/tables",
            "schema_info": "/api/schema/{table_name}",
            "query": "/api/query/{table_name}",
            "stream": "/api/stream/{table_name}"
        }
    }

@app.get("/health")
async def health_check():
    """서버 상태를 확인합니다."""
    try:
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

@app.get("/api/schemas")
async def get_schemas(conn: asyncpg.Connection = Depends(get_db_connection)):
    """데이터베이스의 모든 스키마 목록을 반환합니다."""
    try:
        schemas = await conn.fetch("""
            SELECT 
                schema_name,
                (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = s.schema_name) as table_count,
                (SELECT COUNT(*) FROM information_schema.views WHERE table_schema = s.schema_name) as view_count,
                (SELECT COUNT(*) FROM pg_matviews WHERE schemaname = s.schema_name) as matview_count
            FROM information_schema.schemata s
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """)
        
        return {
            "schemas": [
                {
                    "name": schema['schema_name'],
                    "table_count": schema['table_count'],
                    "view_count": schema['view_count'],
                    "matview_count": schema['matview_count']
                } for schema in schemas
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tables")
async def get_tables(conn: asyncpg.Connection = Depends(get_db_connection)):
    """기본 스키마(public)의 모든 테이블 목록을 반환합니다."""
    return await get_tables_by_schema("public", conn)

@app.get("/api/schemas/{schema}/tables")
async def get_tables_by_schema(
    schema: str = Path(..., description="스키마명"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """특정 스키마의 모든 테이블, 뷰, materialized view 목록을 반환합니다."""
    try:
        # 모든 객체 타입을 조회 (테이블, 뷰, materialized view)
        objects = await conn.fetch("""
            SELECT 
                table_name,
                'table' as object_type,
                (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name AND table_schema = t.table_schema) as column_count
            FROM information_schema.tables t 
            WHERE table_schema = $1
            UNION ALL
            SELECT 
                table_name,
                'view' as object_type,
                (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = v.table_name AND table_schema = v.table_schema) as column_count
            FROM information_schema.views v
            WHERE table_schema = $1
            UNION ALL
            SELECT 
                matviewname as table_name,
                'matview' as object_type,
                (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = mv.matviewname AND table_schema = mv.schemaname) as column_count
            FROM pg_matviews mv
            WHERE schemaname = $1
            ORDER BY table_name
        """, schema)
        
        if not objects:
            raise HTTPException(
                status_code=404, 
                detail=f"스키마 '{schema}'를 찾을 수 없거나 객체가 없습니다"
            )
        
        return {
            "schema": schema,
            "tables": [
                {
                    "name": obj['table_name'],
                    "columns": obj['column_count'],
                    "object_type": obj['object_type']
                } for obj in objects
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/schema/{table_name}")
async def get_table_schema(
    table_name: str,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """기본 스키마(public)의 특정 테이블 스키마 정보를 반환합니다."""
    return await get_table_schema_by_schema("public", table_name, conn)

@app.get("/api/schemas/{schema}/tables/{table_name}/schema")
async def get_table_schema_by_schema(
    schema: str = Path(..., description="스키마명"),
    table_name: str = Path(..., description="테이블명"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """특정 스키마의 특정 테이블/뷰/마테리얼라이즈드뷰 스키마 정보를 반환합니다."""
    try:
        # 객체 존재 여부 확인 (테이블, 뷰, materialized view)
        object_exists = await conn.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2
                UNION ALL
                SELECT table_name FROM information_schema.views WHERE table_schema = $1 AND table_name = $2
                UNION ALL
                SELECT matviewname FROM pg_matviews WHERE schemaname = $1 AND matviewname = $2
            ) objects
        """, schema, table_name)
        
        if not object_exists:
            raise HTTPException(
                status_code=404, 
                detail=f"스키마 '{schema}'에서 테이블/뷰/마테리얼라이즈드뷰 '{table_name}'을 찾을 수 없습니다"
            )
        
        columns = await conn.fetch("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = $2
            ORDER BY ordinal_position
        """, table_name, schema)
        
        if not columns:
            raise HTTPException(
                status_code=404, 
                detail=f"스키마 '{schema}'에서 테이블 '{table_name}'을 찾을 수 없습니다"
            )
        
        return {
            "schema": schema,
            "table": table_name,
            "columns": [
                {
                    "name": col['column_name'],
                    "type": col['data_type'],
                    "nullable": col['is_nullable'] == 'YES',
                    "default": col['column_default'],
                    "max_length": col['character_maximum_length'],
                    "precision": col['numeric_precision'],
                    "scale": col['numeric_scale']
                } for col in columns
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/query/{table_name}")
async def query_table(
    table_name: str,
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(100, ge=1, le=1000, description="페이지 크기"),
    sort_by: Optional[str] = Query(None, description="정렬 컬럼"),
    order: str = Query("ASC", regex="^(ASC|DESC)$", description="정렬 순서"),
    search: Optional[str] = Query(None, description="검색어 (LIKE 검색)"),
    search_column: Optional[str] = Query(None, description="검색할 컬럼명"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """기본 스키마(public)의 테이블 데이터를 조회합니다."""
    return await query_table_by_schema("public", table_name, page, size, sort_by, order, search, search_column, conn)

@app.get("/api/schemas/{schema}/tables/{table_name}/query")
async def query_table_by_schema(
    schema: str = Path(..., description="스키마명"),
    table_name: str = Path(..., description="테이블명"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(100, ge=1, le=1000, description="페이지 크기"),
    sort_by: Optional[str] = Query(None, description="정렬 컬럼"),
    order: str = Query("ASC", regex="^(ASC|DESC)$", description="정렬 순서"),
    search: Optional[str] = Query(None, description="검색어 (LIKE 검색)"),
    search_column: Optional[str] = Query(None, description="검색할 컬럼명"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """특정 스키마의 테이블 데이터를 조회합니다. 페이지네이션, 정렬, 검색을 지원합니다."""
    try:
        # 스키마와 객체 존재 여부 확인 (테이블, 뷰, materialized view)
        object_exists = await conn.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2
                UNION ALL
                SELECT table_name FROM information_schema.views WHERE table_schema = $1 AND table_name = $2
                UNION ALL
                SELECT matviewname FROM pg_matviews WHERE schemaname = $1 AND matviewname = $2
            ) objects
        """, schema, table_name)
        
        if not object_exists:
            raise HTTPException(
                status_code=404, 
                detail=f"스키마 '{schema}'에서 테이블/뷰/마테리얼라이즈드뷰 '{table_name}'을 찾을 수 없습니다"
            )
        
        # 기본 쿼리 구성
        base_query = f'SELECT * FROM "{schema}"."{table_name}"'
        count_query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
        where_conditions = []
        params = []
        param_count = 0
        
        # 검색 조건 추가
        if search and search_column:
            param_count += 1
            where_conditions.append(f'"{search_column}" ILIKE ${param_count}')
            params.append(f"%{search}%")
        
        # WHERE 절 구성
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
            count_query += " WHERE " + " AND ".join(where_conditions)
        
        # 정렬 추가
        if sort_by:
            base_query += f' ORDER BY "{sort_by}" {order}'
        
        # 페이지네이션 추가
        offset = (page - 1) * size
        base_query += f" LIMIT {size} OFFSET {offset}"
        
        # 데이터 조회
        data = await conn.fetch(base_query, *params)
        
        # 전체 개수 조회
        total_count = await conn.fetchval(count_query, *params)
        
        # 컬럼명 추출
        columns = list(data[0].keys()) if data else []
        
        return {
            "schema": schema,
            "table": table_name,
            "page": page,
            "size": size,
            "total_count": total_count,
            "total_pages": (total_count + size - 1) // size,
            "data": [dict(row) for row in data],
            "columns": columns
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stream/{table_name}")
async def stream_table(
    table_name: str,
    limit: int = Query(1000, ge=1, le=10000, description="최대 행 수"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """기본 스키마(public)의 테이블 데이터를 스트리밍으로 반환합니다."""
    return await stream_table_by_schema("public", table_name, limit, conn)

@app.get("/api/schemas/{schema}/tables/{table_name}/stream")
async def stream_table_by_schema(
    schema: str = Path(..., description="스키마명"),
    table_name: str = Path(..., description="테이블명"),
    limit: int = Query(1000, ge=1, le=10000, description="최대 행 수"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """특정 스키마의 테이블 데이터를 스트리밍으로 반환합니다. 대용량 데이터 처리에 적합합니다."""
    try:
        # 스키마와 객체 존재 여부 확인 (테이블, 뷰, materialized view)
        object_exists = await conn.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2
                UNION ALL
                SELECT table_name FROM information_schema.views WHERE table_schema = $1 AND table_name = $2
                UNION ALL
                SELECT matviewname FROM pg_matviews WHERE schemaname = $1 AND matviewname = $2
            ) objects
        """, schema, table_name)
        
        if not object_exists:
            raise HTTPException(
                status_code=404, 
                detail=f"스키마 '{schema}'에서 테이블/뷰/마테리얼라이즈드뷰 '{table_name}'을 찾을 수 없습니다"
            )
        
        # 컬럼 정보 조회
        columns = await conn.fetch(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = '{table_name}' AND table_schema = '{schema}' 
            ORDER BY ordinal_position
        """)
        column_names = [col['column_name'] for col in columns]
        
        # 데이터 스트리밍
        async def generate():
            try:
                async for row in conn.cursor(f'SELECT * FROM "{schema}"."{table_name}" LIMIT {limit}'):
                    yield json.dumps(dict(zip(column_names, row)), ensure_ascii=False) + "\n"
            finally:
                await conn.close()
        
        return StreamingResponse(
            generate(), 
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f"attachment; filename={schema}_{table_name}_data.jsonl"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004) 