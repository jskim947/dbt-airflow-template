"""
Database Router
PostgreSQL 데이터베이스 조회를 위한 API 엔드포인트들
"""

import json
from typing import Optional
from fastapi import APIRouter, Depends, Query, Path, HTTPException
from fastapi.responses import StreamingResponse
import asyncpg

from airflow.api.dependencies import get_db_connection
from airflow.api.models import (
    SchemasResponse,
    TablesResponse,
    SchemaDetailResponse,
    QueryResponse,
    SchemaInfo,
    TableInfo,
    ColumnInfo,
    PaginationInfo,
)
from airflow.api.config import APIConfig

router = APIRouter(prefix="/api", tags=["database"])


@router.get("/schemas", response_model=SchemasResponse)
async def get_schemas(conn: asyncpg.Connection = Depends(get_db_connection)):
    """데이터베이스의 모든 스키마 목록을 반환합니다."""
    try:
        schemas = await conn.fetch("""
            SELECT 
                schema_name,
                (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = s.schema_name) as table_count
            FROM information_schema.schemata s
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """)
        
        return SchemasResponse(
            schemas=[
                SchemaInfo(
                    name=schema['schema_name'],
                    table_count=schema['table_count']
                ) for schema in schemas
            ]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables", response_model=TablesResponse)
async def get_tables(conn: asyncpg.Connection = Depends(get_db_connection)):
    """기본 스키마(public)의 모든 테이블 목록을 반환합니다."""
    return await get_tables_by_schema("public", conn)


@router.get("/schemas/{schema}/tables", response_model=TablesResponse)
async def get_tables_by_schema(
    schema: str = Path(..., description="스키마명"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """특정 스키마의 모든 테이블 목록을 반환합니다."""
    try:
        tables = await conn.fetch("""
            SELECT 
                table_name,
                (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name AND table_schema = t.table_schema) as column_count
            FROM information_schema.tables t 
            WHERE table_schema = $1
            ORDER BY table_name
        """, schema)
        
        if not tables:
            raise HTTPException(
                status_code=404, 
                detail=f"스키마 '{schema}'를 찾을 수 없거나 테이블이 없습니다"
            )
        
        return TablesResponse(
            schema=schema,
            tables=[
                TableInfo(
                    name=table['table_name'],
                    columns=table['column_count']
                ) for table in tables
            ]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema/{table_name}", response_model=SchemaDetailResponse)
async def get_table_schema(
    table_name: str,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """기본 스키마(public)의 특정 테이블 스키마 정보를 반환합니다."""
    return await get_table_schema_by_schema("public", table_name, conn)


@router.get("/schemas/{schema}/tables/{table_name}/schema", response_model=SchemaDetailResponse)
async def get_table_schema_by_schema(
    schema: str = Path(..., description="스키마명"),
    table_name: str = Path(..., description="테이블명"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """특정 스키마의 특정 테이블 스키마 정보를 반환합니다."""
    try:
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
        
        return SchemaDetailResponse(
            schema=schema,
            table=table_name,
            columns=[
                ColumnInfo(
                    name=col['column_name'],
                    type=col['data_type'],
                    nullable=col['is_nullable'] == 'YES',
                    default=col['column_default'],
                    max_length=col['character_maximum_length'],
                    precision=col['numeric_precision'],
                    scale=col['numeric_scale']
                ) for col in columns
            ]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/query/{table_name}", response_model=QueryResponse)
async def query_table(
    table_name: str,
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(APIConfig.DEFAULT_PAGE_SIZE, ge=1, le=APIConfig.MAX_PAGE_SIZE, description="페이지 크기"),
    sort_by: Optional[str] = Query(None, description="정렬 컬럼"),
    order: str = Query("ASC", regex="^(ASC|DESC)$", description="정렬 순서"),
    search: Optional[str] = Query(None, description="검색어 (LIKE 검색)"),
    search_column: Optional[str] = Query(None, description="검색할 컬럼명"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """기본 스키마(public)의 테이블 데이터를 조회합니다."""
    return await query_table_by_schema("public", table_name, page, size, sort_by, order, search, search_column, conn)


@router.get("/schemas/{schema}/tables/{table_name}/query", response_model=QueryResponse)
async def query_table_by_schema(
    schema: str = Path(..., description="스키마명"),
    table_name: str = Path(..., description="테이블명"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(APIConfig.DEFAULT_PAGE_SIZE, ge=1, le=APIConfig.MAX_PAGE_SIZE, description="페이지 크기"),
    sort_by: Optional[str] = Query(None, description="정렬 컬럼"),
    order: str = Query("ASC", regex="^(ASC|DESC)$", description="정렬 순서"),
    search: Optional[str] = Query(None, description="검색어 (LIKE 검색)"),
    search_column: Optional[str] = Query(None, description="검색할 컬럼명"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """특정 스키마의 테이블 데이터를 조회합니다. 페이지네이션, 정렬, 검색을 지원합니다."""
    try:
        # 스키마와 테이블 존재 여부 확인
        table_exists = await conn.fetchval("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
        """, schema, table_name)
        
        if not table_exists:
            raise HTTPException(
                status_code=404, 
                detail=f"스키마 '{schema}'에서 테이블 '{table_name}'을 찾을 수 없습니다"
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
        
        return QueryResponse(
            schema=schema,
            table=table_name,
            pagination=PaginationInfo(
                page=page,
                size=size,
                total_count=total_count,
                total_pages=(total_count + size - 1) // size
            ),
            columns=columns,
            data=[dict(row) for row in data]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stream/{table_name}")
async def stream_table(
    table_name: str,
    limit: int = Query(1000, ge=1, le=APIConfig.MAX_STREAM_LIMIT, description="최대 행 수"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """기본 스키마(public)의 테이블 데이터를 스트리밍으로 반환합니다."""
    return await stream_table_by_schema("public", table_name, limit, conn)


@router.get("/schemas/{schema}/tables/{table_name}/stream")
async def stream_table_by_schema(
    schema: str = Path(..., description="스키마명"),
    table_name: str = Path(..., description="테이블명"),
    limit: int = Query(1000, ge=1, le=APIConfig.MAX_STREAM_LIMIT, description="최대 행 수"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """특정 스키마의 테이블 데이터를 스트리밍으로 반환합니다. 대용량 데이터 처리에 적합합니다."""
    try:
        # 스키마와 테이블 존재 여부 확인
        table_exists = await conn.fetchval("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
        """, schema, table_name)
        
        if not table_exists:
            raise HTTPException(
                status_code=404, 
                detail=f"스키마 '{schema}'에서 테이블 '{table_name}'을 찾을 수 없습니다"
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