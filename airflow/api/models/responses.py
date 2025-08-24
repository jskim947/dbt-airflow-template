"""
Response Models
API 응답을 위한 Pydantic 모델들
"""

from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field


class SchemaInfo(BaseModel):
    """스키마 정보 모델"""
    name: str = Field(..., description="스키마명")
    table_count: int = Field(..., description="테이블 개수")
    view_count: int = Field(..., description="뷰 개수")
    matview_count: int = Field(..., description="Materialized View 개수")


class TableInfo(BaseModel):
    """테이블 정보 모델"""
    name: str = Field(..., description="테이블명")
    columns: int = Field(..., description="컬럼 개수")
    object_type: str = Field(..., description="객체 타입 (table, view, matview)")


class ColumnInfo(BaseModel):
    """컬럼 정보 모델"""
    name: str = Field(..., description="컬럼명")
    type: str = Field(..., description="데이터 타입")
    nullable: bool = Field(..., description="NULL 허용 여부")
    default: Optional[str] = Field(None, description="기본값")
    max_length: Optional[int] = Field(None, description="최대 길이")
    precision: Optional[int] = Field(None, description="정밀도")
    scale: Optional[int] = Field(None, description="소수점 자릿수")


class PaginationInfo(BaseModel):
    """페이지네이션 정보 모델"""
    page: int = Field(..., description="현재 페이지")
    size: int = Field(..., description="페이지 크기")
    total_count: int = Field(..., description="전체 개수")
    total_pages: int = Field(..., description="전체 페이지 수")


class QueryResponse(BaseModel):
    """쿼리 응답 모델"""
    schema: str = Field(..., description="스키마명")
    table: str = Field(..., description="테이블명")
    pagination: PaginationInfo = Field(..., description="페이지네이션 정보")
    columns: List[str] = Field(..., description="컬럼명 목록")
    data: List[Dict[str, Any]] = Field(..., description="데이터")


class SchemasResponse(BaseModel):
    """스키마 목록 응답 모델"""
    schemas: List[SchemaInfo] = Field(..., description="스키마 목록")


class TablesResponse(BaseModel):
    """테이블 목록 응답 모델"""
    schema: str = Field(..., description="스키마명")
    tables: List[TableInfo] = Field(..., description="테이블 목록")


class SchemaDetailResponse(BaseModel):
    """테이블 스키마 상세 응답 모델"""
    schema: str = Field(..., description="스키마명")
    table: str = Field(..., description="테이블명")
    columns: List[ColumnInfo] = Field(..., description="컬럼 정보")


class HealthResponse(BaseModel):
    """헬스체크 응답 모델"""
    status: str = Field(..., description="서버 상태")
    database: str = Field(..., description="데이터베이스 상태")
    error: Optional[str] = Field(None, description="에러 메시지")


class RootResponse(BaseModel):
    """루트 엔드포인트 응답 모델"""
    message: str = Field(..., description="서버 메시지")
    docs: str = Field(..., description="API 문서 URL")
    endpoints: Dict[str, str] = Field(..., description="사용 가능한 엔드포인트") 