#!/usr/bin/env python3
"""
FastAPI 테스트 스크립트
PostgreSQL 데이터베이스 조회 API를 테스트합니다.
"""

import asyncio
import aiohttp
import json
from typing import Dict, Any


class APITester:
    """FastAPI 테스트 클래스"""
    
    def __init__(self, base_url: str = "http://localhost:8004"):
        self.base_url = base_url
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def test_endpoint(self, endpoint: str, method: str = "GET", **kwargs) -> Dict[str, Any]:
        """엔드포인트 테스트"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method.upper() == "GET":
                async with self.session.get(url, **kwargs) as response:
                    return {
                        "status": response.status,
                        "data": await response.json() if response.content_type == "application/json" else await response.text(),
                        "headers": dict(response.headers)
                    }
            else:
                async with self.session.post(url, **kwargs) as response:
                    return {
                        "status": response.status,
                        "data": await response.json() if response.content_type == "application/json" else await response.text(),
                        "headers": dict(response.headers)
                    }
        except Exception as e:
            return {"error": str(e)}
    
    async def run_tests(self):
        """모든 테스트 실행"""
        print("🚀 FastAPI 테스트 시작...\n")
        
        # 1. 루트 엔드포인트 테스트
        print("1. 루트 엔드포인트 테스트")
        result = await self.test_endpoint("/")
        print(f"   상태: {result.get('status', 'N/A')}")
        if 'data' in result and isinstance(result['data'], dict):
            print(f"   메시지: {result['data'].get('message', 'N/A')}")
        print()
        
        # 2. 헬스체크 테스트
        print("2. 헬스체크 테스트")
        result = await self.test_endpoint("/health")
        print(f"   상태: {result.get('status', 'N/A')}")
        if 'data' in result and isinstance(result['data'], dict):
            print(f"   서버 상태: {result['data'].get('status', 'N/A')}")
            print(f"   DB 상태: {result['data'].get('database', 'N/A')}")
        print()
        
        # 3. 스키마 목록 테스트
        print("3. 스키마 목록 테스트")
        result = await self.test_endpoint("/api/schemas")
        print(f"   상태: {result.get('status', 'N/A')}")
        if 'data' in result and isinstance(result['data'], dict):
            schemas = result['data'].get('schemas', [])
            print(f"   스키마 개수: {len(schemas)}")
            for schema in schemas[:3]:  # 처음 3개만 출력
                print(f"     - {schema.get('name', 'N/A')}: {schema.get('table_count', 'N/A')}개 테이블")
        print()
        
        # 4. 테이블 목록 테스트 (public 스키마)
        print("4. 테이블 목록 테스트 (public 스키마)")
        result = await self.test_endpoint("/api/tables")
        print(f"   상태: {result.get('status', 'N/A')}")
        if 'data' in result and isinstance(result['data'], dict):
            tables = result['data'].get('tables', [])
            print(f"   테이블 개수: {len(tables)}")
            for table in tables[:3]:  # 처음 3개만 출력
                print(f"     - {table.get('name', 'N/A')}: {table.get('columns', 'N/A')}개 컬럼")
        print()
        
        # 5. API 문서 URL 출력
        print("5. API 문서")
        print(f"   Swagger UI: {self.base_url}/docs")
        print(f"   ReDoc: {self.base_url}/redoc")
        print()
        
        print("✅ 테스트 완료!")


async def main():
    """메인 함수"""
    async with APITester() as tester:
        await tester.run_tests()


if __name__ == "__main__":
    asyncio.run(main()) 