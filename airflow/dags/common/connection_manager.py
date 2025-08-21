"""
Connection Manager Module
Airflow Connection을 우선적으로 활용하고 환경변수를 fallback으로 사용하는 연결 관리 모듈
"""

import logging
import os
from typing import Any, Dict, Optional
import json

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Airflow Connection을 우선적으로 활용하는 연결 관리 클래스"""

    @staticmethod
    def get_connection_info(conn_id: str) -> Dict[str, Any]:
        """
        연결 정보를 반환 (우선순위: Airflow Connection > 환경변수 > 기본값)

        Args:
            conn_id: 연결 ID

        Returns:
            연결 설정 딕셔너리
        """
        # 1. Airflow Connection에서 연결 정보 가져오기 (최우선)
        try:
            hook = PostgresHook(postgres_conn_id=conn_id)
            conn = hook.get_connection(conn_id)
            
            connection_data = {
                "host": conn.host,
                "port": conn.port,
                "database": conn.schema,  # Airflow에서는 schema가 database를 의미
                "user": conn.login,
                "password": conn.password,
                "schema": "public"  # 기본 스키마
            }
            
            logger.info(f"✅ Airflow Connection에서 연결 정보 가져옴: {conn_id}")
            return connection_data
            
        except Exception as e:
            logger.debug(f"Airflow Connection에서 연결 정보 가져오기 실패: {conn_id} - {str(e)}")

        # 2. 환경변수에서 연결 정보 가져오기 (세 번째 우선순위 - 내부 DB만)
        env_host = os.getenv(f"{conn_id.upper()}_HOST")
        env_port = os.getenv(f"{conn_id.upper()}_PORT")
        env_db = os.getenv(f"{conn_id.upper()}_DB")
        env_user = os.getenv(f"{conn_id.upper()}_USER")
        env_password = os.getenv(f"{conn_id.upper()}_PASSWORD")
        
        if env_host and env_port and env_db:
            env_conn_info = {
                "host": env_host,
                "port": int(env_port),
                "database": env_db,
                "user": env_user,
                "password": env_password,
                "schema": "public"
            }
            logger.info(f"✅ 환경변수에서 연결 정보 가져옴: {conn_id}")
            return env_conn_info

        # 3. 기본 연결 정보 반환 (최후 - 내부 DB만)
        default_connections = {
            "fs2_postgres": {
                "host": os.getenv("SOURCE_POSTGRES_HOST", "source_postgres"),
                "port": int(os.getenv("SOURCE_POSTGRES_PORT", "5432")),
                "database": os.getenv("SOURCE_POSTGRES_DB", "source_db"),
                "schema": "public",
            },
            "postgres_default": {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", "15432")),
                "database": os.getenv("POSTGRES_DB", "airflow"),
                "schema": "raw_data",
            },
            "airflow_db": {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", "15432")),
                "database": os.getenv("POSTGRES_DB", "airflow"),
                "schema": "raw_data",
            },
        }

        if conn_id in default_connections:
            logger.info(f"✅ 기본 연결 정보 사용: {conn_id}")
            return default_connections[conn_id]
        
        logger.warning(f"⚠️ 연결 정보를 찾을 수 없음: {conn_id}")
        return {}

    @staticmethod
    def get_connection_summary() -> Dict[str, Any]:
        """
        모든 연결 상태 요약 반환

        Returns:
            연결 상태 요약 딕셔너리
        """
        # 환경변수에서 연결 ID 목록 가져오기
        connection_ids = [
            os.getenv("SOURCE_POSTGRES_CONN_ID", "fs2_postgres"),
            os.getenv("TARGET_POSTGRES_CONN_ID", "postgres_default"),
            os.getenv("AIRFLOW_DB_CONN_ID", "airflow_db"),
        ]
        
        # 추가로 사용 가능한 연결 ID들 (Airflow Connection에서 확인)
        additional_connections = [
            "digitalocean_postgres",  # DigitalOcean PostgreSQL
            "fs2_postgres",           # 팩셋 데이터베이스
            # 추가 연결 ID들...
        ]
        
        all_connections = list(set(connection_ids + additional_connections))
        
        summary = {
            "total_connections": len(all_connections),
            "connections": {}
        }
        
        for conn_id in all_connections:
            try:
                # 연결 테스트 (실제 연결 정보 사용)
                is_connected = ConnectionManager.test_connection(conn_id)
                
                # 연결 정보 가져오기 (실제 연결 정보)
                conn_info = ConnectionManager.get_connection_info(conn_id)
                
                summary["connections"][conn_id] = {
                    "status": "connected" if is_connected else "disconnected",
                    "host": conn_info.get("host", "unknown"),
                    "port": conn_info.get("port", "unknown"),
                    "database": conn_info.get("database", "unknown"),
                    "schema": conn_info.get("schema", "unknown"),
                    "source": "airflow_connection" if is_connected else "fallback"
                }
                
            except Exception as e:
                summary["connections"][conn_id] = {
                    "status": "error",
                    "error": str(e),
                    "source": "unknown"
                }
        
        return summary

    @staticmethod
    def validate_required_connections(required_conn_ids: list) -> Dict[str, bool]:
        """
        필수 연결들의 유효성 검사

        Args:
            required_conn_ids: 필수 연결 ID 목록

        Returns:
            각 연결의 유효성 검사 결과
        """
        results = {}
        
        for conn_id in required_conn_ids:
            try:
                is_valid = ConnectionManager.test_connection(conn_id)
                results[conn_id] = is_valid
                
                if is_valid:
                    logger.info(f"✅ 필수 연결 검증 성공: {conn_id}")
                else:
                    logger.error(f"❌ 필수 연결 검증 실패: {conn_id}")
                    
            except Exception as e:
                logger.error(f"❌ 필수 연결 검증 중 오류: {conn_id} - {str(e)}")
                results[conn_id] = False
        
        return results

    @staticmethod
    def get_source_connection_id() -> str:
        """
        소스 연결 ID를 반환

        Returns:
            소스 연결 ID
        """
        return os.getenv("SOURCE_POSTGRES_CONN_ID", "fs2_postgres")

    @staticmethod
    def get_target_connection_id() -> str:
        """
        타겟 연결 ID를 반환

        Returns:
            타겟 연결 ID
        """
        return os.getenv("TARGET_POSTGRES_CONN_ID", "postgres_default") 