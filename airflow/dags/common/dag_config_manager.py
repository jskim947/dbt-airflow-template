"""
DAG Configuration Manager Module
모든 DAG가 일관된 형태로 설정을 참조할 수 있는 공통 설정 관리 모듈
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from airflow.models import Variable

logger = logging.getLogger(__name__)


class DAGConfigManager:
    """DAG 설정을 중앙에서 관리하는 클래스"""

    @staticmethod
    def get_dag_config(dag_id: str) -> Dict[str, Any]:
        """
        특정 DAG의 설정을 반환

        Args:
            dag_id: DAG ID

        Returns:
            DAG 설정 딕셔너리
        """
        try:
            dag_configs = json.loads(Variable.get("dag_configs", "{}"))
            return dag_configs.get(dag_id, {})
        except Exception as e:
            logger.warning(f"DAG 설정을 가져올 수 없음: {dag_id} - {str(e)}")
            return {}

    @staticmethod
    def get_connection_config(connection_id: str) -> Dict[str, Any]:
        """
        특정 연결의 설정을 반환 (deprecated - Airflow Connection 사용)

        Args:
            connection_id: 연결 ID

        Returns:
            빈 딕셔너리 (Airflow Connection에서 직접 가져와야 함)
        """
        logger.warning(f"get_connection_config는 deprecated되었습니다. Airflow Connection을 직접 사용하세요: {connection_id}")
        return {}

    @staticmethod
    def get_table_configs(dag_id: str) -> List[Dict[str, Any]]:
        """
        특정 DAG의 테이블 설정을 반환

        Args:
            dag_id: DAG ID

        Returns:
            테이블 설정 목록
        """
        try:
            table_configs = json.loads(Variable.get("table_configs", "{}"))
            return table_configs
        except Exception as e:
            logger.warning(f"테이블 설정을 가져올 수 없음: {dag_id} - {str(e)}")
            return []

    @staticmethod
    def get_monitoring_config() -> Dict[str, Any]:
        """
        모니터링 설정을 반환

        Returns:
            모니터링 설정 딕셔너리
        """
        try:
            monitoring_configs = json.loads(Variable.get("execution_monitoring_configs", "{}"))
            return monitoring_configs
        except Exception as e:
            logger.warning(f"모니터링 설정을 가져올 수 없음: {str(e)}")
            return {}

    @staticmethod
    def get_chunk_mode_config() -> Dict[str, Any]:
        """
        청크 방식 설정을 반환

        Returns:
            청크 방식 설정 딕셔너리
        """
        try:
            chunk_mode_configs = json.loads(Variable.get("chunk_mode_configs", "{}"))
            return chunk_mode_configs
        except Exception as e:
            logger.warning(f"청크 방식 설정을 가져올 수 없음: {str(e)}")
            return {}

    @staticmethod
    def get_dbt_config() -> Dict[str, Any]:
        """
        dbt 설정을 반환

        Returns:
            dbt 설정 딕셔너리
        """
        try:
            dbt_configs = json.loads(Variable.get("dbt_configs", "{}"))
            return dbt_configs
        except Exception as e:
            logger.warning(f"dbt 설정을 가져올 수 없음: {str(e)}")
            return {}

    @staticmethod
    def get_all_dag_configs() -> Dict[str, Any]:
        """
        모든 DAG 설정을 반환

        Returns:
            모든 DAG 설정 딕셔너리
        """
        try:
            dag_configs = json.loads(Variable.get("dag_configs", "{}"))
            return dag_configs
        except Exception as e:
            logger.warning(f"모든 DAG 설정을 가져올 수 없음: {str(e)}")
            return {}

    @staticmethod
    def get_all_connection_configs() -> Dict[str, Any]:
        """
        모든 연결 설정을 반환 (deprecated - Airflow Connection 사용)

        Returns:
            빈 딕셔너리 (Airflow Connection에서 직접 가져와야 함)
        """
        logger.warning("get_all_connection_configs는 deprecated되었습니다. Airflow Connection을 직접 사용하세요.")
        return {}

    @staticmethod
    def is_dag_enabled(dag_id: str) -> bool:
        """
        DAG가 활성화되어 있는지 확인

        Args:
            dag_id: DAG ID

        Returns:
            DAG 활성화 여부
        """
        dag_config = DAGConfigManager.get_dag_config(dag_id)
        return dag_config.get("enabled", False)

    @staticmethod
    def get_dag_connections(dag_id: str) -> Dict[str, str]:
        """
        DAG의 연결 정보를 반환

        Args:
            dag_id: DAG ID

        Returns:
            연결 정보 딕셔너리 (source, target)
        """
        dag_config = DAGConfigManager.get_dag_config(dag_id)
        
        connections = {}
        if "source_connection" in dag_config:
            connections["source"] = dag_config["source_connection"]
        if "target_connection" in dag_config:
            connections["target"] = dag_config["target_connection"]
        if "target_connections" in dag_config:
            connections["targets"] = dag_config["target_connections"]
        
        return connections

    @staticmethod
    def get_dag_tables(dag_id: str) -> List[str]:
        """
        DAG의 테이블 목록을 반환

        Args:
            dag_id: DAG ID

        Returns:
            테이블 목록
        """
        dag_config = DAGConfigManager.get_dag_config(dag_id)
        return dag_config.get("tables", [])

    @staticmethod
    def get_dag_tags(dag_id: str) -> List[str]:
        """
        DAG의 태그를 반환

        Args:
            dag_id: DAG ID

        Returns:
            태그 목록
        """
        dag_config = DAGConfigManager.get_dag_config(dag_id)
        return dag_config.get("tags", [])

    @staticmethod
    def get_dag_schedule(dag_id: str) -> str:
        """
        DAG의 스케줄을 반환

        Args:
            dag_id: DAG ID

        Returns:
            스케줄 문자열
        """
        dag_config = DAGConfigManager.get_dag_config(dag_id)
        return dag_config.get("schedule_interval", "@daily")

    @staticmethod
    def update_dag_execution_status(dag_id: str, status: str, **kwargs) -> bool:
        """
        DAG 실행 상태를 업데이트

        Args:
            dag_id: DAG ID
            status: 실행 상태
            **kwargs: 추가 정보 (last_successful_run, avg_execution_time_minutes 등)

        Returns:
            업데이트 성공 여부
        """
        try:
            dag_configs = json.loads(Variable.get("dag_configs", "{}"))
            
            if dag_id not in dag_configs:
                logger.warning(f"DAG 설정을 찾을 수 없음: {dag_id}")
                return False
            
            # 상태 업데이트
            dag_configs[dag_id]["execution_status"] = status
            dag_configs[dag_id]["last_updated"] = datetime.now().isoformat()
            
            # 추가 정보 업데이트
            for key, value in kwargs.items():
                dag_configs[dag_id][key] = value
            
            # Variable 업데이트
            Variable.set("dag_configs", json.dumps(dag_configs, indent=2))
            
            logger.info(f"DAG 실행 상태 업데이트 성공: {dag_id} -> {status}")
            return True
            
        except Exception as e:
            logger.error(f"DAG 실행 상태 업데이트 실패: {dag_id} - {str(e)}")
            return False

    @staticmethod
    def get_dag_summary() -> Dict[str, Any]:
        """
        모든 DAG의 요약 정보를 반환

        Returns:
            DAG 요약 정보
        """
        try:
            dag_configs = json.loads(Variable.get("dag_configs", "{}"))
            
            summary = {
                "total_dags": len(dag_configs),
                "enabled_dags": 0,
                "active_dags": 0,
                "pending_dags": 0,
                "deprecated_dags": 0,
                "dag_status_summary": {},
                "connection_usage": {}
            }
            
            for dag_id, config in dag_configs.items():
                # 상태별 카운트
                status = config.get("execution_status", "unknown")
                summary["dag_status_summary"][status] = summary["dag_status_summary"].get(status, 0) + 1
                
                if config.get("enabled", False):
                    summary["enabled_dags"] += 1
                
                # 연결 사용량 통계
                source_conn = config.get("source_connection")
                if source_conn:
                    summary["connection_usage"][source_conn] = summary["connection_usage"].get(source_conn, 0) + 1
                
                target_conn = config.get("target_connection")
                if target_conn:
                    summary["connection_usage"][target_conn] = summary["connection_usage"].get(target_conn, 0) + 1
            
            return summary
            
        except Exception as e:
            logger.error(f"DAG 요약 정보를 가져올 수 없음: {str(e)}")
            return {}

    @staticmethod
    def validate_dag_config(dag_id: str) -> Dict[str, Any]:
        """
        DAG 설정의 유효성을 검증

        Args:
            dag_id: DAG ID

        Returns:
            검증 결과 딕셔너리
        """
        dag_config = DAGConfigManager.get_dag_config(dag_id)
        
        if not dag_config:
            return {
                "valid": False,
                "errors": [f"DAG 설정을 찾을 수 없음: {dag_id}"],
                "warnings": []
            }
        
        errors = []
        warnings = []
        
        # 필수 필드 검증
        required_fields = ["dag_id", "description", "schedule_interval", "enabled"]
        for field in required_fields:
            if field not in dag_config:
                errors.append(f"필수 필드 누락: {field}")
        
        # 연결 정보 검증
        if "source_connection" not in dag_config:
            errors.append("소스 연결 정보 누락")
        if "target_connection" not in dag_config and "target_connections" not in dag_config:
            errors.append("타겟 연결 정보 누락")
        
        # 테이블 정보 검증
        if "tables" not in dag_config or not dag_config["tables"]:
            warnings.append("테이블 정보가 없거나 비어있음")
        
        # 태그 검증
        if "tags" not in dag_config or not dag_config["tags"]:
            warnings.append("태그 정보가 없거나 비어있음")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "config": dag_config
        } 

    @staticmethod
    def get_sync_mode_config(sync_mode: str) -> Dict[str, Any]:
        """
        특정 동기화 모드의 설정을 반환

        Args:
            sync_mode: 동기화 모드 (full_sync, incremental_sync)

        Returns:
            동기화 모드 설정 딕셔너리
        """
        try:
            sync_mode_configs = json.loads(Variable.get("sync_mode_configs", "{}"))
            return sync_mode_configs.get(sync_mode, {})
        except Exception as e:
            logger.warning(f"동기화 모드 설정을 가져올 수 없음: {sync_mode} - {str(e)}")
            return {}

    @staticmethod
    def get_all_sync_modes() -> List[str]:
        """
        사용 가능한 모든 동기화 모드를 반환

        Returns:
            동기화 모드 목록
        """
        try:
            sync_mode_configs = json.loads(Variable.get("sync_mode_configs", "{}"))
            return list(sync_mode_configs.keys())
        except Exception as e:
            logger.warning(f"동기화 모드 목록을 가져올 수 없음: {str(e)}")
            return ["full_sync", "incremental_sync"]  # 기본값 