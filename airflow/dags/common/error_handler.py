"""
Error Handler Module
공통 에러 핸들링 및 로깅을 위한 모듈
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class ErrorHandler:
    """공통 에러 핸들링을 위한 클래스"""

    def __init__(self, context: Dict[str, Any]):
        """
        초기화

        Args:
            context: Airflow 컨텍스트
        """
        self.context = context
        self.task_instance = context.get('task_instance')
        self.dag_id = context.get('dag_id', 'unknown')
        self.task_id = context.get('task_id', 'unknown')

    def handle_error(self, operation: str, error: Exception, **kwargs) -> None:
        """
        에러를 처리하고 로깅

        Args:
            operation: 수행 중이던 작업명
            error: 발생한 에러
            **kwargs: 추가 정보
        """
        error_msg = f"{operation} 실패 - DAG: {self.dag_id}, Task: {self.task_id}, 오류: {error}"
        
        # 상세 로깅
        logger.error(error_msg, exc_info=True)
        
        # 추가 정보 로깅
        if kwargs:
            logger.error(f"추가 정보: {kwargs}")
        
        # 에러를 다시 발생시킴
        raise Exception(error_msg)

    def handle_validation_error(self, validation_type: str, details: str, **kwargs) -> None:
        """
        검증 에러를 처리

        Args:
            validation_type: 검증 유형
            details: 검증 실패 상세 내용
            **kwargs: 추가 정보
        """
        error_msg = f"{validation_type} 검증 실패: {details}"
        logger.warning(error_msg)
        
        if kwargs:
            logger.warning(f"검증 실패 상세: {kwargs}")

    def handle_connection_error(self, connection_type: str, connection_id: str, error: Exception) -> None:
        """
        연결 에러를 처리

        Args:
            connection_type: 연결 유형 (source/target)
            connection_id: 연결 ID
            error: 연결 에러
        """
        error_msg = f"{connection_type} 데이터베이스 연결 실패: {connection_id}, 오류: {error}"
        logger.error(error_msg, exc_info=True)
        
        # 연결 에러는 즉시 실패 처리
        raise Exception(error_msg)

    def handle_data_error(self, operation: str, table_name: str, error: Exception, **kwargs) -> None:
        """
        데이터 처리 에러를 처리

        Args:
            operation: 데이터 처리 작업명
            table_name: 테이블명
            error: 데이터 에러
            **kwargs: 추가 정보
        """
        error_msg = f"{operation} 실패 - 테이블: {table_name}, 오류: {error}"
        logger.error(error_msg, exc_info=True)
        
        if kwargs:
            logger.error(f"데이터 처리 실패 상세: {kwargs}")
        
        raise Exception(error_msg)

    def log_warning(self, message: str, **kwargs) -> None:
        """
        경고 메시지 로깅

        Args:
            message: 경고 메시지
            **kwargs: 추가 정보
        """
        warning_msg = f"경고 - DAG: {self.dag_id}, Task: {self.task_id}, {message}"
        logger.warning(warning_msg)
        
        if kwargs:
            logger.warning(f"경고 상세: {kwargs}")

    def log_info(self, message: str, **kwargs) -> None:
        """
        정보 메시지 로깅

        Args:
            message: 정보 메시지
            **kwargs: 추가 정보
        """
        info_msg = f"정보 - DAG: {self.dag_id}, Task: {self.task_id}, {message}"
        logger.info(info_msg)
        
        if kwargs:
            logger.info(f"정보 상세: {kwargs}") 