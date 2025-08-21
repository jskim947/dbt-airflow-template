"""
DAG Utilities Module
DAG 설정, 태스크 생성, 연결 검증 등의 공통 기능을 제공
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.database_operations import DatabaseOperations
from common.monitoring import MonitoringManager, ProgressTracker
from common.settings import DAGSettings, BatchSettings
from common.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


class DAGConfigManager:
    """DAG 설정을 관리하는 클래스"""

    @staticmethod
    def get_default_args(
        owner: str = "data_team",
        email: List[str] | None = None,
        retries: int = 2,
        retry_delay_minutes: int = 5,
    ) -> Dict[str, Any]:
        """
        기본 DAG 인수를 반환

        Args:
            owner: DAG 소유자
            email: 실패 시 알림을 받을 이메일 목록
            retries: 재시도 횟수
            retry_delay_minutes: 재시도 간격 (분)

        Returns:
            기본 DAG 인수 딕셔너리
        """
        default_email = ["admin@example.com"] if email is None else email
        
        return {
            "owner": owner,
            "depends_on_past": False,
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": retries,
            "retry_delay": timedelta(minutes=retry_delay_minutes),
            "email": default_email,
        }

    @staticmethod
    def create_dag(
        dag_id: str,
        description: str,
        schedule_interval: str,
        start_date: datetime | None = None,
        default_args: Dict[str, Any] | None = None,
        tags: List[str] | None = None,
        max_active_runs: int = 1,
        catchup: bool = False,
    ) -> DAG:
        """
        DAG를 생성

        Args:
            dag_id: DAG ID
            description: DAG 설명
            schedule_interval: 스케줄 간격
            start_date: 시작 날짜
            default_args: 기본 인수
            tags: 태그 목록
            max_active_runs: 최대 활성 실행 수
            catchup: 캐치업 여부

        Returns:
            생성된 DAG 객체
        """
        if start_date is None:
            start_date = datetime(2024, 1, 1)
        
        if default_args is None:
            default_args = DAGConfigManager.get_default_args()
        
        if tags is None:
            tags = ["postgres", "data-copy", "etl", "refactored"]

        return DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=description,
            schedule_interval=schedule_interval,
            start_date=start_date,
            catchup=catchup,
            tags=tags,
            max_active_runs=max_active_runs,
        )


class ConnectionValidator:
    """데이터베이스 연결 검증을 담당하는 클래스"""

    def __init__(self, source_conn_id: str, target_conn_id: str):
        """
        초기화

        Args:
            source_conn_id: 소스 데이터베이스 연결 ID
            target_conn_id: 타겟 데이터베이스 연결 ID
        """
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id

    def validate_connections_basic(self, **context) -> Dict[str, Any]:
        """
        기본 연결 검증 (EDI DAG용)

        Args:
            **context: Airflow 컨텍스트

        Returns:
            검증 결과 딕셔너리
        """
        try:
            # 모니터링 시작
            monitoring = MonitoringManager(
                f"연결 검증 - {context['task_instance'].task_id}"
            )
            monitoring.start_monitoring()

            # 진행 상황 추적
            progress = ProgressTracker(2, f"연결 검증 - {context['task_instance'].task_id}")

            # 1단계: 소스 데이터베이스 연결 검증
            progress.start_step("소스 연결 검증", f"소스 데이터베이스 {self.source_conn_id} 연결 테스트")
            db_ops = DatabaseOperations(self.source_conn_id, self.target_conn_id)
            source_conn = db_ops.get_source_hook().get_conn()
            source_conn.close()
            progress.complete_step("소스 연결 검증")

            monitoring.add_checkpoint("소스 연결", f"소스 데이터베이스 {self.source_conn_id} 연결 성공")

            # 2단계: 타겟 데이터베이스 연결 검증
            progress.start_step("타겟 연결 검증", f"타겟 데이터베이스 {self.target_conn_id} 연결 테스트")
            target_conn = db_ops.get_target_hook().get_conn()
            target_conn.close()
            progress.complete_step("타겟 연결 검증")

            monitoring.add_checkpoint("타겟 연결", f"타겟 데이터베이스 {self.target_conn_id} 연결 성공")

            # 모니터링 종료
            monitoring.stop_monitoring("completed")

            # 결과 반환
            return {
                "status": "success",
                "message": "모든 데이터베이스 연결 검증 완료",
                "monitoring_summary": monitoring.get_summary(),
                "progress_summary": progress.get_progress(),
            }

        except Exception as e:
            error_msg = f"연결 검증 실패: {e}"
            logger.error(error_msg)
            
            # 모니터링 종료 (실패 상태)
            if 'monitoring' in locals():
                monitoring.add_error(f"연결 검증 실패: {str(e)}")
                monitoring.stop_monitoring("failed")
            
            raise Exception(error_msg)

    def validate_connections_advanced(self, **context) -> Dict[str, Any]:
        """
        고급 연결 검증 (기본 DAG용)

        Args:
            **context: Airflow 컨텍스트

        Returns:
            검증 결과 딕셔너리
        """
        try:
            # 모니터링 시작
            monitoring = MonitoringManager(
                f"연결 검증 - {context['task_instance'].task_id}"
            )
            monitoring.start_monitoring()

            # 데이터베이스 연결 검증
            db_ops = DatabaseOperations(self.source_conn_id, self.target_conn_id)
            connection_results = db_ops.test_connections()

            monitoring.add_checkpoint(
                "연결 검증", "소스 및 타겟 데이터베이스 연결 테스트 완료"
            )

            # 연결 결과 확인
            if not all(connection_results.values()):
                failed_connections = [
                    conn for conn, status in connection_results.items() if not status
                ]
                error_msg = f"연결 실패: {', '.join(failed_connections)}"
                monitoring.add_error(error_msg)
                monitoring.stop_monitoring("failed")
                raise Exception(error_msg)

            monitoring.add_checkpoint("연결 성공", "모든 데이터베이스 연결 성공")
            monitoring.stop_monitoring("completed")

            return {
                "status": "success",
                "connections": connection_results,
                "monitoring_summary": monitoring.get_summary(),
            }

        except Exception as e:
            logger.error(f"연결 검증 실패: {e!s}")
            raise


class TaskFactory:
    """Airflow 태스크 생성을 위한 팩토리 클래스"""

    @staticmethod
    def create_copy_tasks(
        table_configs: List[Dict[str, Any]],
        copy_function: callable,
        dag: DAG,
        task_prefix: str = "copy_table",
        **kwargs
    ) -> List[PythonOperator]:
        """
        복사 태스크들을 생성

        Args:
            table_configs: 테이블 설정 목록
            copy_function: 복사 함수
            dag: DAG 객체
            task_prefix: 태스크 ID 접두사
            **kwargs: 추가 키워드 인수

        Returns:
            생성된 태스크 목록
        """
        copy_tasks = []

        for i, table_config in enumerate(table_configs):
            # 테이블명에서 특수문자 제거하여 태스크 ID 생성
            table_name_clean = table_config['source'].replace('.', '_').replace('-', '_')
            task_id = f"{task_prefix}_{i}_{table_name_clean}"

            task = PythonOperator(
                task_id=task_id,
                python_callable=copy_function,
                op_kwargs={"table_config": table_config, **kwargs},
                dag=dag,
            )

            copy_tasks.append(task)

        return copy_tasks

    @staticmethod
    def create_edi_copy_tasks(
        table_configs: List[Dict[str, Any]],
        copy_function: callable,
        dag: DAG,
        **kwargs
    ) -> List[PythonOperator]:
        """
        EDI 복사 태스크들을 생성

        Args:
            table_configs: EDI 테이블 설정 목록
            copy_function: 복사 함수
            dag: DAG 객체
            **kwargs: 추가 키워드 인수

        Returns:
            생성된 EDI 태스크 목록
        """
        edi_copy_tasks = []

        for i, table_config in enumerate(table_configs):
            # EDI 테이블용 태스크 ID 생성
            table_name_clean = (
                table_config['source']
                .replace('.', '_')
                .replace('m23_', '')
                .replace('-', '_')
            )
            task_id = f"copy_edi_table_{i+1}_{table_name_clean}"
            
            copy_task = PythonOperator(
                task_id=task_id,
                python_callable=copy_function,
                op_kwargs={"table_config": table_config, **kwargs},
                dag=dag,
            )
            
            edi_copy_tasks.append(copy_task)

        return edi_copy_tasks


class TableManager:
    """테이블 관리 관련 공통 기능을 제공하는 클래스"""

    @staticmethod
    def ensure_target_table_exists(table_config: dict[str, Any], **context) -> str:
        """
        타겟 테이블이 존재하는지 확인하고, 없으면 생성
        (DatabaseOperations 클래스의 메서드 사용 권장)

        Args:
            table_config: 테이블 설정
            **context: Airflow 컨텍스트

        Returns:
            성공 메시지
        """
        try:
            # DatabaseOperations 인스턴스를 생성하여 메서드 호출
            from common.database_operations import DatabaseOperations
            
            db_ops = DatabaseOperations(
                ConnectionManager.get_source_connection_id(),
                ConnectionManager.get_target_connection_id()
            )
            return db_ops.ensure_target_table_exists(table_config, **context)
        except Exception as e:
            logger.error(f"ensure_target_table_exists 실패: {e}")
            raise


class DAGDocumentationHelper:
    """DAG 문서화를 돕는 헬퍼 클래스"""

    @staticmethod
    def add_task_documentation(
        task: PythonOperator,
        task_name: str,
        description: str,
        **kwargs
    ) -> None:
        """
        태스크에 문서화 추가

        Args:
            task: 문서화할 태스크
            task_name: 태스크 이름
            description: 태스크 설명
            **kwargs: 추가 문서화 정보
        """
        doc_parts = [f"{task_name}\n\n{description}"]
        
        if kwargs:
            doc_parts.append("\n설정:")
            for key, value in kwargs.items():
                doc_parts.append(f"- {key}: {value}")
        
        task.doc_md = "\n".join(doc_parts)

    @staticmethod
    def add_edi_task_documentation(
        task: PythonOperator,
        table_config: Dict[str, Any],
        index: int
    ) -> None:
        """
        EDI 태스크에 문서화 추가

        Args:
            task: 문서화할 EDI 태스크
            table_config: 테이블 설정
            index: 태스크 인덱스
        """
        DAGDocumentationHelper.add_task_documentation(
            task=task,
            task_name=f"EDI 테이블 복사 태스크 - {table_config['source']}",
            description="""이 태스크는 다음을 수행합니다:
1. EDI 소스 테이블 {source} 스키마 조회
2. 타겟 테이블 {target} 생성 및 스키마 검증
3. EDI 데이터 복사 및 변환
4. 데이터 무결성 검증
5. 모니터링 및 진행 상황 추적""".format(
                source=table_config['source'],
                target=table_config['target']
            ),
            **{
                "기본키": table_config.get('primary_key', []),
                "동기화 모드": table_config.get('sync_mode', 'incremental_sync'),
                "배치 크기": table_config.get('batch_size', 10000),
            }
        ) 