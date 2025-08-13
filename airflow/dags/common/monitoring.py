"""
Monitoring Module
진행 상황 추적, 성능 메트릭, 알림 기능 등을 담당
"""

import json
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class MonitoringManager:
    """모니터링 관리자 클래스"""

    def __init__(self, task_name: str | None = None, enable_notifications: bool = True):
        """
        초기화

        Args:
            task_name: 태스크 이름
            enable_notifications: 알림 기능 활성화 여부
        """
        self.task_name = task_name or "unknown_task"
        self.enable_notifications = enable_notifications
        self.start_time = None
        self.metrics = {}
        self.checkpoints = []
        self.errors = []
        self.warnings = []

        # 알림 핸들러 등록
        self.notification_handlers = []
        if enable_notifications:
            self._register_default_handlers()

    def _register_default_handlers(self):
        """기본 알림 핸들러 등록"""
        self.add_notification_handler(self._log_notification)
        self.add_notification_handler(self._console_notification)

    def add_notification_handler(self, handler: Callable):
        """알림 핸들러 추가"""
        if handler not in self.notification_handlers:
            self.notification_handlers.append(handler)

    def start_monitoring(self):
        """모니터링 시작"""
        self.start_time = datetime.now()
        self.metrics = {
            "start_time": self.start_time.isoformat(),
            "status": "running",
            "checkpoints": [],
            "errors": [],
            "warnings": [],
            "performance": {},
        }

        logger.info(f"모니터링 시작: {self.task_name}")
        self._send_notification("info", f"모니터링 시작: {self.task_name}")

    def stop_monitoring(self, status: str = "completed"):
        """모니터링 종료"""
        if self.start_time:
            end_time = datetime.now()
            duration = (end_time - self.start_time).total_seconds()

            self.metrics["end_time"] = end_time.isoformat()
            self.metrics["duration"] = duration
            self.metrics["status"] = status

            logger.info(
                f"모니터링 종료: {self.task_name}, 상태: {status}, "
                f"소요시간: {duration:.2f}초"
            )
            self._send_notification(
                "info",
                f"모니터링 종료: {self.task_name}, 상태: {status}, "
                f"소요시간: {duration:.2f}초",
            )

    def add_checkpoint(
        self,
        name: str,
        description: str | None = None,
        data: dict[str, Any] | None = None,
    ):
        """
        체크포인트 추가

        Args:
            name: 체크포인트 이름
            description: 설명
            data: 추가 데이터
        """
        checkpoint = {
            "name": name,
            "description": description,
            "timestamp": datetime.now().isoformat(),
            "data": data or {},
        }

        self.checkpoints.append(checkpoint)
        self.metrics["checkpoints"].append(checkpoint)

        logger.info(f"체크포인트: {name} - {description}")
        self._send_notification("checkpoint", f"체크포인트: {name} - {description}")

    def add_error(self, error: str, details: dict[str, Any] | None = None):
        """
        오류 추가

        Args:
            error: 오류 메시지
            details: 상세 정보
        """
        error_info = {
            "message": error,
            "timestamp": datetime.now().isoformat(),
            "details": details or {},
        }

        self.errors.append(error_info)
        self.metrics["errors"].append(error_info)

        logger.error(f"오류 발생: {error}")
        self._send_notification("error", f"오류 발생: {error}")

    def add_warning(self, warning: str, details: dict[str, Any] | None = None):
        """
        경고 추가

        Args:
            warning: 경고 메시지
            details: 상세 정보
        """
        warning_info = {
            "message": warning,
            "timestamp": datetime.now().isoformat(),
            "details": details or {},
        }

        self.warnings.append(warning_info)
        self.metrics["warnings"].append(warning_info)

        logger.warning(f"경고: {warning}")
        self._send_notification("warning", f"경고: {warning}")

    def add_performance_metric(self, name: str, value: Any, unit: str | None = None):
        """
        성능 메트릭 추가

        Args:
            name: 메트릭 이름
            value: 값
            unit: 단위
        """
        metric = {
            "name": name,
            "value": value,
            "unit": unit,
            "timestamp": datetime.now().isoformat(),
        }

        if "performance" not in self.metrics:
            self.metrics["performance"] = {}

        self.metrics["performance"][name] = metric

        logger.info(f"성능 메트릭: {name} = {value}{unit or ''}")

    def get_progress_percentage(self) -> float:
        """진행률 계산 (체크포인트 기반)"""
        if not self.checkpoints:
            return 0.0

        # 예상 총 체크포인트 수 (하드코딩된 값 또는 동적으로 계산)
        expected_checkpoints = 10  # 기본값

        return min(100.0, (len(self.checkpoints) / expected_checkpoints) * 100)

    def get_summary(self) -> dict[str, Any]:
        """모니터링 요약 정보 반환"""
        if not self.start_time:
            return {"status": "not_started"}

        current_time = datetime.now()
        duration = (current_time - self.start_time).total_seconds()

        summary = {
            "task_name": self.task_name,
            "status": self.metrics.get("status", "running"),
            "start_time": self.start_time.isoformat(),
            "current_time": current_time.isoformat(),
            "duration": duration,
            "checkpoints_count": len(self.checkpoints),
            "errors_count": len(self.errors),
            "warnings_count": len(self.warnings),
            "progress_percentage": self.get_progress_percentage(),
            "latest_checkpoint": self.checkpoints[-1] if self.checkpoints else None,
            "latest_error": self.errors[-1] if self.errors else None,
            "latest_warning": self.warnings[-1] if self.warnings else None,
        }

        return summary

    def _send_notification(
        self, level: str, message: str, data: dict[str, Any] | None = None
    ):
        """알림 전송"""
        notification = {
            "level": level,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "task_name": self.task_name,
            "data": data or {},
        }

        for handler in self.notification_handlers:
            try:
                handler(notification)
            except Exception as e:
                logger.warning(f"알림 핸들러 실행 실패: {e!s}")

    def _log_notification(self, notification: dict[str, Any]):
        """로그 알림 핸들러"""
        level = notification["level"]
        message = notification["message"]

        if level == "error":
            logger.error(message)
        elif level == "warning":
            logger.warning(message)
        elif level == "checkpoint":
            logger.info(message)
        else:
            logger.info(message)

    def _console_notification(self, notification: dict[str, Any]):
        """콘솔 알림 핸들러"""
        level = notification["level"]
        message = notification["message"]
        timestamp = notification["timestamp"]

        # 콘솔에 색상이 있는 출력 (터미널에서만 작동)
        level_colors = {
            "error": "\033[91m",  # 빨간색
            "warning": "\033[93m",  # 노란색
            "checkpoint": "\033[94m",  # 파란색
            "info": "\033[92m",  # 초록색
        }

        color = level_colors.get(level, "\033[0m")
        reset = "\033[0m"

        print(f"{color}[{level.upper()}]{reset} {timestamp} - {message}")

    def export_metrics(self, format: str = "json") -> str:
        """
        메트릭 내보내기

        Args:
            format: 내보내기 형식 ('json', 'text')

        Returns:
            내보낸 메트릭 문자열
        """
        if format == "json":
            return json.dumps(self.metrics, indent=2, ensure_ascii=False, default=str)
        elif format == "text":
            return self._format_metrics_as_text()
        else:
            raise ValueError(f"지원하지 않는 형식: {format}")

    def _format_metrics_as_text(self) -> str:
        """메트릭을 텍스트 형식으로 변환"""
        lines = []
        lines.append(f"=== {self.task_name} 모니터링 리포트 ===")
        lines.append(f"상태: {self.metrics.get('status', 'unknown')}")
        lines.append(f"시작 시간: {self.metrics.get('start_time', 'unknown')}")

        if "end_time" in self.metrics:
            lines.append(f"종료 시간: {self.metrics['end_time']}")
            lines.append(f"소요 시간: {self.metrics.get('duration', 0):.2f}초")

        lines.append(f"체크포인트 수: {len(self.checkpoints)}")
        lines.append(f"오류 수: {len(self.errors)}")
        lines.append(f"경고 수: {len(self.warnings)}")

        if self.checkpoints:
            lines.append("\n=== 체크포인트 ===")
            for cp in self.checkpoints:
                lines.append(f"- {cp['name']}: {cp['description']} ({cp['timestamp']})")

        if self.errors:
            lines.append("\n=== 오류 ===")
            for error in self.errors:
                lines.append(f"- {error['message']} ({error['timestamp']})")

        if self.warnings:
            lines.append("\n=== 경고 ===")
            for warning in self.warnings:
                lines.append(f"- {warning['message']} ({warning['timestamp']})")

        if self.metrics.get("performance"):
            lines.append("\n=== 성능 메트릭 ===")
            for name, metric in self.metrics["performance"].items():
                unit = metric.get("unit", "")
                lines.append(f"- {name}: {metric['value']}{unit}")

        return "\n".join(lines)


class ProgressTracker:
    """진행 상황 추적기 클래스"""

    def __init__(self, total_steps: int, task_name: str | None = None):
        """
        초기화

        Args:
            total_steps: 전체 단계 수
            task_name: 태스크 이름
        """
        self.total_steps = total_steps
        self.task_name = task_name or "unknown_task"
        self.current_step = 0
        self.step_details = {}
        self.start_time = datetime.now()

    def start_step(self, step_name: str, description: str | None = None):
        """
        단계 시작

        Args:
            step_name: 단계 이름
            description: 단계 설명
        """
        self.current_step += 1
        step_info = {
            "name": step_name,
            "description": description,
            "start_time": datetime.now().isoformat(),
            "status": "running",
        }

        self.step_details[step_name] = step_info

        logger.info(f"단계 {self.current_step}/{self.total_steps} 시작: {step_name}")
        if description:
            logger.info(f"설명: {description}")

    def complete_step(self, step_name: str, result: dict[str, Any] | None = None):
        """
        단계 완료

        Args:
            step_name: 단계 이름
            result: 단계 결과
        """
        if step_name in self.step_details:
            self.step_details[step_name]["end_time"] = datetime.now().isoformat()
            self.step_details[step_name]["status"] = "completed"
            self.step_details[step_name]["result"] = result or {}

            logger.info(f"단계 완료: {step_name}")

    def fail_step(
        self, step_name: str, error: str, details: dict[str, Any] | None = None
    ):
        """
        단계 실패

        Args:
            step_name: 단계 이름
            error: 오류 메시지
            details: 상세 정보
        """
        if step_name in self.step_details:
            self.step_details[step_name]["end_time"] = datetime.now().isoformat()
            self.step_details[step_name]["status"] = "failed"
            self.step_details[step_name]["error"] = error
            self.step_details[step_name]["details"] = details or {}

            logger.error(f"단계 실패: {step_name} - {error}")

    def get_progress(self) -> dict[str, Any]:
        """진행 상황 정보 반환"""
        completed_steps = sum(
            1 for step in self.step_details.values() if step["status"] == "completed"
        )
        failed_steps = sum(
            1 for step in self.step_details.values() if step["status"] == "failed"
        )
        running_steps = sum(
            1 for step in self.step_details.values() if step["status"] == "running"
        )

        progress_percentage = (
            (completed_steps / self.total_steps) * 100 if self.total_steps > 0 else 0
        )

        return {
            "total_steps": self.total_steps,
            "completed_steps": completed_steps,
            "failed_steps": failed_steps,
            "running_steps": running_steps,
            "progress_percentage": progress_percentage,
            "current_step": self.current_step,
            "step_details": self.step_details,
        }

    def is_completed(self) -> bool:
        """모든 단계가 완료되었는지 확인"""
        return self.current_step >= self.total_steps and all(
            step["status"] in ["completed", "failed"]
            for step in self.step_details.values()
        )
