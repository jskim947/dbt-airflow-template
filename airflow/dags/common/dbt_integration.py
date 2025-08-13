"""
DBT Integration Module
dbt 프로젝트 검증, 스냅샷 실행, 오류 처리 등을 담당
"""

import logging
import os
import subprocess
from typing import Any

logger = logging.getLogger(__name__)


class DBTIntegration:
    """dbt 통합을 담당하는 클래스"""

    def __init__(self, dbt_project_path: str = "/opt/airflow/dbt"):
        """
        초기화

        Args:
            dbt_project_path: dbt 프로젝트 경로
        """
        self.dbt_project_path = dbt_project_path
        # ✅ 수정: profiles.yml을 dbt 프로젝트 디렉토리에서 찾기
        self.dbt_profiles_path = dbt_project_path

        # dbt 명령어 경로 확인
        self.dbt_cmd = self._find_dbt_command()

    def _find_dbt_command(self) -> str:
        """dbt 명령어 경로 찾기"""
        try:
            # dbt 명령어가 PATH에 있는지 확인
            result = subprocess.run(["which", "dbt"], capture_output=True, text=True)
            if result.returncode == 0:
                return "dbt"

            # dbt-core가 설치되어 있는지 확인
            result = subprocess.run(
                ["which", "dbt-core"], capture_output=True, text=True
            )
            if result.returncode == 0:
                return "dbt-core"

            # pip로 설치된 dbt 확인
            result = subprocess.run(
                ["python", "-m", "dbt"], capture_output=True, text=True
            )
            if result.returncode == 0:
                return "python -m dbt"

            logger.warning(
                "dbt 명령어를 찾을 수 없습니다. "
                "'dbt' 또는 'python -m dbt'를 사용합니다."
            )
            return "dbt"

        except Exception as e:
            logger.warning(f"dbt 명령어 검색 중 오류: {e!s}")
            return "dbt"

    def validate_dbt_project(self) -> dict[str, Any]:
        """
        dbt 프로젝트 유효성 검증

        Returns:
            검증 결과
        """
        try:
            if not os.path.exists(self.dbt_project_path):
                return {
                    "is_valid": False,
                    "error": (
                        f"dbt 프로젝트 경로가 존재하지 않습니다: "
                        f"{self.dbt_project_path}"
                    ),
                }

            # dbt_project.yml 파일 확인
            dbt_project_yml = os.path.join(self.dbt_project_path, "dbt_project.yml")
            if not os.path.exists(dbt_project_yml):
                return {
                    "is_valid": False,
                    "error": (
                        f"dbt_project.yml 파일이 존재하지 않습니다: "
                        f"{dbt_project_yml}"
                    ),
                }

            # profiles.yml 파일 확인
            profiles_yml = os.path.join(self.dbt_profiles_path, "profiles.yml")
            if not os.path.exists(profiles_yml):
                return {
                    "is_valid": False,
                    "error": f"profiles.yml 파일이 존재하지 않습니다: {profiles_yml}",
                }

            # dbt debug 실행
            debug_result = self._run_dbt_command(["debug"])
            if debug_result["return_code"] != 0:
                return {
                    "is_valid": False,
                    "error": f"dbt debug 실패: {debug_result['stderr']}",
                }

            # dbt parse 실행
            parse_result = self._run_dbt_command(["parse"])
            if parse_result["return_code"] != 0:
                return {
                    "is_valid": False,
                    "error": f"dbt parse 실패: {parse_result['stderr']}",
                }

            return {
                "is_valid": True,
                "message": f"dbt 프로젝트 검증 성공: {self.dbt_project_path}",
                "debug_output": debug_result["stdout"],
                "parse_output": parse_result["stdout"],
            }

        except Exception as e:
            return {"is_valid": False, "error": f"dbt 프로젝트 검증 중 오류: {e!s}"}

    def _run_dbt_command(
        self, args: list[str], working_dir: str | None = None
    ) -> dict[str, Any]:
        """
        dbt 명령어 실행

        Args:
            args: dbt 명령어 인수 리스트
            working_dir: 작업 디렉토리 (None이면 dbt 프로젝트 경로 사용)

        Returns:
            명령어 실행 결과
        """
        if working_dir is None:
            working_dir = self.dbt_project_path

        try:
            # 환경 변수 설정
            env = os.environ.copy()
            env["DBT_PROFILES_DIR"] = self.dbt_profiles_path
            env["DBT_PROJECT_DIR"] = self.dbt_project_path

            # dbt 명령어 실행
            cmd = [self.dbt_cmd, *args]
            logger.info(
                f"dbt 명령어 실행: {' '.join(cmd)} (작업 디렉토리: {working_dir})"
            )

            result = subprocess.run(
                cmd,
                cwd=working_dir,
                env=env,
                capture_output=True,
                text=True,
                timeout=300,  # 5분 타임아웃
            )

            return {
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "command": " ".join(cmd),
            }

        except subprocess.TimeoutExpired:
            return {
                "return_code": -1,
                "stdout": "",
                "stderr": "dbt 명령어 실행 시간 초과 (5분)",
                "command": " ".join(cmd),
            }
        except Exception as e:
            return {
                "return_code": -1,
                "stdout": "",
                "stderr": f"dbt 명령어 실행 중 오류: {e!s}",
                "command": " ".join(cmd),
            }

    def run_dbt_snapshot(
        self, models: list[str] | None = None, select: str | None = None
    ) -> dict[str, Any]:
        """
        dbt 스냅샷 실행

        Args:
            models: 실행할 모델 리스트 (None이면 모든 모델)
            select: 선택 조건 문자열

        Returns:
            스냅샷 실행 결과
        """
        try:
            # dbt 스냅샷 명령어 인수 구성
            args = ["snapshot"]

            if models:
                args.extend(["--models", *models])

            if select:
                args.extend(["--select", select])

            # 스냅샷 실행
            logger.info(f"dbt 스냅샷 실행 시작: {' '.join(args)}")
            result = self._run_dbt_command(args)

            if result["return_code"] == 0:
                return {
                    "status": "success",
                    "message": f"dbt 스냅샷 실행 성공: {' '.join(args)}",
                    "stdout": result["stdout"],
                    "command": result["command"],
                }
            else:
                return {
                    "status": "error",
                    "message": f"dbt 스냅샷 실행 실패: {' '.join(args)}",
                    "stderr": result["stderr"],
                    "command": result["command"],
                }

        except Exception as e:
            return {
                "status": "error",
                "message": f"dbt 스냅샷 실행 중 오류: {e!s}",
                "error": str(e),
            }

    def run_dbt_run(
        self,
        models: list[str] | None = None,
        select: str | None = None,
        full_refresh: bool = False,
    ) -> dict[str, Any]:
        """
        dbt run 실행

        Args:
            models: 실행할 모델 리스트 (None이면 모든 모델)
            select: 선택 조건 문자열
            full_refresh: 전체 새로고침 여부

        Returns:
            run 실행 결과
        """
        try:
            # dbt run 명령어 인수 구성
            args = ["run"]

            if models and isinstance(models, list) and len(models) > 0:
                args.extend(["--models", *models])

            if select:
                args.extend(["--select", select])

            if full_refresh:
                args.append("--full-refresh")

            # run 실행
            logger.info(f"dbt run 실행 시작: {' '.join(args)}")
            result = self._run_dbt_command(args)

            if result["return_code"] == 0:
                return {
                    "status": "success",
                    "message": f"dbt run 실행 성공: {' '.join(args)}",
                    "stdout": result["stdout"],
                    "command": result["command"],
                }
            else:
                return {
                    "status": "error",
                    "message": f"dbt run 실행 실패: {' '.join(args)}",
                    "stderr": result["stderr"],
                    "command": result["command"],
                }

        except Exception as e:
            return {
                "status": "error",
                "message": f"dbt run 실행 중 오류: {e!s}",
                "error": str(e),
            }

    def run_dbt_test(
        self, models: list[str] | None = None, select: str | None = None
    ) -> dict[str, Any]:
        """
        dbt test 실행

        Args:
            models: 테스트할 모델 리스트 (None이면 모든 모델)
            select: 선택 조건 문자열

        Returns:
            test 실행 결과
        """
        try:
            # dbt test 명령어 인수 구성
            args = ["test"]

            if models:
                args.extend(["--models", *models])

            if select:
                args.extend(["--select", select])

            # test 실행
            logger.info(f"dbt test 실행 시작: {' '.join(args)}")
            result = self._run_dbt_command(args)

            if result["return_code"] == 0:
                return {
                    "status": "success",
                    "message": f"dbt test 실행 성공: {' '.join(args)}",
                    "stdout": result["stdout"],
                    "command": result["command"],
                }
            else:
                return {
                    "status": "error",
                    "message": f"dbt test 실행 실패: {' '.join(args)}",
                    "stderr": result["stderr"],
                    "command": result["command"],
                }

        except Exception as e:
            return {
                "status": "error",
                "message": f"dbt test 실행 중 오류: {e!s}",
                "error": str(e),
            }

    def get_dbt_models_status(self) -> dict[str, Any]:
        """
        dbt 모델 상태 조회

        Returns:
            모델 상태 정보
        """
        try:
            # dbt ls 명령어로 모델 목록 조회
            result = self._run_dbt_command(["ls"])

            if result["return_code"] == 0:
                models = result["stdout"].strip().split("\n")
                models = [model.strip() for model in models if model.strip()]

                return {
                    "status": "success",
                    "models": models,
                    "count": len(models),
                    "message": f"dbt 모델 {len(models)}개 조회 성공",
                }
            else:
                return {
                    "status": "error",
                    "message": f"dbt 모델 목록 조회 실패: {result['stderr']}",
                    "error": result["stderr"],
                }

        except Exception as e:
            return {
                "status": "error",
                "message": f"dbt 모델 상태 조회 중 오류: {e!s}",
                "error": str(e),
            }

    def run_dbt_cleanup(self) -> dict[str, Any]:
        """
        dbt 정리 작업 실행

        Returns:
            정리 작업 결과
        """
        try:
            # dbt clean 명령어 실행
            logger.info("dbt 정리 작업 시작")
            result = self._run_dbt_command(["clean"])

            if result["return_code"] == 0:
                return {
                    "status": "success",
                    "message": "dbt 정리 작업 성공",
                    "stdout": result["stdout"],
                }
            else:
                return {
                    "status": "error",
                    "message": f"dbt 정리 작업 실패: {result['stderr']}",
                    "stderr": result["stderr"],
                }

        except Exception as e:
            return {
                "status": "error",
                "message": f"dbt 정리 작업 중 오류: {e!s}",
                "error": str(e),
            }

    def execute_dbt_pipeline(self, pipeline_config: dict[str, Any]) -> dict[str, Any]:
        """
        dbt 파이프라인 실행 (스냅샷 -> run -> test)

        Args:
            pipeline_config: 파이프라인 설정

        Returns:
            파이프라인 실행 결과
        """
        try:
            results = {}

            # 1. dbt 프로젝트 검증
            logger.info("1단계: dbt 프로젝트 검증")
            validation_result = self.validate_dbt_project()
            results["validation"] = validation_result

            if not validation_result["is_valid"]:
                return {
                    "status": "error",
                    "message": "dbt 프로젝트 검증 실패",
                    "results": results,
                }

            # 2. dbt 스냅샷 실행
            if pipeline_config.get("run_snapshot", True):
                logger.info("2단계: dbt 스냅샷 실행")

                # 스냅샷 실행 전 소스 테이블 존재 확인
                source_check_result = self._check_source_tables()
                if not source_check_result["is_valid"]:
                    logger.warning(
                        f"소스 테이블 확인 실패: {source_check_result['message']}"
                    )
                    # 스냅샷 실행을 건너뛰고 계속 진행
                    results["snapshot"] = {
                        "status": "skipped",
                        "message": f"소스 테이블 문제로 스킵: {source_check_result['message']}",
                        "warning": source_check_result["message"],
                    }
                else:
                    snapshot_result = self.run_dbt_snapshot(
                        models=pipeline_config.get("snapshot_models"),
                        select=pipeline_config.get("snapshot_select"),
                    )
                    results["snapshot"] = snapshot_result

                    if snapshot_result["status"] == "error":
                        return {
                            "status": "error",
                            "message": "dbt 스냅샷 실행 실패",
                            "results": results,
                        }

            # 3. dbt run 실행
            if pipeline_config.get("run_models", True):
                logger.info("3단계: dbt run 실행")
                run_result = self.run_dbt_run(
                    models=pipeline_config.get("run_models"),
                    select=pipeline_config.get("run_select"),
                    full_refresh=pipeline_config.get("full_refresh", False),
                )
                results["run"] = run_result

                if run_result["status"] == "error":
                    return {
                        "status": "error",
                        "message": "dbt run 실행 실패",
                        "results": results,
                    }

            # 4. dbt test 실행
            if pipeline_config.get("run_tests", True):
                logger.info("4단계: dbt test 실행")
                test_result = self.run_dbt_test(
                    models=pipeline_config.get("test_models"),
                    select=pipeline_config.get("test_select"),
                )
                results["test"] = test_result

                if test_result["status"] == "error":
                    return {
                        "status": "error",
                        "message": "dbt test 실행 실패",
                        "results": results,
                    }

            # 5. 정리 작업
            if pipeline_config.get("cleanup", True):
                logger.info("5단계: dbt 정리 작업")
                cleanup_result = self.run_dbt_cleanup()
                results["cleanup"] = cleanup_result

            return {
                "status": "success",
                "message": "dbt 파이프라인 실행 완료",
                "results": results,
            }

        except Exception as e:
            return {
                "status": "error",
                "message": f"dbt 파이프라인 실행 중 오류: {e!s}",
                "error": str(e),
            }

    def _check_source_tables(self) -> dict[str, Any]:
        """
        소스 테이블 존재 여부 확인

        Returns:
            소스 테이블 확인 결과
        """
        try:
            # dbt ls 명령어로 소스 테이블 확인
            result = self._run_dbt_command(["ls", "--select", "source:*"])

            if result["return_code"] == 0:
                return {
                    "is_valid": True,
                    "message": "소스 테이블 확인 성공",
                    "stdout": result["stdout"],
                }
            else:
                return {
                    "is_valid": False,
                    "message": f"소스 테이블 확인 실패: {result['stderr']}",
                    "stderr": result["stderr"],
                }

        except Exception as e:
            return {
                "is_valid": False,
                "message": f"소스 테이블 확인 중 오류: {e!s}",
                "error": str(e),
            }
