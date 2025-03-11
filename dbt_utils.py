# dbt_utils.py
import logging
import os
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

# 로깅 설정 (파일 및 콘솔 출력)
log_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '/usr/local/airflow'), 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'dbt_utils.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# DbtTaskGroup 생성 함수
def create_dbt_task_group(
    dag: DAG,
    group_id: str,
    dbt_project_path: str,
    conn_id: str,
    db_schema: str,
    execution_mode: str = "local",
    test_behavior: str = "after_each",
    select_filters: dict = None,
    exclude_filters: dict = None
) -> DbtTaskGroup:
    """
    DbtTaskGroup을 생성하는 함수.

    Args:
        dag (DAG): Airflow DAG 객체
        group_id (str): TaskGroup의 고유 ID
        dbt_project_path (str): dbt 프로젝트 경로
        conn_id (str): Airflow Connection ID
        db_schema (str): 데이터베이스 스키마 이름
        execution_mode (str): Cosmos 실행 모드 (예: "local", "virtualenv")
        test_behavior (str): 테스트 실행 방식 (예: "after_each", "none")
        select_filters (dict): dbt 모델 선택 필터
        exclude_filters (dict): dbt 모델 제외 필터

    Returns:
        DbtTaskGroup: 생성된 DbtTaskGroup 객체
    """
    logger.info(f"Creating DbtTaskGroup '{group_id}' for project: {dbt_project_path}")

    project_config = ProjectConfig(dbt_project_path=dbt_project_path)
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=conn_id,
            profile_args={"schema": db_schema}
        )
    )
    execution_config = ExecutionConfig(
        execution_mode=ExecutionMode[execution_mode.upper()],
        dbt_executable_path=f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/dbt"
    )
    render_config = RenderConfig(
        test_behavior=TestBehavior[test_behavior.upper()],
        select=select_filters or {},
        exclude=exclude_filters or {}
    )

    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        dag=dag,
        on_failure_callback=lambda context: _handle_failure(context, group_id, conn_id)
    )

# 실패 콜백 함수
def _handle_failure(context, group_id: str, slack_conn_id: str):
    """DbtTaskGroup 실패 시 호출되는 콜백."""
    logger.error(f"DbtTaskGroup '{group_id}' 실패: {context.get('exception')}")
    log_dbt_run_status(group_id, success=False, details=str(context.get('exception')))
    send_slack_notification(
        dag=context['dag'],
        task_id=f"notify_failure_{group_id}",
        slack_conn_id=slack_conn_id,
        message=f"DbtTaskGroup '{group_id}' 실행 실패: {context.get('exception')}"
    )

# 유틸리티 함수: dbt 프로젝트 경로 검증
def validate_dbt_project_path(dbt_project_path: str) -> bool:
    """dbt 프로젝트 경로가 유효한지 확인."""
    required_files = ["dbt_project.yml"]
    try:
        for file in required_files:
            if not os.path.exists(os.path.join(dbt_project_path, file)):
                raise FileNotFoundError(f"{file}이 {dbt_project_path}에 존재하지 않습니다.")
        logger.info(f"dbt 프로젝트 경로 검증 성공: {dbt_project_path}")
        return True
    except Exception as e:
        logger.error(f"dbt 프로젝트 경로 검증 실패: {e}")
        return False

# 유틸리티 함수: Airflow Connection 상태 확인
def check_connection(conn_id: str) -> bool:
    """Airflow Connection이 유효한지 확인."""
    try:
        BaseHook.get_connection(conn_id)
        logger.info(f"Connection '{conn_id}' 확인 성공")
        return True
    except Exception as e:
        logger.error(f"Connection '{conn_id}' 확인 실패: {e}")
        return False

# 유틸리티 함수: 슬랙 알림 전송
def send_slack_notification(
    dag: DAG,
    task_id: str,
    slack_conn_id: str,
    message: str,
    channel: str = "#airflow-notifications"
) -> SlackAPIPostOperator:
    """슬랙으로 알림을 전송."""
    logger.info(f"Preparing Slack notification for {task_id}: {message}")
    return SlackAPIPostOperator(
        task_id=task_id,
        slack_conn_id=slack_conn_id,
        text=message,
        channel=channel,
        dag=dag
    )

# 유틸리티 함수: dbt 실행 상태 로깅
def log_dbt_run_status(group_id: str, success: bool, details: str = ""):
    """dbt 실행 상태를 로깅."""
    status = "성공" if success else "실패"
    logger.info(f"DbtTaskGroup '{group_id}' 실행 {status} - 세부 정보: {details}")

# 유틸리티 함수: 환경 변수에서 설정 값 로드
def load_config_from_env(key: str, default: str = None) -> str:
    """환경 변수에서 설정 값을 로드."""
    value = os.environ.get(key, default)
    if value is None:
        logger.warning(f"환경 변수 '{key}'가 설정되지 않았습니다. 기본값 '{default}' 사용.")
    else:
        logger.info(f"환경 변수 '{key}'에서 값 로드: {value}")
    return value

# 유틸리티 함수: 데이터베이스 상태 모니터링
def monitor_db_status(conn_id: str, schema: str, table_name: str):
    """dbt 실행 후 데이터베이스 상태를 확인."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    try:
        row_count = hook.get_first(f"SELECT COUNT(*) FROM {schema}.{table_name}")[0]
        logger.info(f"테이블 '{schema}.{table_name}'의 행 수: {row_count}")
        return row_count > 0
    except Exception as e:
        logger.error(f"DB 상태 확인 실패: {e}")
        return False
