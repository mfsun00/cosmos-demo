# example_dbt_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
from dbt_utils import (
    create_dbt_task_group,
    validate_dbt_project_path,
    check_connection,
    send_slack_notification,
    log_dbt_run_status,
    load_config_from_env,
    monitor_db_status
)

# DAG 정의
with DAG(
    dag_id="example_dbt_dag",
    start_date=datetime(2025, 3, 11),
    schedule="@daily",
    catchup=False
) as dag:
    # 환경 변수에서 설정 로드
    dbt_project_path = load_config_from_env("DBT_PROJECT_PATH", "/usr/local/airflow/dags/dbt/my_project")
    slack_conn_id = load_config_from_env("SLACK_CONN_ID", "slack_default")

    # 경로 및 연결 검증
    if validate_dbt_project_path(dbt_project_path) and check_connection("postgres_default"):
        # DbtTaskGroup 생성
        dbt_task_group = create_dbt_task_group(
            dag=dag,
            group_id="my_dbt_transform",
            dbt_project_path=dbt_project_path,
            conn_id="postgres_default",
            db_schema="public",
            execution_mode="local",
            test_behavior="after_each",
            select_filters={"tags": ["daily"]},
            exclude_filters={"tags": ["exclude_test"]}
        )

        # 성공 시 알림 및 상태 로깅
        success_notification = send_slack_notification(
            dag=dag,
            task_id="notify_success",
            slack_conn_id=slack_conn_id,
            message=f"DbtTaskGroup '{dbt_task_group.group_id}' 실행 성공!"
        )

        # DB 상태 모니터링 태스크
        monitor_task = PythonOperator(
            task_id="monitor_db_status",
            python_callable=monitor_db_status,
            op_kwargs={
                "conn_id": "postgres_default",
                "schema": "public",
                "table_name": "my_table"  # 실제 테이블 이름으로 변경 필요
            },
            dag=dag
        )

        # 성공 시 상태 로깅 (간단히 수동 호출, 실제로는 callback으로 처리 가능)
        def log_success_task(**context):
            log_dbt_run_status(dbt_task_group.group_id, success=True, details="모델 변환 및 DB 상태 확인 완료")

        log_success = PythonOperator(
            task_id="log_success",
            python_callable=log_success_task,
            provide_context=True,
            dag=dag
        )

        # 태스크 의존성 설정
        dbt_task_group >> monitor_task >> log_success >> success_notification
    else:
        raise ValueError("DAG 생성 실패: 프로젝트 경로 또는 연결 설정을 확인하세요.")
