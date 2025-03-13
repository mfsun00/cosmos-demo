from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# DAG 정의
with DAG(
    dag_id='parent1_to_parent2_parallel_children',
    start_date=datetime(2025, 3, 13),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 최상위 태스크: 시작
    start = DummyOperator(task_id='start')

    # 첫 번째 부모 태스크 그룹
    with TaskGroup('parent_group_1', tooltip='첫 번째 부모 태스크 그룹') as parent_group_1:

        # 부모1의 첫 번째 자식 태스크 그룹
        with TaskGroup('child_group_1a', tooltip='부모1의 첫 번째 자식 그룹') as child_group_1a:
            task_1a_1 = DummyOperator(task_id='task_1a_1')
            task_1a_2 = DummyOperator(task_id='task_1a_2')
            task_1a_1 >> task_1a_2  # 내부 순차 실행

        # 부모1의 두 번째 자식 태스크 그룹
        with TaskGroup('child_group_1b', tooltip='부모1의 두 번째 자식 그룹') as child_group_1b:
            task_1b_1 = DummyOperator(task_id='task_1b_1')
            task_1b_2 = DummyOperator(task_id='task_1b_2')
            task_1b_1 >> task_1b_2  # 내부 순차 실행

        # child_group_1a와 child_group_1b는 의존성 없음 -> 병렬 실행

    # 두 번째 부모 태스크 그룹
    with TaskGroup('parent_group_2', tooltip='두 번째 부모 태스크 그룹') as parent_group_2:

        # 부모2의 첫 번째 자식 태스크 그룹
        with TaskGroup('child_group_2a', tooltip='부모2의 첫 번째 자식 그룹') as child_group_2a:
            task_2a_1 = DummyOperator(task_id='task_2a_1')
            task_2a_2 = DummyOperator(task_id='task_2a_2')
            task_2a_1 >> task_2a_2  # 내부 순차 실행

        # 부모2의 두 번째 자식 태스크 그룹
        with TaskGroup('child_group_2b', tooltip='부모2의 두 번째 자식 그룹') as child_group_2b:
            task_2b_1 = DummyOperator(task_id='task_2b_1')
            task_2b_2 = DummyOperator(task_id='task_2b_2')
            task_2b_1 >> task_2b_2  # 내부 순차 실행

        # child_group_2a와 child_group_2b는 의존성 없음 -> 병렬 실행

    # 최상위 태스크: 종료
    end = DummyOperator(task_id='end')

    # 부모 그룹 간 순차 실행: parent_group_1 -> parent_group_2
    start >> parent_group_1 >> parent_group_2 >> end
