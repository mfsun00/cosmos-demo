from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from pendulum import datetime

# 데이터셋 정의
DATASET_SENT_1 = Dataset("file://send_dataset_1")  # DAG 1 -> DAG 2 연결
DATASET_RECEIVED_1 = Dataset("file://receive_dataset_1")  # DAG 2 -> DAG 3 연결

# 송출 작업 시뮬레이션
def send_data(dataset_name):
    print(f"Sending data to {dataset_name}")

# 수신 작업 시뮬레이션
def receive_data(dataset_name):
    print(f"Receiving data from {dataset_name}")

# DAG 1: 송출 (Send 1)
with DAG(
    dag_id="send_dag_1",
    start_date=datetime(2025, 3, 11),
    schedule="@daily",  # 최초 트리거
    catchup=False
) as send_dag_1:
    send_task_1 = PythonOperator(
        task_id="send_task_1",
        python_callable=send_data,
        op_args=["send_dataset_1"],
        outlets=[DATASET_SENT_1]  # DAG 2로 전달
    )

# DAG 2: 수신 (Receive 1, Send 1에 의존)
with DAG(
    dag_id="receive_dag_1",
    start_date=datetime(2025, 3, 11),
    schedule=[DATASET_SENT_1],  # DAG 1 완료 후 실행
    catchup=False
) as receive_dag_1:
    receive_task_1 = PythonOperator(
        task_id="receive_task_1",
        python_callable=receive_data,
        op_args=["send_dataset_1"],
        outlets=[DATASET_RECEIVED_1]  # DAG 3으로 전달
    )

# DAG 3: 송출 → 수신 (Send 2 → Receive 2, Receive 1에 의존)
with DAG(
    dag_id="send_receive_dag_2",
    start_date=datetime(2025, 3, 11),
    schedule=[DATASET_RECEIVED_1],  # DAG 2 완료 후 실행
    catchup=False
) as send_receive_dag_2:
    send_task_2 = PythonOperator(
        task_id="send_task_2",
        python_callable=send_data,
        op_args=["send_dataset_2"]
    )

    receive_task_2 = PythonOperator(
        task_id="receive_task_2",
        python_callable=receive_data,
        op_args=["send_dataset_2"]
    )

    send_task_2 >> receive_task_2  # DAG 3 내부 순서

