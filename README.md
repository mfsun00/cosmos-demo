# Airflow Cosmos DBT Integration

이 프로젝트는 Apache Airflow에서 Astronomer Cosmos를 활용해 dbt 프로젝트를 실행하는 예제입니다. 유틸리티 함수와 DAG를 분리하여 모듈화했으며, 로깅, 알림, 모니터링 기능을 포함합니다.

## 디렉토리 구조
/dags/
├── dbt_utils.py          # Cosmos 및 유틸리티 함수 정의
├── example_dbt_dag.py    # DAG 정의
└── dbt/
└── my_project/       # dbt 프로젝트 디렉토리
├── dbt_project.yml
└── ...



## 주요 기능
- **DbtTaskGroup 생성**: Cosmos를 사용해 dbt 작업을 Airflow TaskGroup으로 변환.
- **로깅**: 파일(`logs/dbt_utils.log`) 및 콘솔에 작업 상태 기록.
- **슬랙 알림**: 성공/실패 시 슬랙 채널로 알림 전송.
- **DB 모니터링**: dbt 실행 후 데이터베이스 상태 확인.
- **환경 변수 지원**: 설정 값을 환경 변수에서 동적으로 로드.

## 설치 및 설정
1. **필요 패키지 설치**:
   ```bash
   pip install astronomer-cosmos apache-airflow-providers-slack apache-airflow-providers-postgres


2. Airflow Connection 설정:
postgres_default: Postgres 데이터베이스 연결.
slack_default: Slack Webhook 연결 (Airflow UI에서 설정).


3. 환경 변수 설정 (선택):
export DBT_PROJECT_PATH="/path/to/dbt/project"
export SLACK_CONN_ID="slack_default"

4. 파일 배치:
dbt_utils.py와 example_dbt_dag.py를 dags/ 폴더에 배치.
dbt 프로젝트를 dags/dbt/my_project/에 준비.

5. 실행:
Airflow 스케줄러를 실행하여 DAG를 로드 및 실행.
