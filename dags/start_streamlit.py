from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.bash import BashOperator


@dag(
    dag_id="start_streamlit",
    schedule="@once",
    start_date=datetime(2022, 9, 1),
    catchup=False,
    tags=["start"],
)
def start_streamlit():
    start = BashOperator(
        task_id="start_streamlit",
        cwd="/opt/airflow",
        bash_command="nohup streamlit run app/dashboard.py",
    )

    start

start_streamlit()