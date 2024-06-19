from airflow.models.dag import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract_load_staging import load_to_staging
from etl.tranform import table_read



default_args = {
    "start_date":datetime(2024,6,15),
}

with DAG(
    dag_id="etl_ecomerc_pipeline_test_trans",
    default_args=default_args,
    schedule_interval = "@monthly",
    catchup=False,
    tags=["etl", "ecom"]
) as dag:
    #extraction
    # pull_api =  BashOperator(
    #     task_id = "download_api",
    #     bash_command='kaggle datasets download -d devarajv88/target-dataset && unzip -o target-dataset.zip -d /opt/airflow/data/'
    # )

    # extract_csv = PythonOperator(
    #     task_id = "load_cvs_to_staging",
    #     python_callable = load_to_staging
    # )

    #transformation
    # pull_api >> extract_csv 

    test_read_csv = PythonOperator(
        task_id = "test_read",
        python_callable = table_read,
        op_kwargs = {
            "table_name":"customers"
        }
    )

    test_read_csv