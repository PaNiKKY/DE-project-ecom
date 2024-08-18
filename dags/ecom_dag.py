from airflow.models.dag import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
import s3fs

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.constants import OUT_PATH, CURRENT_MONTH_YEAR, AWS_S3_BUCKET
from etl.load_to_s3 import connect_to_s3
from pipelines.load_s3_pipeline import load_to_S3_pipeline, transform_pipeline
from pipelines.fromS3_to_DW import load_to_DW_pipeline
# from etl.clean_all import load_to_staging
# from etl.tranform import transform_table

MONTH_YEAR = CURRENT_MONTH_YEAR

default_args = {
    "start_date":datetime(2024,6,15),
}

with DAG(
    dag_id="etl_ecomerc_pipeline",
    default_args=default_args,
    schedule_interval = "@monthly",
    catchup=False,
    tags=["etl", "ecom"]
) as dag:
    #extraction
    pull_api =  BashOperator(
        task_id = "download_api",
        bash_command=f'kaggle datasets download -d devarajv88/target-dataset && unzip -o target-dataset.zip -d {OUT_PATH}'
    )

    load_to_s3 = PythonOperator(
        task_id = "load_cvs_to_S3",
        python_callable = load_to_S3_pipeline,
        op_kwargs = {"MONTH_YEAR": MONTH_YEAR}
    )

    remove_files =  BashOperator(
        task_id = "remove_local_files",
        bash_command=f'rm -f {OUT_PATH}/*.csv'
    )

    transfrom_load_to_s3 = PythonOperator(
        task_id = "transform_load_to_s3",
        python_callable = transform_pipeline,
        op_kwargs = {"MONTH_YEAR": MONTH_YEAR}
    )

    load_to_DW = PythonOperator(
        task_id = "load_to_DW",
        python_callable = load_to_DW_pipeline,
        op_kwargs = {"BUCKET_NAME": AWS_S3_BUCKET}
    )

    pull_api >> load_to_s3 >> [transfrom_load_to_s3, remove_files] >> load_to_DW