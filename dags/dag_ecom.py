from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator

import os
import glob
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.connections import connect_to_s3
from etl.load.load_to_DW import load_table_to_DW
from etl.transform.transform_tables import  transform_tables
from etl.transform.data_cleaning import clean_df
from etl.transform.read_file_s3 import read_file_from_S3
from etl.load.load_to_s3 import create_s3_bucket, create_staging_object, load_to_S3, write_df_to_s3
from etl.extract.extract_from_kaggle import download_kaggle_dataset
from src.constants import AWS_S3_BUCKET, CURRENT_MONTH_YEAR, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME

bucket_name ="ecom-de-project"
raw_folder = "raw/"
cleaned_folder = "cleaned/"
aws_access_key_id = AWS_ACCESS_KEY_ID
aws_secret_access_key = AWS_SECRET_ACCESS_KEY
bucket_name = AWS_S3_BUCKET
database_host = DATABASE_HOST
database_port = DATABASE_PORT
database_user = DATABASE_USER
database_password = DATABASE_PASSWORD 
database_name = DATABASE_NAME
@dag(
    dag_id="ecom_pipline",
    schedule="@monthly",
    start_date=datetime(2022, 9, 1),
    catchup=False,
    tags=["etl", "ecom"]
)
def ecom_dag():
    s3 = connect_to_s3(aws_access_key_id, aws_secret_access_key)
    @task
    def extract_data_kaggle():
        path_files = download_kaggle_dataset("devarajv88/target-dataset")
        print(f"files downloaded in {path_files}")
        return path_files
    
    @task
    def load_raw_S3(path_files):
        file_list = glob.glob(f"{path_files}/*.csv")
        file_list.remove(f"{path_files}/payments.csv")
        print(file_list)
        tables_list_form_s3 = []

        create_s3_bucket(s3, bucket_name)
        create_staging_object(s3, bucket_name, raw_folder)

        # upload file to s3
        for file in file_list:
            file_name_form_s3 = load_to_S3(s3,bucket_name, raw_folder, file)
            tables_list_form_s3.append(file_name_form_s3)

        return tables_list_form_s3
    
    @task
    def clean_data(tables_list_form_s3):

        clean_tables_list = []

        create_staging_object(s3, bucket_name, cleaned_folder)

        for file in tables_list_form_s3:
        # read raw csv file from s3
            read_file_df = read_file_from_S3(s3,
                                             bucket_name, 
                                             f"{raw_folder}{CURRENT_MONTH_YEAR}_{file}"
                                        )
            df_clean = clean_df(file, read_file_df)

            clean_tables = file.split(".")[0]+"_cleaned.csv"
            write_df_to_s3(s3,
                           df_clean,
                           bucket_name, 
                           f"{cleaned_folder}{CURRENT_MONTH_YEAR}_{clean_tables}"
                        )
            clean_tables_list.append(clean_tables)
            print(f"{file} is cleaned and uploaded to s3")

        return clean_tables_list
    
    @task
    def model_load_to_DW(clean_tables_list):
        read_file_list = {}
        for file in clean_tables_list:
            read_file_df = read_file_from_S3(s3,
                                            bucket_name, 
                                            f"{raw_folder}{CURRENT_MONTH_YEAR}_{file}"
                                        )
            read_file_list[file.split("_")[0]] = read_file_df
        
        transform_df_dict = transform_tables(read_file_list)

        for table_name, df in transform_df_dict.items():
            load_table_to_DW(table_name, 
                            df,
                            database_user,
                            database_password,
                            database_host, 
                            database_port,
                            database_name)

    files = extract_data_kaggle()
    tales_list_form_s3 = load_raw_S3(files)
    clean_tables_list = clean_data(tales_list_form_s3)
    # model_load_to_DW(clean_tables_list)
    

ecom_dag()