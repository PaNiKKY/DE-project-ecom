from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator

import os
import glob
import sys
from datetime import datetime


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.transform.transform_tables import customers_geolocation_df, order_items_order_df, orders_payments_df, sellers_geolocation_df
from etl.transform.data_cleaning import customer_clean_df, geolocation_clean_df, order_clean_df, order_items_clean_df, payment_clean_df, product_clean_df, seller_clean_df
from etl.transform.read_file_s3 import read_file_from_S3
from etl.load.load_to_s3 import create_s3_bucket, create_staging_object, load_to_S3, write_df_to_s3
from etl.extract.extract_from_kaggle import download_kaggle_dataset
from src.constants import CURRENT_MONTH_YEAR

bucket_name = "ecom-de-project"
raw_folder = "raw/"
cleaned_folder = "cleaned/"

@dag(
    dag_id="ecom_pipline",
    schedule="@monthly",
    start_date=datetime(2022, 9, 1),
    catchup=False,
    tags=["etl", "ecom"]
)
def ecom_dag():
    @task
    def extract_data_kaggle():
        path_files = download_kaggle_dataset("devarajv88/target-dataset")
        print(f"files downloaded in {path_files}")
        return path_files
    
    @task
    def load_raw_S3(path_files, bucket_name, folder_name):
        file_list = glob.glob(f"{path_files}/*.csv")
        print(file_list)
        
        # create bucket if not exist
        create_s3_bucket(bucket_name)

        # create staging folder if not exist
        create_staging_object(bucket_name, folder_name)

        # upload file to s3
        for file in file_list:
            load_to_S3(bucket_name, folder_name, file)

    
    @task
    def clean_merge_data(bucket_name, raw_folder, cleaned_folder):
        # read raw csv file from s3
        orders_df = read_file_from_S3(bucket_name, f"{raw_folder}{CURRENT_MONTH_YEAR}_orders.csv")
        print("orders.csv is read")
        payments_df = read_file_from_S3(bucket_name, f"{raw_folder}{CURRENT_MONTH_YEAR}_payments.csv")
        print("payments.csv is read")
        order_items_df = read_file_from_S3(bucket_name, f"{raw_folder}{CURRENT_MONTH_YEAR}_order_items.csv")
        print("order_items.csv is read")
        customers_df = read_file_from_S3(bucket_name, f"{raw_folder}{CURRENT_MONTH_YEAR}_customers.csv")
        print("customers.csv is read")
        sellers_df = read_file_from_S3(bucket_name, f"{raw_folder}{CURRENT_MONTH_YEAR}_sellers.csv")
        print("sellers.csv is read")
        geolocation_df = read_file_from_S3(bucket_name, f"{raw_folder}{CURRENT_MONTH_YEAR}_geolocation.csv")
        print("geolocation.csv is read")
        products_df = read_file_from_S3(bucket_name, f"{raw_folder}{CURRENT_MONTH_YEAR}_products.csv")
        print("products.csv is read")

        # clean each table
        orders_df_clean = order_clean_df(orders_df)
        print("orders.csv is cleaned")
        payments_df_clean = payment_clean_df(payments_df)
        print("payments.csv is cleaned")
        order_items_df_clean = order_items_clean_df(order_items_df)
        print("order_items.csv is cleaned")
        customers_df_clean = customer_clean_df(customers_df)
        print("customers.csv is cleaned")
        sellers_df_clean = seller_clean_df(sellers_df)
        print("sellers.csv is cleaned")
        geolocation_df_clean = geolocation_clean_df(geolocation_df)
        print("geolocation.csv is cleaned")
        products_df_clean = product_clean_df(products_df)
        print("products.csv is cleaned")
        
        # transform and load table to s3
        orders_payments_transform = orders_payments_df(orders_df_clean, payments_df_clean)
        print("orders_payments is transformed")
        orders_items_transform = order_items_order_df(orders_df_clean, order_items_df_clean)
        print("orders_items is transformed")
        customers_geo_transform = customers_geolocation_df(customers_df_clean, geolocation_df_clean)
        print("customers_geo is transformed")
        sellers_geo_transform = sellers_geolocation_df(sellers_df_clean, geolocation_df_clean)
        print("sellers_geo is transformed")
        
        # create staging folder if not exist
        create_staging_object(bucket_name, cleaned_folder)

        # upload file to s3
        write_df_to_s3(orders_payments_transform, bucket_name, f"{cleaned_folder}{CURRENT_MONTH_YEAR}_orders_payments.csv")
        print("orders_payments is uploaded")
        write_df_to_s3(orders_items_transform, bucket_name, f"{cleaned_folder}{CURRENT_MONTH_YEAR}_orders_items.csv")
        print("orders_items is uploaded")
        write_df_to_s3(customers_geo_transform, bucket_name, f"{cleaned_folder}{CURRENT_MONTH_YEAR}_customers_geo.csv")
        print("customers_geo is uploaded")
        write_df_to_s3(sellers_geo_transform, bucket_name, f"{cleaned_folder}{CURRENT_MONTH_YEAR}_sellers_geo.csv")
        print("sellers_geo is uploaded")
        write_df_to_s3(products_df_clean, bucket_name, f"{cleaned_folder}{CURRENT_MONTH_YEAR}_products.csv")
        print("products is uploaded")

    files = extract_data_kaggle()
    load_raw_S3(files, bucket_name, raw_folder) >> clean_merge_data(bucket_name, raw_folder, cleaned_folder)
    

ecom_dag()