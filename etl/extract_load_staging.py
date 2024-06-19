from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import os
import glob


def load_to_staging():
    # os.chdir("/opt/airflow/")
    # conn = psycopg2.connect(database="ecom_staging",user="user",password="pass",
    #                         host="postgres",port="5432")
    # conn.autocommit = True
    # cursor = conn.cursor()
    # create_table = '''
    #             CREATE TABLE IF NOT EXISTS customers (
    #             customer_id int PRIMARY KEY,
    #             customer_unique_id int,
    #             customer_zip_code_prefix varchar(50),
    #             customer_city varchar(50),
    #             customer_state varchar(50));
    #     '''
    
    # load_csv = '''
    #             COPY customers(customer_id, customer_unique_id, customer_zip_code_prefix, customer_city,customer_state)
    #             FROM '/opt/airflow/data/csv/customers.csv'
    #             DELIMITER ','
    #             CSV HEADER;
    #     '''
    # cursor.execute(create_table)
    # cursor.execute(load_csv)
    # conn.close()
    staging_engine = create_engine("postgresql+psycopg2://user:pass@postgres/ecom_staging")
    customers_df = pd.read_csv("/opt/airflow/data/csv/customers.csv")
    customers_df.to_sql(name="customers", con=staging_engine, if_exists="append")

