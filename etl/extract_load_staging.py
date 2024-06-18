from sqlalchemy import create_engine
import pandas as pd
import os
import glob


def load_to_staging():
    #os.chdir("c:\\Work\\DE\\eCommerceSales")
    staging_engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/ecom_staging")
    customers_df = pd.read_csv("/opt/airflow/data/csv/customers.csv")
    customers_df.to_sql(name="customers", con=staging_engine, if_exists="append")

