from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import os
import glob


def load_to_staging():
    staging_engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/ecom_staging")
    file_path = glob.glob(os.path.join("/opt/airflow/data/", "*.csv"))
    for file in file_path:
        pre_name = os.path.basename(file)
        name = pre_name.split(".")[0]
        df = pd.read_csv(file)
        df.to_sql(name=name, con=staging_engine, if_exists="replace")

