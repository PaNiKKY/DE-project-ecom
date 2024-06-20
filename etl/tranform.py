from sqlalchemy import create_engine
import pandas as pd
from src.data_cleaning import customer_clean_df, geolocation_clean_df

# def table_read(table_name):
#     staging_engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/ecom_staging").connect()
#     with staging_engine as conn:
#         df = pd.read_sql_query(f'''SELECT * FROM public.{table_name}''', con=conn.connection)
#     print(df)

def transform_table():
    df = customer_clean_df()
    df2 = geolocation_clean_df()
    dim_customer = pd.merge(df, df2, left_on="customer_zip_code_prefix", right_on="geolocation_zip_code_prefix", how="left").drop(["geolocation_zip_code_prefix"], axis=1)
    print(f"dim_customers row number: {len(dim_customer)}")
