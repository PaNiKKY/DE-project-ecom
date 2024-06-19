from sqlalchemy import create_engine
import pandas as pd

def table_read(table_name):
    staging_engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/ecom_staging").connect()
    with staging_engine as conn:
        df = pd.read_sql_query(f'''SELECT * FROM public.{table_name}''', con=conn.connection)
    print(df)

