import pandas as pd
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.schema import CreateSchema

from src.connections import connect_to_postgres
from src.create_dw import create_tables

def put_table_to_DW(df: pd.DataFrame, engine, table_name: str):
    df.to_sql(table_name, engine, if_exists="append", index=False, schema="warehouses")
    print(f"Table {table_name} loaded to data warehouse successfully")

def load_table_to_DW(df: pd.DataFrame, table_name: str, *args):
    engine = connect_to_postgres(*args)
    conn = engine.connect()
    if not engine.dialect.has_table(conn,"dim_customers", "warehouses"):
        create_tables(engine)
    else:
        put_table_to_DW(df,engine, table_name)


