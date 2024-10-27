import duckdb
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.constants import DATABASE_NAME

def load_to_duckdb(df_list: list):
    conn = duckdb.connect(f"data/{DATABASE_NAME}.duckdb")
    conn.sql(f"INSERT INTO {df_list[0]} SELECT * FROM {df_list[1]}")
    print(f"{df_list[0]} table loaded successfully")
