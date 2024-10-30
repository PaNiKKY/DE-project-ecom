import duckdb
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.constants import DATABASE_NAME

def load_to_duckdb(conn, df_list: list):
    df = df_list[1]
    conn.sql(f"INSERT INTO {df_list[0]} SELECT * FROM df")
    print(f"{df_list[0]} table loaded successfully")
