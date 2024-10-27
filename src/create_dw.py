import os
import sys
import duckdb


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.connections import connect_to_duckdb

def create_tables(conn, sql_file):
    with open(f"src/sql_scripts/{sql_file}") as script:
        conn.sql(script.read())
    print(f"table {os.path.basename(sql_file)} created successfully")

def create_data_warehouse(data_warehouse):
    conn = connect_to_duckdb(data_warehouse)
    tables = conn.sql("SHOW TABLES").fetchall()
    if len(tables) == 0:
        create_tables(conn, "dim_date.sql")
        create_tables(conn, "dim_time.sql")
        create_tables(conn, "fact_dim.sql")
    else:
        print("Tables already exist")

    
