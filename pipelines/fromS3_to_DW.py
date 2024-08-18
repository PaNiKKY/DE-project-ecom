from etl.read_file_s3 import read_file_from_S3
from etl.load_to_DW import load_to_postgres
from src.conn_db import create_data_warehouse
from etl.load_to_s3 import connect_to_s3
from etl.transform_DW import tables_constraint
import os

def load_to_DW_pipeline(BUCKET_NAME: str):
    engine = create_data_warehouse()
    s3 = connect_to_s3()
    list_files = s3.ls(f"s3://{BUCKET_NAME}/cleaned/")[1:]
    for file in list_files:
        file_name = os.path.basename(file).split(".")[0]
        table_name = file_name.split("_")[1:]
        df = read_file_from_S3(s3, f"s3://{file}")
        if len(table_name) > 1:
            table_name = "_".join(table_name)
        else:
            table_name = table_name[0]

        load_to_postgres(df, engine, table_name)

    tables_constraint(engine)
    print("----Data warehouse building successfully----")
    
