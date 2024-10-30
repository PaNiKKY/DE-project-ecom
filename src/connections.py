import duckdb
import boto3

def connect_to_duckdb(dw_name):
    try:
        conn = duckdb.connect(f"data/{dw_name}.duckdb")
    except Exception as e:
        print(f"failed to connect to duckdb error: {e}")
    else:
        print("connected to duckdb")
        return conn

def connect_to_s3(aws_access_key_id, aws_secret_access_key):
    session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    try:
        s3 = session.client('s3')
    except Exception as e:
        print(f"failed to connect to s3 error: {e}")
        return None
    else:
        print("connected to s3")
        return s3