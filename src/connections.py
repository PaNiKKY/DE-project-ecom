from sqlalchemy import create_engine
import boto3

def connect_to_postgres(database_username, database_password, database_host, database_port, database_name):
    engine = create_engine(f"postgresql+psycopg2://{database_username}:{database_password}@{database_host}:{database_port}/{database_name}")
    
    return engine

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