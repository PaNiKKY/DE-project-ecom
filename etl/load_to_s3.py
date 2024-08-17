import s3fs
import pandas as pd
from src.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

def connect_to_s3():
    try:
        fs = s3fs.S3FileSystem(key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY,anon=False)
        print("connected to s3")
        return fs
    except Exception as e:
        print(e)
        print("failed to connect to s3")
        return None

def load_to_S3(s3: s3fs.S3FileSystem, file_path: str, bucket: str, file_name: str): 
    if s3 is not None:
        try:
            s3.put(file_path, f"{bucket}/raw/{file_name}")
            print("file uploaded to s3")
        except Exception as e:
            print(e)
    else:
        print("Don't have bucket in s3")

def save_df_to_s3(df: pd.DataFrame, s3: s3fs.S3FileSystem, bucket: str, file_name: str):
    if s3 is not None:
        try:
            df.to_csv(s3.open(f"s3://{bucket}/cleaned/{file_name}.csv", "w"), index=False)
            print("file uploaded to s3")
        except Exception as e:
            print(e)
    else:
        print("Don't have bucket in s3")
    