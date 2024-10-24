import pandas as pd
import os
from io import StringIO
from src.connections import connect_to_s3
from src.constants import CURRENT_MONTH_YEAR

# Create S3 bucket
def create_s3_bucket(s3,BUCKET_NAME: str):
    if BUCKET_NAME in s3.list_buckets()["Buckets"]:
        print(f"bucket {BUCKET_NAME} already exists")
    else:
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
        except Exception as e:
            print(f"failed to create bucket {BUCKET_NAME} error: {e}")
        else:
            print(f"bucket {BUCKET_NAME} created")

# Create staging folder
def create_staging_object(s3, bucket_name: str, folder_name: str):
    try:
        s3.get_object(Bucket=bucket_name, Key=folder_name)
    except Exception as e:
        s3.put_object(Bucket=bucket_name, Key=folder_name, Body="")
        print(f"folder {folder_name} is created")
    else:
        print(f"folder {folder_name} has already created")

# Load file to s3 bucket
def load_to_S3(s3,bucket_name: str, folder_name: str, file_path: str):
    file_name = os.path.basename(file_path)
    try:
        s3.upload_file(file_path, bucket_name, f"{folder_name}{CURRENT_MONTH_YEAR}_{file_name}")
    except Exception as e:
        print(f"Error uploading {file_path} to s3 error: {e}")
    else:
        print(f"{file_path} is uploaded to s3")
        return file_name


def write_df_to_s3(s3, df: pd.DataFrame, bucket: str, file_name: str):
    with StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        try:
            response = s3.put_object(Bucket=bucket, Key=file_name, Body=csv_buffer.getvalue())
        except Exception as e:
            print(e)
        else:
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"File uploaded successfully. Status - {status}")
            else:
                print(f"File upload failed. Status - {status}")