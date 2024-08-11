from src.constants import AWS_S3_BUCKET, OUT_PATH
from etl.load_to_s3 import load_to_S3, connect_to_s3
import glob
import os
def load_to_S3_pipeline(MONTH_YEAR: str):
    s3 = connect_to_s3()
    file_paths = glob.glob(f"{OUT_PATH}/*.csv")
    if s3 is not None:
        for file_path in file_paths:
            file_name = f"{MONTH_YEAR}_"+ os.path.basename(file_path)
            load_to_S3(s3, file_path, AWS_S3_BUCKET, file_name)