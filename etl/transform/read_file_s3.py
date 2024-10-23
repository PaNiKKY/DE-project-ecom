import pandas as pd

from src.connections import connect_to_s3

s3 = connect_to_s3()
    
# read csv file from s3 with pandas dataframe
def read_file_from_S3(bucket_name: str, file_name: str):
    if s3 is not None:
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_csv(obj["Body"])
        return df
    else:
        return None
