import s3fs
import pandas as pd

def read_file_from_S3(s3: s3fs.S3FileSystem, file_name: str):
    if s3 is not None:
        df = pd.read_csv(s3.open(file_name, "rb"))
        return df
    else:
        return None

# read_rawFile_from_S3(AWS_S3_BUCKET, CURRENT_MONTH_YEAR)
# print(os.getcwd())
