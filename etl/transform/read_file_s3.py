import pandas as pd

# read csv file from s3 with pandas dataframe
def read_file_from_S3(s3, bucket_name: str, file_name: str):
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    except Exception as e:
        print(f"Error reading {file_name} from s3 error: {e}")
        return None
    else:
        df = pd.read_csv(obj["Body"])
        return df