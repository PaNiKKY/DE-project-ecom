import configparser
import os
import datetime

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_USER = parser.get('database', 'database_username')
DATABASE_PASSWORD = parser.get('database', 'database_password')
DATABASE_NAME = parser.get('database', 'database_name')

OUT_PATH = parser.get('file_paths', 'data_path')

CURRENT_MONTH_YEAR = datetime.datetime.now().strftime("%Y-%m")

AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_SECRET_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
AWS_S3_BUCKET = parser.get('aws', 'aws_bucket_name')