import boto3
import os
from dotenv import load_dotenv

load_dotenv()

folder_to_upload = os.getenv('FOLDER_PATH')
s3_bucket = os.getenv('BUCKET_NAME')

s3 = boto3.client(
    's3', 
	aws_access_key_id=os.getenv('ACCESS_KEY'),
	aws_secret_access_key=os.getenv('SECRET_KEY')
)

def upload_folder_to_s3(folder_path, bucket_name):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)

            s3_key = os.path.relpath(file_path, folder_path).replace("\\", "/") 
            
            s3.upload_file(
                Filename=file_path,
                Bucket=bucket_name,
                Key=s3_key,
                ExtraArgs={
                    'StorageClass': 'GLACIER_IR'
                }
            )
            print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key} with Glacier Instant Retrieval.")

upload_folder_to_s3(folder_to_upload, s3_bucket)