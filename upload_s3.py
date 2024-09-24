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

def check_file_exists(bucket_name, s3_key):
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise
        
def upload_folder_to_s3(folder_path, bucket_name):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            try:
                file_path = os.path.join(root, file)

                s3_key = os.path.relpath(file_path, folder_path).replace("\\", "/")
                
                if check_file_exists(bucket_name, s3_key):
                print(f"File {s3_key} already exists in {bucket_name}. Skipping upload.")
                
                else:
                    s3.upload_file(
                        Filename=file_path,
                        Bucket=bucket_name,
                        Key=s3_key,
                        ExtraArgs={
                            'StorageClass': 'GLACIER_IR'  # s3 class를 Glacier Instant Retrieval 설정
                        }
                    )
                    print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key} with Glacier Instant Retrieval.")
            except:
                print(f"Uploaded failed {file_path}")

upload_folder_to_s3(folder_to_upload, s3_bucket)