import boto3
import os
from botocore.exceptions import NoCredentialsError

def upload_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(f"Upload Successful: {local_file} to {bucket}/{s3_file}")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

if __name__ == "__main__":
    # 呢度你可以填返你 AWS S3 嘅資料
    # 建議之後用 environment variables 處理
    BUCKET_NAME = 'your-s3-bucket-name' # 請替換為你的 Bucket Name
    LOCAL_FILE_RAW = 'cleaned_data.csv'
    
    # 上傳 Raw Data (後端 replay engine 需要用)
    if os.path.exists(LOCAL_FILE_RAW):
        print("Uploading cleaned_data.csv...")
        upload_to_s3(LOCAL_FILE_RAW, BUCKET_NAME, 'data/cleaned_data.csv')
