import boto3
import pandas as pd
import json
import os
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

class AWSClient:
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.bucket = os.getenv('AWS_S3_BUCKET', 'your-s3-bucket-name')

    def load_summary_from_s3(self, key='data/summary.json'):
        """從 S3 讀取預先計好嘅 Summary JSON"""
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            print(f"Error loading summary from S3: {e}")
            return None

    def load_raw_csv_from_s3(self, key='data/cleaned_data.csv'):
        """從 S3 讀取清洗過嘅原始 CSV 做 Cache"""
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            csv_content = response['Body'].read().decode('utf-8')
            return pd.read_csv(StringIO(csv_content))
        except Exception as e:
            print(f"Error loading raw CSV from S3: {e}")
            return None

    # Local Fallback (開發測試用)
    def load_summary_local(self, path='../data_processor/summary.json'):
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def load_raw_local(self, path='../data_processor/cleaned_data.csv'):
        if os.path.exists(path):
            return pd.read_csv(path)
        return None
