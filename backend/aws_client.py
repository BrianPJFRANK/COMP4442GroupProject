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

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            print(f"Error loading summary from S3: {e}")
            return None

    def load_raw_csv_from_s3(self, key='cleaned_data.csv'):

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            csv_content = response['Body'].read().decode('utf-8')
            return pd.read_csv(StringIO(csv_content))
        except Exception as e:
            print(f"Error loading raw CSV from S3: {e}")
            return None

    # Local Fallback
    def load_summary_local(self, path='../data_processor/summary.json'):
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def load_raw_local(self, path='../data_processor/cleaned_data.csv'):
        if os.path.exists(path):
            return pd.read_csv(path)
        return None
