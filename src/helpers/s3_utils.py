import boto3
import pandas as pd
from io import BytesIO

def read_csv_from_s3(bucket: str, key: str, **read_csv_kwargs) -> pd.DataFrame:
    """
    Read CSV file from S3 and return as DataFrame.
    """
    s3_client = boto3.client('s3')  
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(BytesIO(obj['Body'].read()), **read_csv_kwargs)

def read_excel_from_s3(bucket: str, key: str, sheet_name=0, skiprows=0, **read_excel_kwargs) -> pd.DataFrame:
    """
    Read Excel file from S3 and return as DataFrame.
    """
    s3_client = boto3.client('s3')  
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_excel(BytesIO(obj['Body'].read()), sheet_name=sheet_name, skiprows=skiprows, **read_excel_kwargs)

def write_csv_to_s3(df: pd.DataFrame, bucket: str, key: str, encoding='utf-8') -> None:
    """
    Save DataFrame to CSV and write to S3.
    """
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding=encoding)

    s3_client = boto3.client('s3')  
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())