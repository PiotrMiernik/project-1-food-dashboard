import boto3
import pandas as pd
import pytest
from io import BytesIO
from moto import mock_s3

from helpers.s3_utils import read_csv_from_s3, read_excel_from_s3, write_csv_to_s3

BUCKET = "test-bucket"
CSV_KEY = "sample.csv"
EXCEL_KEY = "sample.xlsx"

@pytest.fixture
def s3_setup():
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        # CSV
        csv_data = "col1,col2\n1,A\n2,B"
        client.put_object(Bucket=BUCKET, Key=CSV_KEY, Body=csv_data.encode())

        # Excel
        excel_df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        excel_buffer = BytesIO()
        excel_df.to_excel(excel_buffer, index=False)
        client.put_object(Bucket=BUCKET, Key=EXCEL_KEY, Body=excel_buffer.getvalue())

        yield

def test_read_csv_from_s3(s3_setup):
    df = read_csv_from_s3(BUCKET, CSV_KEY)

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
    assert list(df.columns) == ['col1', 'col2']

def test_read_excel_from_s3(s3_setup):
    df = read_excel_from_s3(BUCKET, EXCEL_KEY)

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['a', 'b']
    assert df.iloc[1]['b'] == 'y'

def test_write_csv_to_s3(s3_setup):
    df = pd.DataFrame({'x': [10, 20], 'y': ['yes', 'no']})
    new_key = "written.csv"

    write_csv_to_s3(df, BUCKET, new_key)

    client = boto3.client("s3", region_name="us-east-1")
    obj = client.get_object(Bucket=BUCKET, Key=new_key)
    content = pd.read_csv(BytesIO(obj['Body'].read()))

    assert content.equals(df)
