import boto3
import pandas as pd
import pytest
from io import BytesIO
from moto import mock_aws

# Import the functions from s3_utils.py module for testing
from src.helpers.s3_utils import read_csv_from_s3, read_excel_from_s3, write_csv_to_s3

# Define constants for the test environment (bucket name, file keys)
BUCKET = "test-bucket"
CSV_KEY = "sample.csv"
EXCEL_KEY = "sample.xlsx"

@pytest.fixture
def s3_setup():
    """
    Pytest fixture to set up a mocked S3 environment for tests.
    This ensures tests are isolated, fast, and don't incur AWS costs.    
    """
    # 1. Mock the S3 service: All boto3 S3 calls will be intercepted by Moto.
    with mock_aws():
        # Initialize a boto3 S3 client that now interacts with the mocked S3.
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        # 2. Prepare sample CSV data and upload it to the mocked S3.
        csv_data = "col1,col2\n1,A\n2,B"
        client.put_object(Bucket=BUCKET, Key=CSV_KEY, Body=csv_data.encode())

        # 3. Prepare sample Excel data (DataFrame converted to BytesIO) and upload to mocked S3.
        excel_df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        excel_buffer = BytesIO()
        excel_df.to_excel(excel_buffer, index=False)
        client.put_object(Bucket=BUCKET, Key=EXCEL_KEY, Body=excel_buffer.getvalue())

        # 'yield' indicates the setup phase is complete, and tests can now run.
        # After tests complete, the 'with mock_s3()' context automatically cleans up the mocked environment.
        yield

def test_read_csv_from_s3(s3_setup):
    """
    Test case for the read_csv_from_s3 function.
    """
    # 1. Execute the function under test.
    # It will read from the mocked S3 bucket set up by the fixture.
    df = read_csv_from_s3(BUCKET, CSV_KEY)

    # 2. Assertions: Verify the output of the function.
    assert isinstance(df, pd.DataFrame) # Check if the return type is a Pandas DataFrame
    assert df.shape == (2, 2)           # Check if the DataFrame has the expected dimensions (rows, columns)
    assert list(df.columns) == ['col1', 'col2'] # Check if column names are correct

def test_read_excel_from_s3(s3_setup):
    """
    Test case for the read_excel_from_s3 function.
    """
    # 1. Execute the function under test.
    df = read_excel_from_s3(BUCKET, EXCEL_KEY)

    # 2. Assertions: Verify the output.
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['a', 'b']
    assert df.iloc[1]['b'] == 'y' # Check a specific value to ensure correct data parsing

def test_write_csv_to_s3(s3_setup):
    """
    Test case for the write_csv_to_s3 function.
    This test writes data and then attempts to read it back from the mocked S3
    to verify the write operation was successful and data integrity is maintained.
    """
    # 1. Prepare sample DataFrame to be written.
    df = pd.DataFrame({'x': [10, 20], 'y': ['yes', 'no']})
    new_key = "written.csv"

    # 2. Execute the function under test: write the DataFrame to mocked S3.
    write_csv_to_s3(df, BUCKET, new_key)

    # 3. Verification: Read the data back directly from the mocked S3 using boto3.
    client = boto3.client("s3", region_name="us-east-1") # Re-initialize client (still mocks S3)
    obj = client.get_object(Bucket=BUCKET, Key=new_key)
    # Read the content of the object and convert it back to a DataFrame for comparison.
    content = pd.read_csv(BytesIO(obj['Body'].read()))

    # 4. Assertions: Compare the original DataFrame with the content read back from S3.
    # df.equals() is a robust Pandas method for DataFrame comparison.
    assert content.equals(df)
