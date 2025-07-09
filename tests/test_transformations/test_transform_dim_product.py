import pandas as pd
import pytest
from io import StringIO, BytesIO
from src.transformation.transform_dim_product import lambda_handler

# Mock boto3 and environment setup
import boto3
from moto import mock_aws
import zipfile
import os

@pytest.fixture
def setup_s3_mock():
    with mock_aws():
        # Prepare mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)

        # Sample CSV data (as it would appear inside the ZIP)
        csv_content = """Item Code,Item
15,Wheat
27,Rice
30,Maize (corn)
42,Soya beans
50,Potatoes
60,Other
"""

        # Create in-memory ZIP with CSV inside
        csv_buffer = StringIO(csv_content)
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr("Value_of_Production_E_All_Data.csv", csv_buffer.getvalue())
        zip_buffer.seek(0)

        # Upload ZIP to mocked S3
        s3.put_object(Bucket=bucket, Key="raw/faostat_production.zip", Body=zip_buffer.read())

        # Set environment variables expected by lambda_handler
        os.environ["S3_BUCKET_PROJECT_1"] = bucket
        os.environ["S3_PREFIX_RAW"] = "raw/"
        os.environ["S3_PREFIX_TRANSFORMED"] = "transformed/"

        yield s3, bucket

def test_transform_dim_product_lambda(setup_s3_mock):
    s3, bucket = setup_s3_mock

    # Run transformation
    result = lambda_handler({}, {})

    # Assertions
    assert result["statusCode"] == 200
    assert "successfully" in result["body"]

    # Download the output file from mocked S3
    response = s3.get_object(Bucket=bucket, Key="transformed/dim_product.csv")
    content = response["Body"].read().decode("utf-8")

    df_result = pd.read_csv(StringIO(content))

    # Check correctness
    assert df_result.shape[0] == 5  # Only 5 products of interest
    assert list(df_result.columns) == ["product_id", "product_name"]
    assert set(df_result["product_name"]) == {"Wheat", "Maize", "Rice", "Soya", "Potatoes"}



