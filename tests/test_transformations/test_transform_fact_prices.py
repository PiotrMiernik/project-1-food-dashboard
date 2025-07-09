import os
import boto3
import pandas as pd
import pytest
from io import BytesIO
from moto import mock_aws

from src.transformation.transform_fact_prices import lambda_handler

@pytest.fixture
def setup_s3_mock():
    with mock_aws():
        # Mock S3 setup
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)

        # Upload mock Excel (Monthly Prices) with proper structure
        excel_data = {
            "Monthly Prices": pd.DataFrame({
                "Date": ["2020M01", "2020M02", "2020M03"],
                "Soybeans": [350, 360, 370],
                "Maize": [180, 185, 190],
                "Rice, Thai 5%": [500, 510, 520],
                "Wheat, US HRW": [220, 230, 240]
            })
        }

        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            for sheet, df in excel_data.items():
                df.to_excel(writer, sheet_name=sheet, index=False, startrow=4)
        excel_buffer.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key="raw/WB/CMO-Historical-Data-Monthly.xlsx",
            Body=excel_buffer.getvalue()
        )

        # Upload mock dim_product.csv
        dim_product = pd.DataFrame({
            "product_id": [1, 2, 3, 4],
            "product_name": ["Soya", "Maize", "Rice", "Wheat"]
        })
        buffer_product = BytesIO()
        dim_product.to_csv(buffer_product, index=False)
        buffer_product.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key="transformed/dim_product.csv",
            Body=buffer_product.getvalue()
        )

        # Upload mock dim_date.csv
        dim_date = pd.DataFrame({
            "date_id": [1, 2, 3],
            "year": [2020, 2020, 2020],
            "month": [1, 2, 3]
        })
        buffer_date = BytesIO()
        dim_date.to_csv(buffer_date, index=False)
        buffer_date.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key="transformed/dim_date.csv",
            Body=buffer_date.getvalue()
        )

        os.environ['S3_BUCKET_PROJECT_1'] = bucket
        os.environ['S3_PREFIX_RAW'] = "raw/"
        os.environ['S3_PREFIX_TRANSFORMED'] = "transformed/"

        yield s3


def test_transform_fact_prices_lambda(setup_s3_mock):
    """
    Test for lambda_handler in transform_fact_prices.
    Validates that output fact_prices.csv is correctly created and uploaded to mock S3.
    """
    # Run the transformation
    result = lambda_handler({}, {})

    # Verify response structure
    assert result['statusCode'] == 200
    assert 'fact_prices completed' in result['body']

    # Download the result from mock S3 and check structure
    s3 = boto3.client("s3", region_name="us-east-1")
    obj = s3.get_object(Bucket=os.environ['S3_BUCKET_PROJECT_1'],
                        Key="transformed/fact_prices.csv")
    df = pd.read_csv(BytesIO(obj['Body'].read()))

    # Check expected columns and some values
    expected_columns = ['price_id', 'date_id', 'product_id', 'price_usd_per_ton',
                        'avg_annual_price', 'price_annual_change_pct', 'price_month_change_pct']
    assert list(df.columns) == expected_columns
    assert not df.empty
    assert df['price_usd_per_ton'].notna().all()
