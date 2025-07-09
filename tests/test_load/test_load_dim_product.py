import os
import pandas as pd
import pytest
from unittest import mock
from io import StringIO

from src.load.load_dim_product import lambda_handler


@mock.patch("src.load.load_dim_product.get_db_connection")
@mock.patch("src.load.load_dim_product.read_csv_from_s3")
def test_lambda_handler_load_dim_product(mock_read_csv, mock_get_conn):
    """
    Unit test for lambda_handler in load_dim_product.py.
    Mocks S3 read and PostgreSQL connection to validate data loading logic.
    """

    # Mock returned DataFrame from S3
    mock_df = pd.DataFrame({
        "product_id": [1, 2],
        "product_name": ["Wheat", "Rice"]
    })
    mock_read_csv.return_value = mock_df

    # Mock PostgreSQL connection and cursor
    mock_cursor = mock.Mock()
    mock_conn = mock.Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn

    # Set environment variables
    os.environ['S3_BUCKET_PROJECT_1'] = "test-bucket"
    os.environ['S3_PREFIX_TRANSFORMED'] = "transformed/"

    # Run the function
    result = lambda_handler()

    # Check returned result
    assert result["statusCode"] == 200
    assert "loaded successfully" in result["body"]

    # Verify function calls
    mock_read_csv.assert_called_once_with("test-bucket", "transformed/dim_product.csv")
    mock_cursor.execute.assert_called_once_with("TRUNCATE TABLE dim_product RESTART IDENTITY")
    mock_cursor.copy_expert.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()
