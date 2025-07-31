import os
import pandas as pd

from src.helpers.s3_utils import read_csv_from_s3
from src.helpers.validation import (
    validate_dim_product,
    validate_dim_country,
    validate_dim_date,
    validate_fact_prices,
    validate_fact_metrics
)

def lambda_handler(event, context):
    """
    Lambda entry point triggered by S3 upload (ObjectCreated).
    Validates the specific transformed CSV file that was uploaded.
    """

    # Extract bucket and key (file path) from the S3 event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']
    file_name = os.path.basename(s3_key)

    print(f"🔍 Triggered by upload of: {s3_key}")

    # Map file names to corresponding validation functions
    validation_map = {
        'dim_product.csv': validate_dim_product,
        'dim_country.csv': validate_dim_country,
        'dim_date.csv': validate_dim_date,
        'fact_prices.csv': validate_fact_prices,
        'fact_metrics.csv': validate_fact_metrics,
    }

    if file_name not in validation_map:
        raise ValueError(f"No validator defined for file: {file_name}")

    # Read data from S3
    try:
        df = read_csv_from_s3(s3_bucket, s3_key)
    except Exception as e:
        print(f"Failed to read file from S3: {e}")
        raise

    # Run validation
    try:
        validation_fn = validation_map[file_name]
        validation_fn(df)
        print(f"Validation passed for {file_name}")
        return {
            'statusCode': 200,
            'body': f'Validation passed for {file_name}'
        }
    except Exception as e:
        print(f"Validation failed for {file_name}: {e}")
        raise ValueError(f"Validation failed for {file_name}: {e}")
