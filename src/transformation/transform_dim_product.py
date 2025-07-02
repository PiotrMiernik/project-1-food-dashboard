import boto3
import pandas as pd
import os
from io import BytesIO
import zipfile

def lambda_handler(event, context):
    """
    AWS Lambda function to process dim_product data from FAO source file stored in S3 (raw zone),
    and save transformed file to S3 (transformed zone).
    """

    # Read AWS S3 environment variables for bucket and prefixes
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    raw_prefix = os.environ.get('S3_PREFIX_RAW', 'raw/')
    transformed_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')

    # Name of the ZIP file and CSV inside it
    zip_file_key = f'{raw_prefix}faostat_production.zip'
    csv_inside_zip = 'Value_of_Production_E_All_Data.csv'

    # Read ZIP file from S3
    s3_client = boto3.client('s3')
    zip_obj = s3_client.get_object(Bucket=s3_bucket, Key=zip_file_key)
    zip_bytes = BytesIO(zip_obj['Body'].read())

    # Unzip the file directly in memory
    with zipfile.ZipFile(zip_bytes, 'r') as zip_file:
        with zip_file.open(csv_inside_zip) as csv_file:
            df_raw = pd.read_csv(csv_file, encoding='utf-8')

    # Define dictionary of products of interest
    PRODUCTS_OF_INTEREST = {
        "Wheat": "Wheat",
        "Maize (corn)": "Maize",
        "Rice": "Rice",
        "Soya beans": "Soya",
        "Potatoes": "Potatoes"
    }

    # Extract relevant columns & filter products
    df_products = df_raw[['Item Code', 'Item']].drop_duplicates()
    df_filtered = df_products[df_products['Item'].isin(PRODUCTS_OF_INTEREST.keys())].copy()
    df_filtered['product_name'] = df_filtered['Item'].map(PRODUCTS_OF_INTEREST)

    # Generate surrogate key
    df_filtered.reset_index(drop=True, inplace=True)
    df_filtered['product_id'] = df_filtered.index + 1
    dim_product = df_filtered[['product_id', 'product_name']]

    # Convert to CSV buffer
    csv_buffer = BytesIO()
    dim_product.to_csv(csv_buffer, index=False)

    # Upload transformed file to S3 (transformed zone)
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=f"{transformed_prefix}dim_product.csv",
        Body=csv_buffer.getvalue()
    )

    return {
        'statusCode': 200,
        'body': 'Transformation of dim_product completed successfully!'
    }
