import boto3
import pandas as pd
import os
from io import BytesIO
import zipfile


def lambda_handler(event, context):
    """
    AWS Lambda function to generate fact_metrics data for production from FAOSTAT ZIP file stored in S3 (raw zone),
    and save the transformed CSV to S3 (transformed zone).
    """

    # Environment config
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    raw_prefix = os.environ.get('S3_PREFIX_RAW', 'raw/')
    transformed_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')

    # File keys
    source_zip_key = f"{raw_prefix}FAO/FoodBalance/faostat_consumption.zip"
    dim_country_key = f"{transformed_prefix}dim_country.csv"
    dim_product_key = f"{transformed_prefix}dim_product.csv"
    dim_date_key = f"{transformed_prefix}dim_date.csv"
    output_fact_metrics_key = f"{transformed_prefix}fact_metrics_production.csv"
    csv_filename = "FoodBalanceSheets_E_All_Data.csv"

    # Product and metric config
    METRIC_TYPE = 'production'
    PRODUCTS_MAPPING = {
        'Wheat and products': 'Wheat',
        'Rice and products': 'Rice',
        'Maize and products': 'Maize',
        'Potatoes and products': 'Potatoes',
        'Sweet potatoes': 'Potatoes',
        'Soyabeans': 'Soya'
    }

    # Read and extract zip
    s3_client = boto3.client('s3')
    zip_obj = s3_client.get_object(Bucket=s3_bucket, Key=source_zip_key)
    zip_bytes = BytesIO(zip_obj['Body'].read())
    with zipfile.ZipFile(zip_bytes, 'r') as zip_file:
        with zip_file.open(csv_filename) as f:
            df_raw = pd.read_csv(f, encoding='utf-8')

    # Filter relevant data
    df_filtered = df_raw[(df_raw['Element Code'] == 5510) & (df_raw['Element'] == 'Production')].copy()
    df_filtered = df_filtered[df_filtered['Item'].isin(PRODUCTS_MAPPING.keys())].copy()
    df_filtered['product_name'] = df_filtered['Item'].map(PRODUCTS_MAPPING)

    # Melt year columns
    year_cols = [col for col in df_filtered.columns if col.startswith('Y') and not col.endswith(('F', 'N'))]
    df_melted = df_filtered.melt(id_vars=['Area', 'product_name'], value_vars=year_cols,
                                 var_name='year_str', value_name='value')

    # Parse year
    df_melted['year'] = df_melted['year_str'].str.extract(r'Y(\d{4})').astype(int)
    df_melted['metric_type'] = METRIC_TYPE
    df_melted.rename(columns={'Area': 'country_name'}, inplace=True)

    # Load dimensions
    dim_country = pd.read_csv(BytesIO(s3_client.get_object(Bucket=s3_bucket, Key=dim_country_key)['Body'].read()))
    dim_product = pd.read_csv(BytesIO(s3_client.get_object(Bucket=s3_bucket, Key=dim_product_key)['Body'].read()))
    dim_date = pd.read_csv(BytesIO(s3_client.get_object(Bucket=s3_bucket, Key=dim_date_key)['Body'].read()))
    dim_date = dim_date[dim_date['month'] == 1][['date_id', 'year']]

    # Join dimensions
    df = df_melted.merge(dim_country[['country_id', 'country_name']], on='country_name', how='left')
    df = df.merge(dim_product[['product_id', 'product_name']], on='product_name', how='left')
    df = df.merge(dim_date, on='year', how='left')

    # Final table
    fact_metrics = df[['date_id', 'product_id', 'country_id', 'metric_type', 'value']].copy()
    fact_metrics.dropna(subset=['value', 'date_id', 'product_id', 'country_id'], inplace=True)
    fact_metrics.reset_index(drop=True, inplace=True)
    fact_metrics['fact_id'] = fact_metrics.index + 1
    fact_metrics = fact_metrics[['fact_id'] + [col for col in fact_metrics.columns if col != 'fact_id']]

    # Save to S3
    csv_buffer = BytesIO()
    fact_metrics.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=s3_bucket, Key=output_fact_metrics_key, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': 'Transformation of fact_metrics_production completed successfully!'
    }
