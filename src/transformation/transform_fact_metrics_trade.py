import boto3
import pandas as pd
import os
from io import BytesIO


def lambda_handler(event, context):
    """
    AWS Lambda function to generate fact_metrics (trade) from FAOSTAT CSV stored in S3 (raw zone),
    and save the transformed CSV to S3 (transformed zone).
    """

    # Environment config
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    raw_prefix = os.environ.get('S3_PREFIX_RAW', 'raw/')
    transformed_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')

    # File keys
    source_csv_key = f"{raw_prefix}FAO/Trade/Trade_CropsLivestock_E_All_Data_NOFLAG.csv"
    dim_country_key = f"{transformed_prefix}dim_country.csv"
    dim_product_key = f"{transformed_prefix}dim_product.csv"
    dim_date_key = f"{transformed_prefix}dim_date.csv"
    output_key = f"{transformed_prefix}fact_metrics_trade.csv"

    # Constants
    METRIC_TYPE_MAP = {5610: 'import', 5910: 'export'}
    PRODUCTS_MAPPING = {
        'Wheat': 'Wheat',
        'Maize (corn)': 'Maize',
        'Green corn (maize)': 'Maize',
        'Rice': 'Rice',
        'Soya beans': 'Soya',
        'Potatoes': 'Potatoes'
    }

    # Init S3 client
    s3_client = boto3.client('s3')

    # Load source data
    source_obj = s3_client.get_object(Bucket=s3_bucket, Key=source_csv_key)
    df_raw = pd.read_csv(BytesIO(source_obj['Body'].read()))

    # Filter rows
    df_filtered = df_raw[df_raw['Element Code'].isin(METRIC_TYPE_MAP.keys())]
    df_filtered = df_filtered[df_filtered['Item'].isin(PRODUCTS_MAPPING.keys())].copy()
    df_filtered['product_name'] = df_filtered['Item'].map(PRODUCTS_MAPPING)
    df_filtered['metric_type'] = df_filtered['Element Code'].map(METRIC_TYPE_MAP)

    # Melt year columns
    year_cols = [col for col in df_filtered.columns if col.startswith('Y')]
    df_melted = df_filtered.melt(id_vars=['Area', 'product_name', 'metric_type'],
                                 value_vars=year_cols,
                                 var_name='year_str', value_name='value')
    df_melted['year'] = df_melted['year_str'].str.extract(r'Y(\d{4})').astype(int)
    df_melted.rename(columns={'Area': 'country_name'}, inplace=True)

    # Load dimensions
    dim_country_obj = s3_client.get_object(Bucket=s3_bucket, Key=dim_country_key)
    dim_product_obj = s3_client.get_object(Bucket=s3_bucket, Key=dim_product_key)
    dim_date_obj = s3_client.get_object(Bucket=s3_bucket, Key=dim_date_key)
    dim_country = pd.read_csv(BytesIO(dim_country_obj['Body'].read()))
    dim_product = pd.read_csv(BytesIO(dim_product_obj['Body'].read()))
    dim_date = pd.read_csv(BytesIO(dim_date_obj['Body'].read()))
    dim_date_filtered = dim_date[dim_date['month'] == 1][['date_id', 'year']]

    # Join dimensions
    df_trade = df_melted.merge(dim_country[['country_id', 'country_name']], on='country_name', how='left')
    df_trade = df_trade.merge(dim_product[['product_id', 'product_name']], on='product_name', how='left')
    df_trade = df_trade.merge(dim_date_filtered, on='year', how='left')

    # Final table
    fact_metrics = df_trade[['date_id', 'product_id', 'country_id', 'metric_type', 'value']].copy()
    fact_metrics.dropna(subset=['value', 'date_id', 'product_id', 'country_id'], inplace=True)

    # Add fact_id
    fact_metrics.reset_index(drop=True, inplace=True)
    fact_metrics['fact_id'] = fact_metrics.index + 1
    fact_metrics = fact_metrics[['fact_id'] + [col for col in fact_metrics.columns if col != 'fact_id']]

    # Save to S3
    csv_buffer = BytesIO()
    fact_metrics.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': 'Transformation of fact_metrics_trade completed successfully!'
    }
