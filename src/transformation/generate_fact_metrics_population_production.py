import boto3
import pandas as pd
import os
from io import BytesIO
import zipfile


def lambda_handler(event, context):
    """
    AWS Lambda function to generate fact_metrics (population) from World Bank CSV ZIP stored in S3 (raw zone),
    and save the transformed CSV to S3 (transformed zone).
    """

    # Environment config
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    raw_prefix = os.environ.get('S3_PREFIX_RAW', 'raw/')
    transformed_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')

    # File keys
    zip_key = f"{raw_prefix}WB/wb_population.zip"
    internal_csv = "API_SP.POP.TOTL_DS2_en_csv_v2_127006.csv"
    dim_country_key = f"{transformed_prefix}dim_country.csv"
    dim_date_key = f"{transformed_prefix}dim_date.csv"
    output_key = f"{transformed_prefix}fact_metrics_population.csv"

    # Constants
    METRIC_TYPE = "population"
    TECHNICAL_PRODUCT_ID = 0

    # Init S3 client
    s3_client = boto3.client('s3')

    # Load zip and extract CSV
    zip_obj = s3_client.get_object(Bucket=s3_bucket, Key=zip_key)
    with zipfile.ZipFile(BytesIO(zip_obj['Body'].read()), 'r') as z:
        with z.open(internal_csv) as f:
            df_raw = pd.read_csv(f, skiprows=4)

    # Filter columns
    df_filtered = df_raw[['Country Name'] + [str(y) for y in range(1960, 2025)]].copy()
    df_filtered.rename(columns={'Country Name': 'country_name'}, inplace=True)

    # Melt to long format
    df_melted = df_filtered.melt(id_vars='country_name', var_name='year', value_name='value')
    df_melted['year'] = df_melted['year'].astype(int)
    df_melted['product_id'] = TECHNICAL_PRODUCT_ID
    df_melted['metric_type'] = METRIC_TYPE

    # Load dimension tables
    dim_country_obj = s3_client.get_object(Bucket=s3_bucket, Key=dim_country_key)
    dim_date_obj = s3_client.get_object(Bucket=s3_bucket, Key=dim_date_key)
    dim_country = pd.read_csv(BytesIO(dim_country_obj['Body'].read()))
    dim_date = pd.read_csv(BytesIO(dim_date_obj['Body'].read()))
    dim_date_filtered = dim_date[dim_date['month'] == 1][['date_id', 'year']]

    # Join with dimensions
    df = df_melted.merge(dim_country[['country_id', 'country_name']], on='country_name', how='left')
    df = df.merge(dim_date_filtered, on='year', how='left')

    # Final table
    fact_metrics = df[['date_id', 'product_id', 'country_id', 'metric_type', 'value']].copy()
    fact_metrics.dropna(subset=['value', 'date_id', 'country_id'], inplace=True)

    # Add fact_id
    fact_metrics.reset_index(drop=True, inplace=True)
    fact_metrics['fact_id'] = fact_metrics.index + 1
    fact_metrics = fact_metrics[['fact_id'] + [col for col in fact_metrics.columns if col != 'fact_id']]
    fact_metrics['value'] = fact_metrics['value'].astype(int)

    # Export to CSV and upload to S3
    csv_buffer = BytesIO()
    fact_metrics.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': 'Transformation of fact_metrics_population completed successfully!'
    }
