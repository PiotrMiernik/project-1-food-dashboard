import boto3
import pandas as pd
import os
from io import BytesIO
import zipfile

def lambda_handler(event, context):
    """
    AWS Lambda function to generate dim_country table for the data warehouse.
    - Extracts country data from FAO ZIP file in S3.
    - Enriches data with continent info using mapping file in S3 (resources zone).
    - Saves transformed dimension table into transformed zone on S3.
    """
    
    # Environment variables
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    raw_prefix = os.environ.get('S3_PREFIX_RAW', 'raw/')
    transformed_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')
    resources_prefix = os.environ.get('S3_PREFIX_RESOURCES', 'resources/')
    
    # Filenames
    zip_key = f'{raw_prefix}faostat_production.zip'
    csv_inside_zip = 'Value_of_Production_E_All_Data.csv'
    mapping_key = f'{resources_prefix}m49_continents.csv'
    
    # Initialize boto3 client
    s3_client = boto3.client('s3')
    
    # Load ZIP file from S3 and extract target CSV file
    zip_obj = s3_client.get_object(Bucket=s3_bucket, Key=zip_key)
    zip_bytes = BytesIO(zip_obj['Body'].read())
    
    with zipfile.ZipFile(zip_bytes, 'r') as zip_file:
        with zip_file.open(csv_inside_zip) as csv_file:
            df_raw = pd.read_csv(csv_file, encoding='utf-8')
    
    # Extract country columns
    df_countries = df_raw[['Area Code (M49)', 'Area']].drop_duplicates().copy()
    
    # Clean M49 codes — remove leading quote and cast to int
    df_countries['m49_code'] = df_countries['Area Code (M49)'].str.replace("'",  "").astype(int)
    
    # Load mapping file with continents from S3
    mapping_obj = s3_client.get_object(Bucket=s3_bucket, Key=mapping_key)
    mapping_data = mapping_obj['Body'].read()
    df_mapping = pd.read_csv(BytesIO(mapping_data), encoding='utf-8')
    
    # Merge countries with continents
    df_enriched = pd.merge(df_countries, df_mapping, on='m49_code', how='left')
    
    # Generate surrogate key for country_id (start from 1)
    df_enriched.reset_index(drop=True, inplace=True)
    df_enriched['country_id'] = df_enriched.index + 1
    
    # Build final dimension table
    dim_country = df_enriched[['country_id', 'Area', 'continent']]
    dim_country.rename(columns={'Area': 'country_name', 'continent': 'continent_name'}, inplace=True)
    dim_country = dim_country[dim_country['continent_name'].notna()]
    
    # Write transformed file to S3 (transformed zone)
    csv_buffer = BytesIO()
    dim_country.to_csv(csv_buffer, index=False)
    
    s3_client.put_object(Bucket=s3_bucket, Key=f"{transformed_prefix}dim_country.csv", Body=csv_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': 'Transformation of dim_country completed successfully!'
    }
# Test CI workflow on push to dev