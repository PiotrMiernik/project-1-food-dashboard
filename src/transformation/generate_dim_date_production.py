import pandas as pd
import boto3
import os
import io
import datetime

def lambda_handler(event, context):
    """
    Lambda function to generate dim_date table and store it as CSV in S3.
    """

    # Get bucket and prefix from environment variables
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    s3_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')

    # Dynamic date range based on current year
    start_year = 1960
    current_year = datetime.date.today().year
    end_year = current_year + 1

    # Generate monthly date range
    date_range = pd.date_range(start=f'{start_year}-01-01', 
                                end=f'{end_year}-12-31', 
                                freq='MS')

    # Create DataFrame
    dim_date = pd.DataFrame({
        'all_date': date_range
    })

    # Generate columns according to data warehouse schema
    dim_date['year'] = dim_date['all_date'].dt.year
    dim_date['month'] = dim_date['all_date'].dt.month
    dim_date['month_name'] = dim_date['all_date'].dt.strftime('%B')
    dim_date['quarter'] = dim_date['all_date'].dt.quarter

    # Generate SERIAL-like ID starting from 1
    dim_date.reset_index(inplace=True)
    dim_date.rename(columns={'index': 'date_id'}, inplace=True)
    dim_date['date_id'] += 1

    # Reorder columns
    dim_date = dim_date[['date_id', 'all_date', 'year', 'month', 'month_name', 'quarter']]

    # Convert to CSV in memory
    csv_buffer = io.StringIO()
    dim_date.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3 = boto3.client('s3')
    s3_key = f"{s3_prefix}dim_date.csv"
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': 'dim_date.csv successfully created and uploaded to S3'
    }
