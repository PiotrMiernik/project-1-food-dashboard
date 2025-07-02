import boto3
import pandas as pd
import os
from io import BytesIO, StringIO

def lambda_handler(event, context):
    """
    AWS Lambda function to generate fact_prices from World Bank source Excel file stored in S3 (raw zone),
    and save the transformed CSV to S3 (transformed zone).
    """

    # Environment config
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    raw_prefix = os.environ.get('S3_PREFIX_RAW', 'raw/')
    transformed_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')

    # File keys
    source_excel_key = f'{raw_prefix}WB/CMO-Historical-Data-Monthly.xlsx'
    dim_product_key = f'{transformed_prefix}dim_product.csv'
    dim_date_key = f'{transformed_prefix}dim_date.csv'
    output_fact_prices_key = f'{transformed_prefix}fact_prices.csv'
    sheet_name = 'Monthly Prices'

    s3_client = boto3.client('s3')

    # Load Excel file from S3
    excel_obj = s3_client.get_object(Bucket=s3_bucket, Key=source_excel_key)
    df_raw = pd.read_excel(BytesIO(excel_obj['Body'].read()), sheet_name=sheet_name, header=4)

    # Clean and rename columns
    df_raw.drop(index=0, inplace=True)
    df_raw.rename(columns={df_raw.columns[0]: 'year_month'}, inplace=True)
    df_raw.columns = df_raw.columns.str.strip()

    # Select and rename products
    PRODUCT_MAPPING = {
        'Soybeans': 'Soya',
        'Maize': 'Maize',
        'Rice, Thai 5%': 'Rice',
        'Wheat, US HRW': 'Wheat'
    }
    selected_columns = ['year_month'] + list(PRODUCT_MAPPING.keys())
    df_filtered = df_raw[selected_columns].copy()
    df_filtered.rename(columns=PRODUCT_MAPPING, inplace=True)

    # Reshape and transform
    df_melted = df_filtered.melt(id_vars='year_month', var_name='product_name', value_name='price_usd_per_ton')
    df_melted['date'] = pd.to_datetime(df_melted['year_month'].str.replace('M', ''), format='%Y%m')
    df_melted['year'] = df_melted['date'].dt.year
    df_melted['month'] = df_melted['date'].dt.month
    df_melted.sort_values(by=['product_name', 'date'], inplace=True)

    # Feature engineering
    df_melted['avg_annual_price'] = df_melted.groupby(['product_name', 'year'])['price_usd_per_ton'].transform('mean')
    df_melted['price_month_change_pct'] = df_melted.groupby('product_name')['price_usd_per_ton'].pct_change() * 100
    df_melted['price_annual_change_pct'] = df_melted.groupby('product_name')['avg_annual_price'].pct_change() * 100

    # Load dimension tables from S3
    dim_product_obj = s3_client.get_object(Bucket=s3_bucket, Key=dim_product_key)
    dim_date_obj = s3_client.get_object(Bucket=s3_bucket, Key=dim_date_key)
    dim_product = pd.read_csv(BytesIO(dim_product_obj['Body'].read()))
    dim_date = pd.read_csv(BytesIO(dim_date_obj['Body'].read()))

    # Join dimensions
    df_melted = df_melted.merge(dim_product[['product_id', 'product_name']], on='product_name', how='left')
    df_melted = df_melted.merge(dim_date[['date_id', 'year', 'month']], on=['year', 'month'], how='left')

    # Final fact table
    fact_prices = df_melted[['date_id', 'product_id', 'price_usd_per_ton',
                             'avg_annual_price', 'price_annual_change_pct',
                             'price_month_change_pct']].copy()
    fact_prices.reset_index(drop=True, inplace=True)
    fact_prices['price_id'] = fact_prices.index + 1
    fact_prices = fact_prices[['price_id'] + [col for col in fact_prices.columns if col != 'price_id']]
    fact_prices[['avg_annual_price', 'price_annual_change_pct', 'price_month_change_pct']] = \
        fact_prices[['avg_annual_price', 'price_annual_change_pct', 'price_month_change_pct']].round(2)

    # Save to CSV and upload to S3
    csv_buffer = BytesIO()
    fact_prices.to_csv(csv_buffer, index=False, encoding='utf-8')
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=output_fact_prices_key,
        Body=csv_buffer.getvalue()
)

    return {
        'statusCode': 200,
        'body': 'Transformation of fact_prices completed successfully!'
    }
