import boto3
import pandas as pd
import os
from io import BytesIO


def lambda_handler(event, context):
    """
    AWS Lambda function to generate unified fact_metrics table (long format) from 4 transformed CSVs in S3,
    and store the final table in the transformed zone.
    """

    # Environment config
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    transformed_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')

    # File keys
    input_keys = {
        'consumption': f"{transformed_prefix}fact_metrics_consumption.csv",
        'production': f"{transformed_prefix}fact_metrics_production.csv",
        'trade': f"{transformed_prefix}fact_metrics_trade.csv",
        'population': f"{transformed_prefix}fact_metrics_population.csv"
    }
    output_key = f"{transformed_prefix}fact_metrics.csv"

    # Init S3
    s3_client = boto3.client('s3')

    # Expected columns
    TARGET_COLUMNS = ["date_id", "product_id", "country_id", "metric_type", "value"]
    frames = []

    # Read and normalize all partial fact tables
    for metric_name, key in input_keys.items():
        obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
        df = pd.read_csv(BytesIO(obj['Body'].read()))

        if "value" not in df.columns:
            df["value"] = pd.NA

        df = df[TARGET_COLUMNS]
        frames.append(df)

    # Concatenate all dataframes
    fact_metrics = pd.concat(frames, ignore_index=True)
    fact_metrics.dropna(subset=["date_id", "product_id", "country_id", "metric_type"], inplace=True)

    # Add fact_id
    fact_metrics.reset_index(drop=True, inplace=True)
    fact_metrics["fact_id"] = fact_metrics.index + 1
    fact_metrics = fact_metrics[["fact_id"] + TARGET_COLUMNS]

    # Export to CSV and upload to S3
    csv_buffer = BytesIO()
    fact_metrics.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': 'Final unified fact_metrics table generated successfully!'
    }
