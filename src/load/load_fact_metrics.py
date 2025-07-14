import os
import pandas as pd
import psycopg2
from io import StringIO

from src.helpers.s3_utils import read_csv_from_s3
from src.helpers.db_utils import get_db_connection

def lambda_handler(event=None, context=None):
    """
    Lambda function to load transformed fact_metrics.csv from S3 to Amazon RDS (PostgreSQL).
    """
    # Environment variables
    s3_bucket = os.environ['S3_BUCKET_PROJECT_1']
    transformed_prefix = os.environ.get('S3_PREFIX_TRANSFORMED', 'transformed/')
    transformed_key = f'{transformed_prefix}fact_metrics.csv'

    # Load data from S3
    df = read_csv_from_s3(s3_bucket, transformed_key)

    # Establish DB connection
    conn = get_db_connection()
    cursor = conn.cursor()

    # Truncate table before inserting new data
    cursor.execute("TRUNCATE TABLE fact_metrics RESTART IDENTITY")

    # Prepare buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Load into RDS
    cursor.copy_expert(
        """
        COPY fact_metrics(
            fact_id,
            date_id,
            product_id,
            country_id,
            metric_type,
            value
        ) FROM STDIN WITH CSV
        """,
        buffer
    )

    # Commit and clean up
    conn.commit()
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': 'fact_metrics loaded successfully into data warehouse',
    }
