from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2

def get_db_connection():
    """
    Create and return a PostgreSQL database connection using environment variables.
    """
    return psycopg2.connect(
        host=os.environ['RDS_HOST'],
        port=os.environ.get('RDS_PORT', 5432),
        dbname=os.environ['RDS_DATABASE'],
        user=os.environ['RDS_USER'],
        password=os.environ['RDS_PASSWORD']
    )
