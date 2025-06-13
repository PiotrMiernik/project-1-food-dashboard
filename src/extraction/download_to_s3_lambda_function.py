import boto3
import requests
from io import BytesIO
import os

def lambda_handler(event, context):
    """
    Downloads data from FAOSTAT and World Bank and saves it to an AWS S3 bucket.

    The following environment variables must be set:

    - S3_BUCKET: the name of the S3 bucket to write to

    The following environment variable is optional:

    - S3_PREFIX: the prefix to use for the S3 key (default is "raw/")

    Returns a dictionary with a single key "status" set to "success" if all downloads were successful.
    """   
    data_sources = {
        "faostat_production": "https://bulks-faostat.fao.org/production/Value_of_Production_E_All_Data.zip",
        "faostat_export_import": "https://bulks-faostat.fao.org/production/Trade_CropsLivestock_E_All_Data.zip",
        "faostat_consumption": "https://bulks-faostat.fao.org/production/FoodBalanceSheets_E_All_Data.zip",
        "wb_prices": "https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx"

    }

    s3 = boto3.client("s3") 
    bucket = os.environ["PROJECT-1-FOOD-DASHBOARD"]
    prefix = os.environ.get("S3_PREFIX", "raw/")    

    for filename, url in data_sources.items():
        response = requests.get(url, stream=True)
        if response.status == 200:
            buffer = BytesIO(response.content)
            if url.endswith(".xlsx"):
                extension = ".xlsx"
            elif url.endswith(".zip"):
                extension = ".zip"
            else:
                extension = ""
            s3.put_object(Bucket=bucket, Key=f"{prefix}{filename}{extension}", Body=buffer.getvalue())
        else:
            raise Exception(f"Failed to download {filename}")
    return {"status": "success"}
        
 