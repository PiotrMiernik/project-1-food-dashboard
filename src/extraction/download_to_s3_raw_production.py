import boto3
import requests
from io import BytesIO
import os
import zipfile

def lambda_handler(event, context):
    """
    Downloads data from FAOSTAT and World Bank and saves it to an AWS S3 bucket.

    Environment variables:
    - S3_BUCKET_PROJECT_1: the name of the S3 bucket to write to
    - S3_PREFIX_RAW: prefix for S3 keys (default is "raw/")

    Returns:
        dict: {"status": "success"} if all downloads were successful.
    """
    data_sources = {
        "faostat_production": "https://bulks-faostat.fao.org/production/Value_of_Production_E_All_Data.zip",
        "faostat_export_import": "https://bulks-faostat.fao.org/production/Trade_CropsLivestock_E_All_Data.zip",
        "faostat_consumption": "https://bulks-faostat.fao.org/production/FoodBalanceSheets_E_All_Data.zip",
        "wb_prices": "https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx"
    }

    s3 = boto3.client("s3")
    s3_bucket = os.environ["S3_BUCKET_PROJECT_1"]
    raw_prefix = os.environ.get("S3_PREFIX_RAW", "raw/")

    # Downloading data from FAOSTAT and WB (prices) and save to S3
    for filename, url in data_sources.items():
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            buffer = BytesIO(response.content)
            extension = ".xlsx" if url.endswith(".xlsx") else ".zip"
            s3.put_object(Bucket=s3_bucket, Key=f"{raw_prefix}{filename}{extension}", Body=buffer.getvalue())
        else:
            raise Exception(f"Failed to download {filename}")

    # Downloading population data from the World Bank API (in ZIP format)
    population_url = "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv"
    response = requests.get(population_url, stream=True)
    if response.status_code == 200:
        buffer = BytesIO(response.content)
        with zipfile.ZipFile(buffer) as z:
            for file_name in z.namelist():
                if file_name.endswith(".csv") and "Metadata" not in file_name:
                    extracted = z.read(file_name)
                    s3.put_object(Bucket=s3_bucket, Key=f"{raw_prefix}wb_population.csv", Body=extracted)
                    break
    else:
        raise Exception("Failed to download population data")

    return {"status": "success"}

        
 