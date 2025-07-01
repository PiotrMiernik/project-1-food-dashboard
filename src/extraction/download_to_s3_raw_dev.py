import requests
from pathlib import Path

# Local target folders
raw_fao_path_value_of_production = Path("data/raw/FAO/Value_of_Production")
raw_fao_path_trade = Path("data/raw/FAO/Trade")
raw_fao_path_food_balance = Path("data/raw/FAO/FoodBalance")
raw_wb_path = Path("data/raw/WB")

# Ensure directories exist
raw_fao_path_value_of_production.mkdir(parents=True, exist_ok=True)
raw_fao_path_trade.mkdir(parents=True, exist_ok=True)
raw_fao_path_food_balance.mkdir(parents=True, exist_ok=True)
raw_wb_path.mkdir(parents=True, exist_ok=True)

# Data sources
data_sources = {
    "faostat_production": (
        "https://bulks-faostat.fao.org/production/Value_of_Production_E_All_Data.zip",
        raw_fao_path_value_of_production
    ),
    "faostat_export_import": (
        "https://bulks-faostat.fao.org/production/Trade_CropsLivestock_E_All_Data.zip",
        raw_fao_path_trade
    ),
    "faostat_consumption": (
        "https://bulks-faostat.fao.org/production/FoodBalanceSheets_E_All_Data.zip",
        raw_fao_path_food_balance
    ),
    "wb_prices": (
        "https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx",
        raw_wb_path
    ),
    "wb_population": (
        "http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv",
        raw_wb_path
    )
}

# Download loop
for filename, (url, folder) in data_sources.items():
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        if url.endswith(".xlsx"):
            ext = ".xlsx"
        elif url.endswith(".zip"):
            ext = ".zip"
        else:
            ext = ".zip"  # fallback for WB API CSV ZIP
        with open(folder / f"{filename}{ext}", "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {filename}{ext}")
    else:
        print(f"Failed to download {filename} from {url}")
