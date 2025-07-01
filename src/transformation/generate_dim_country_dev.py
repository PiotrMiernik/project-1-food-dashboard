import pandas as pd
from zipfile import ZipFile
from pathlib import Path

# Path to ZIP archive and external mapping file
ZIP_PATH = Path("data/raw/FAO/Value_of_Production/faostat_production.zip")
MAPPING_FILE = Path("data/resources/m49_continents.csv")

# Locate the correct CSV file inside the ZIP
with ZipFile(ZIP_PATH, 'r') as zip_ref:
    csv_files = [f for f in zip_ref.namelist() if "Value_of_Production_E_All_Data" in f and f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("Expected production CSV file not found in ZIP archive.")
    with zip_ref.open(csv_files[0]) as f:
        df_raw = pd.read_csv(f, encoding='utf-8')

# Select country-related columns
df_countries = df_raw[['Area Code (M49)', 'Area']].drop_duplicates()

# Normalize M49 codes: remove apostrophes, pad with zeros
df_countries['m49_code'] = (
    df_countries['Area Code (M49)']
    .astype(str)
    .str.replace("'", "", regex=False)
    .str.zfill(3)
)

# Load M49-to-continent mapping
df_mapping = pd.read_csv(MAPPING_FILE, dtype={'m49_code': str})

# Merge with mapping
df_enriched = pd.merge(df_countries, df_mapping, on='m49_code', how='left')

# Generate surrogate key
df_enriched.reset_index(drop=True, inplace=True)
df_enriched['country_id'] = df_enriched.index + 1

# Final schema
dim_country = df_enriched[['country_id', 'Area', 'continent_name']]
dim_country.rename(columns={'Area': 'country_name'}, inplace=True)
dim_country = dim_country[dim_country['continent_name'].notna()]

# Save output
dim_country.to_csv('data/transformed/dim_country.csv', index=False)

# Preview
print(dim_country.head(10))
print(f"\nTotal countries processed: {len(dim_country)}")
